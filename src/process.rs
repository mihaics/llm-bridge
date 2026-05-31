//! Spawn child CLIs with stdin, a per-turn timeout, and a GLOBAL concurrency cap. One instance is
//! shared across all requests (held in AppState via the runner), so cloning shares the semaphore.
use std::process::Output;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::Semaphore;

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("the child timed out")]
    Timeout,
    #[error("spawn/io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone)]
pub struct ProcessSupervisor {
    pub(crate) sem: Arc<Semaphore>,
}

impl ProcessSupervisor {
    pub fn new(max_concurrency: usize) -> Self {
        ProcessSupervisor { sem: Arc::new(Semaphore::new(max_concurrency.max(1))) }
    }

    /// Run `cmd`, optionally writing `stdin`, killing it after `timeout`. Returns the full Output.
    pub async fn run(
        &self,
        mut cmd: Command,
        stdin: Option<String>,
        timeout: Duration,
    ) -> Result<Output, ProcessError> {
        let _permit = self.sem.acquire().await.expect("semaphore not closed");

        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        cmd.kill_on_drop(true);

        let mut child = cmd.spawn()?;

        if let Some(payload) = stdin {
            let mut sink = child.stdin.take().expect("stdin piped");
            sink.write_all(payload.as_bytes()).await?;
            sink.shutdown().await?; // close stdin so the child sees EOF
        } else {
            drop(child.stdin.take());
        }

        match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(result) => Ok(result?),
            Err(_) => Err(ProcessError::Timeout), // child is killed on drop
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::process::Command;

    #[tokio::test]
    async fn runs_command_and_captures_stdout() {
        let sup = ProcessSupervisor::new(2);
        let cmd = Command::new("cat"); // echoes stdin to stdout
        let out = sup.run(cmd, Some("hello\n".into()), Duration::from_secs(5)).await.unwrap();
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "hello");
    }

    #[tokio::test]
    async fn times_out_long_commands() {
        let sup = ProcessSupervisor::new(2);
        let mut cmd = Command::new("sleep");
        cmd.arg("5");
        let err = sup.run(cmd, None, Duration::from_millis(200)).await.unwrap_err();
        assert!(matches!(err, ProcessError::Timeout));
    }

    #[tokio::test]
    async fn surfaces_nonzero_exit() {
        let sup = ProcessSupervisor::new(2);
        let mut cmd = Command::new("bash");
        cmd.arg("-c").arg("echo oops >&2; exit 3");
        let out = sup.run(cmd, None, Duration::from_secs(5)).await.unwrap();
        assert_eq!(out.status.code(), Some(3));
        assert!(String::from_utf8_lossy(&out.stderr).contains("oops"));
    }

    #[tokio::test]
    async fn is_cloneable_and_shares_the_permit_pool() {
        // Cloning shares the same Arc<Semaphore> so the cap is global, not per-clone.
        let sup = ProcessSupervisor::new(1);
        let sup2 = sup.clone();
        assert!(std::sync::Arc::ptr_eq(&sup.sem, &sup2.sem));
    }
}
