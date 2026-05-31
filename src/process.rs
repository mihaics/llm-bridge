//! Spawn child CLIs with stdin, a per-turn timeout, and a GLOBAL concurrency cap. One instance is
//! shared across all requests (held in AppState via the runner), so cloning shares the semaphore.
use futures::Stream;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::Command;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct ProcessSupervisor {
    pub(crate) sem: Arc<Semaphore>,
}

impl ProcessSupervisor {
    pub fn new(max_concurrency: usize) -> Self {
        ProcessSupervisor { sem: Arc::new(Semaphore::new(max_concurrency.max(1))) }
    }

    /// Acquire an owned active-concurrency permit. Dropping it frees the slot (a parked suspension
    /// drops its permit while idle; the resume path re-acquires). Used by the tools-turn path.
    pub async fn acquire(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.sem.clone().acquire_owned().await.expect("semaphore not closed")
    }

    /// Currently-free active slots (for tests/observability).
    pub fn available(&self) -> usize {
        self.sem.available_permits()
    }

    /// Spawn a child and stream its stdout line-by-line as it arrives. The concurrency permit and
    /// the child are owned by the returned stream: dropping the stream frees the permit and kills
    /// the child (kill_on_drop). A per-stream deadline turns into a terminal TimedOut error.
    pub fn spawn_streaming(
        &self,
        mut cmd: Command,
        stdin: Option<String>,
        timeout: Duration,
    ) -> impl Stream<Item = std::io::Result<String>> + Send {
        let sem = self.sem.clone();
        async_stream::stream! {
            let _permit = match sem.acquire_owned().await {
                Ok(p) => p,
                Err(_) => { yield Err(std::io::Error::other("supervisor shut down")); return; }
            };

            cmd.stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            cmd.kill_on_drop(true);

            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => { yield Err(e); return; }
            };

            if let Some(payload) = stdin {
                if let Some(mut sink) = child.stdin.take() {
                    use tokio::io::AsyncWriteExt;
                    let _ = sink.write_all(payload.as_bytes()).await;
                    let _ = sink.shutdown().await;
                }
            }

            let stdout = match child.stdout.take() {
                Some(s) => s,
                None => { yield Err(std::io::Error::other("no stdout")); return; }
            };
            let mut reader = BufReader::new(stdout).lines();
            let deadline = tokio::time::Instant::now() + timeout;

            loop {
                match tokio::time::timeout_at(deadline, reader.next_line()).await {
                    Ok(Ok(Some(line))) => yield Ok(line),
                    Ok(Ok(None)) => break,                       // clean EOF
                    Ok(Err(e)) => { yield Err(e); break; }
                    Err(_elapsed) => {
                        yield Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "child timed out"));
                        break;
                    }
                }
            }
            // `child` (kill_on_drop) and `_permit` drop here when the stream is dropped/exhausted.
            drop(child);
        }
    }

    /// Like `spawn_streaming` but WITHOUT acquiring a concurrency permit — the caller manages the
    /// active slot (via `acquire`/drop) so a parked tool-call suspension can release it while idle.
    ///
    /// `timeout` here is a PER-LINE IDLE budget, not the whole-turn absolute deadline `spawn_streaming`
    /// uses. The tools-turn child is polled in two active phases (collection, then continuation)
    /// separated by an arbitrary PARKED gap — the client's think-time between the `tool_calls`
    /// response and the result follow-up. An absolute deadline would charge that idle gap against
    /// claude's active budget and kill a healthy child on resume; an idle timeout only counts time
    /// spent waiting on output while we ARE polling, so the parked gap (unpolled) is excluded and the
    /// continuation gets a fresh window. The parked duration is bounded separately by the suspension
    /// reaper (`tool_result_timeout`).
    pub fn spawn_streaming_unmetered(
        &self,
        mut cmd: Command,
        stdin: Option<String>,
        timeout: Duration,
    ) -> impl Stream<Item = std::io::Result<String>> + Send {
        async_stream::stream! {
            cmd.stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            cmd.kill_on_drop(true);

            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => { yield Err(e); return; }
            };

            if let Some(payload) = stdin {
                if let Some(mut sink) = child.stdin.take() {
                    use tokio::io::AsyncWriteExt;
                    let _ = sink.write_all(payload.as_bytes()).await;
                    let _ = sink.shutdown().await;
                }
            }

            let stdout = match child.stdout.take() {
                Some(s) => s,
                None => { yield Err(std::io::Error::other("no stdout")); return; }
            };
            let mut reader = BufReader::new(stdout).lines();

            // Per-line IDLE timeout (reset each iteration), NOT an absolute whole-turn deadline — see
            // the fn doc: this excludes the parked gap between the collection and continuation phases.
            loop {
                match tokio::time::timeout(timeout, reader.next_line()).await {
                    Ok(Ok(Some(line))) => yield Ok(line),
                    Ok(Ok(None)) => break,                       // clean EOF
                    Ok(Err(e)) => { yield Err(e); break; }
                    Err(_elapsed) => {
                        yield Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "child idle-timed out"));
                        break;
                    }
                }
            }
            // `child` (kill_on_drop) drops here when the stream is dropped/exhausted. No permit is
            // held: the caller owns the active slot.
            drop(child);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::process::Command;

    #[tokio::test]
    async fn is_cloneable_and_shares_the_permit_pool() {
        // Cloning shares the same Arc<Semaphore> so the cap is global, not per-clone.
        let sup = ProcessSupervisor::new(1);
        let sup2 = sup.clone();
        assert!(std::sync::Arc::ptr_eq(&sup.sem, &sup2.sem));
    }

    #[tokio::test]
    async fn spawn_streaming_yields_stdout_lines() {
        use futures::StreamExt;
        let sup = ProcessSupervisor::new(2);
        let mut cmd = Command::new("bash");
        cmd.arg("-c").arg("printf 'a\\nb\\nc\\n'");
        let stream = sup.spawn_streaming(cmd, None, Duration::from_secs(5));
        let lines: Vec<String> = stream.filter_map(|r| async move { r.ok() }).collect().await;
        assert_eq!(lines, vec!["a", "b", "c"]);
    }

    #[tokio::test]
    async fn spawn_streaming_unmetered_yields_lines_without_consuming_a_permit() {
        use futures::StreamExt;
        let sup = ProcessSupervisor::new(2);
        assert_eq!(sup.available(), 2);
        let mut cmd = Command::new("bash");
        cmd.arg("-c").arg("printf 'a\\nb\\nc\\n'");
        let stream = sup.spawn_streaming_unmetered(cmd, None, Duration::from_secs(5));
        // available() stays unchanged while the stream is live (no permit acquired by the variant).
        assert_eq!(sup.available(), 2);
        let lines: Vec<String> = stream.filter_map(|r| async move { r.ok() }).collect().await;
        assert_eq!(lines, vec!["a", "b", "c"]);
        assert_eq!(sup.available(), 2);
    }

    #[tokio::test]
    async fn unmetered_idle_timeout_resets_per_line_not_whole_turn() {
        use futures::StreamExt;
        let sup = ProcessSupervisor::new(1);
        let mut cmd = Command::new("bash");
        // 4 lines, ~200ms apart → ~800ms total runtime, but every inter-line gap < the 500ms budget.
        cmd.arg("-c").arg("for i in 1 2 3 4; do printf \"L$i\\n\"; sleep 0.2; done");
        let stream = sup.spawn_streaming_unmetered(cmd, None, Duration::from_millis(500));
        let lines: Vec<String> = stream.filter_map(|r| async move { r.ok() }).collect().await;
        // An absolute 500ms whole-turn deadline would truncate this (~800ms total); a per-line idle
        // timeout resets each line, so all four survive.
        assert_eq!(lines, vec!["L1", "L2", "L3", "L4"]);
    }

    #[tokio::test]
    async fn unmetered_idle_timeout_fires_on_long_gap() {
        use futures::StreamExt;
        let sup = ProcessSupervisor::new(1);
        let mut cmd = Command::new("bash");
        cmd.arg("-c").arg("printf 'first\\n'; sleep 5; printf 'late\\n'");
        let stream = sup.spawn_streaming_unmetered(cmd, None, Duration::from_millis(300));
        let results: Vec<_> = stream.collect().await;
        assert!(results.iter().any(|r| matches!(r, Ok(l) if l == "first")));
        assert!(results.iter().any(|r| matches!(r, Err(e) if e.kind() == std::io::ErrorKind::TimedOut)));
        assert!(!results.iter().any(|r| matches!(r, Ok(l) if l == "late")));
    }

    #[tokio::test]
    async fn spawn_streaming_writes_stdin() {
        use futures::StreamExt;
        let sup = ProcessSupervisor::new(2);
        let cmd = Command::new("cat");
        let stream = sup.spawn_streaming(cmd, Some("hello\n".into()), Duration::from_secs(5));
        let lines: Vec<String> = stream.filter_map(|r| async move { r.ok() }).collect().await;
        assert_eq!(lines, vec!["hello"]);
    }

    #[tokio::test]
    async fn spawn_streaming_times_out() {
        use futures::StreamExt;
        let sup = ProcessSupervisor::new(2);
        let mut cmd = Command::new("bash");
        cmd.arg("-c").arg("printf 'first\\n'; sleep 5; printf 'late\\n'");
        let stream = sup.spawn_streaming(cmd, None, Duration::from_millis(300));
        let results: Vec<_> = stream.collect().await;
        // first line arrives, then a timeout error terminates the stream (no "late").
        assert_eq!(results.iter().filter(|r| matches!(r, Ok(l) if l == "first")).count(), 1);
        assert!(results.iter().any(|r| matches!(r, Err(e) if e.kind() == std::io::ErrorKind::TimedOut)));
        assert!(!results.iter().any(|r| matches!(r, Ok(l) if l == "late")));
    }

    #[tokio::test]
    async fn acquire_and_drop_releases_active_slot() {
        let sup = ProcessSupervisor::new(1); // one active slot
        let permit = sup.acquire().await;     // take it
        assert_eq!(sup.available(), 0);
        drop(permit);                          // park-release
        assert_eq!(sup.available(), 1);
        let _p2 = sup.acquire().await;         // resume re-acquires
        assert_eq!(sup.available(), 0);
    }
}
