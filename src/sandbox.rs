//! External sandbox backend (`bubblewrap`) and the startup canary probes (spec §4.8). Wrapping
//! makes the workspace the only writable root and exposes ONLY the engine's own cred/home dir
//! read-only; a fresh tmpfs over `/tmp` shadows any planted secret. The pure argv builder +
//! `canary_decision` are unit-tested; `run_probes` shells out to real `bwrap` (feature-gated test).
use crate::config::SandboxBackend;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};

/// Result of the two startup probes against a model's effective sandbox.
#[derive(Debug, Clone, Copy)]
pub struct ProbeOutcome {
    pub write_denied: bool,
    pub read_denied: bool,
}

/// Refuse unless BOTH the write- and read-denial probes passed (the path is denied). Pure.
pub fn canary_decision(o: &ProbeOutcome) -> Result<(), String> {
    let mut bad = Vec::new();
    if !o.write_denied {
        bad.push("write-denial probe FAILED (a write outside the workspace succeeded)");
    }
    if !o.read_denied {
        bad.push("read-denial probe FAILED (the sandboxed process read a planted secret)");
    }
    if bad.is_empty() { Ok(()) } else { Err(bad.join("; ")) }
}

/// Build the argv that follows `bwrap` (system RO binds, tmpfs /tmp, workspace RW, cred dirs RO,
/// then `-- <program> <args...>`). Pure so it is unit-testable without executing anything.
pub(crate) fn bwrap_argv(
    workspace: Option<&Path>,
    ro_paths: &[PathBuf],
    inner_program: &OsStr,
    inner_args: &[OsString],
    cwd: Option<&Path>,
) -> Vec<OsString> {
    // Fixed preamble flags: namespace isolation, proc/dev mounts, tmpfs over /tmp.
    // tmpfs over /tmp shadows any secret planted on the host -> read-denial probe passes.
    let mut a: Vec<OsString> = vec![
        OsString::from("--die-with-parent"),
        OsString::from("--unshare-all"),
        OsString::from("--share-net"), // engines need network to reach the model vendor
        OsString::from("--proc"),
        OsString::from("/proc"),
        OsString::from("--dev"),
        OsString::from("/dev"),
        OsString::from("--tmpfs"),
        OsString::from("/tmp"),
    ];
    for sys in ["/usr", "/bin", "/sbin", "/lib", "/lib64", "/etc"] {
        if Path::new(sys).exists() {
            a.push(OsString::from("--ro-bind"));
            a.push(OsString::from(sys));
            a.push(OsString::from(sys));
        }
    }
    if let Some(ws) = workspace {
        a.push(OsString::from("--bind"));
        a.push(ws.as_os_str().to_os_string());
        a.push(ws.as_os_str().to_os_string());
    }
    for ro in ro_paths {
        a.push(OsString::from("--ro-bind"));
        a.push(ro.as_os_str().to_os_string());
        a.push(ro.as_os_str().to_os_string());
    }
    if let Some(dir) = cwd {
        a.push(OsString::from("--chdir"));
        a.push(dir.as_os_str().to_os_string());
    }
    a.push(OsString::from("--"));
    a.push(inner_program.to_os_string());
    a.extend(inner_args.iter().cloned());
    a
}

/// Wrap a built engine command in `bwrap`, preserving its program/args/cwd/env.
fn wrap_bubblewrap(inner: tokio::process::Command, workspace: Option<&Path>, ro_paths: &[PathBuf]) -> tokio::process::Command {
    let std_inner = inner.as_std();
    let program = std_inner.get_program().to_os_string();
    let inner_args: Vec<OsString> = std_inner.get_args().map(|a| a.to_os_string()).collect();
    let cwd = std_inner.get_current_dir().map(|p| p.to_path_buf());
    let envs: Vec<(OsString, Option<OsString>)> =
        std_inner.get_envs().map(|(k, v)| (k.to_os_string(), v.map(|v| v.to_os_string()))).collect();

    // The inner command's cwd becomes the sandbox cwd via `--chdir` in the argv; the outer `bwrap`
    // process intentionally keeps the inherited cwd (bwrap controls the in-namespace cwd itself).
    let argv = bwrap_argv(workspace, ro_paths, &program, &inner_args, cwd.as_deref());

    let mut bw = tokio::process::Command::new("bwrap");
    bw.args(argv);
    bw.env_clear();
    for (k, v) in envs {
        // Faithfully replay the inner command's env onto bwrap: set present vars, and honor an
        // explicit removal (`None`) so a caller's `env_remove` intent is never silently lost.
        match v {
            Some(v) => { bw.env(k, v); }
            None => { bw.env_remove(k); }
        }
    }
    bw
}

/// Wrap the command for the configured backend. `none` (and the not-yet-implemented `container`,
/// which startup validation refuses) return the command unchanged.
pub fn maybe_wrap(
    backend: SandboxBackend,
    workspace: Option<&Path>,
    ro_paths: &[PathBuf],
    cmd: tokio::process::Command,
) -> tokio::process::Command {
    match backend {
        SandboxBackend::Bubblewrap => wrap_bubblewrap(cmd, workspace, ro_paths),
        SandboxBackend::None | SandboxBackend::Container => cmd,
    }
}

/// Run the two real bwrap canary probes against a model's effective sandbox. Called at startup
/// (main) when `sandbox_backend != none`. A bwrap spawn failure surfaces as `Err` so startup refuses.
pub fn run_probes(workspace: Option<&Path>, ro_paths: &[PathBuf]) -> std::io::Result<ProbeOutcome> {
    // Sanity gate: a benign command MUST succeed inside the sandbox. Otherwise a non-functional
    // sandbox (e.g. `sh` not reachable) would make every probe "fail" and read as a false DENIAL,
    // letting an unsafe posture pass startup. If the benign command can't run, the sandbox is
    // broken — surface that as an error so startup refuses rather than silently "passing".
    if !run_wrapped(workspace, ro_paths, "true")? {
        return Err(std::io::Error::other(
            "sandbox sanity check failed: a benign command did not run inside bwrap (sandbox not functional)",
        ));
    }

    // write-denial: writing into a RO-bound system path must fail.
    let write_denied = !run_wrapped(workspace, ro_paths, "echo probe > /etc/llm-bridge-write-probe 2>/dev/null")?;

    // read-denial: plant a secret in the host temp dir (shadowed by the sandbox tmpfs), then try
    // to read it from inside the sandbox; it must be invisible.
    let secret = std::env::temp_dir().join(format!("llm-bridge-canary-{}", std::process::id()));
    std::fs::write(&secret, b"TOP-SECRET-CANARY")?;
    let script = format!("cat '{}' 2>/dev/null", secret.display());
    let read_result = run_wrapped(workspace, ro_paths, &script);
    let _ = std::fs::remove_file(&secret);
    let read_denied = !read_result?;

    Ok(ProbeOutcome { write_denied, read_denied })
}

/// Run `sh -c <script>` inside the bubblewrap sandbox; return whether it SUCCEEDED (exit 0).
fn run_wrapped(workspace: Option<&Path>, ro_paths: &[PathBuf], script: &str) -> std::io::Result<bool> {
    let argv = bwrap_argv(
        workspace,
        ro_paths,
        OsStr::new("sh"),
        &[OsString::from("-c"), OsString::from(script)],
        None,
    );
    let status = std::process::Command::new("bwrap")
        .args(argv)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .status()?;
    Ok(status.success())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::path::PathBuf;

    #[test]
    fn decision_ok_only_when_both_denied() {
        assert!(canary_decision(&ProbeOutcome { write_denied: true, read_denied: true }).is_ok());
        let e = canary_decision(&ProbeOutcome { write_denied: false, read_denied: true }).unwrap_err();
        assert!(e.contains("write"), "{e}");
        let e = canary_decision(&ProbeOutcome { write_denied: true, read_denied: false }).unwrap_err();
        assert!(e.contains("read"), "{e}");
    }

    #[test]
    fn argv_binds_workspace_rw_cred_ro_and_ends_with_inner() {
        let ws = PathBuf::from("/work/repoB");
        let cred = PathBuf::from("/cred/codex");
        let program = OsString::from("codex");
        let argv = bwrap_argv(
            Some(ws.as_path()),
            std::slice::from_ref(&cred),
            program.as_os_str(),
            &[OsString::from("exec"), OsString::from("--json")],
            Some(ws.as_path()),
        );
        let s: Vec<String> = argv.iter().map(|a| a.to_string_lossy().into_owned()).collect();
        // workspace is the only writable root
        assert!(s.windows(3).any(|w| w == ["--bind", "/work/repoB", "/work/repoB"]));
        // cred dir is read-only
        assert!(s.windows(3).any(|w| w == ["--ro-bind", "/cred/codex", "/cred/codex"]));
        // tmpfs shadows /tmp (read-denial)
        assert!(s.windows(2).any(|w| w == ["--tmpfs", "/tmp"]));
        // the inner command follows the `--` separator, in order
        let dd = s.iter().position(|a| a == "--").expect("-- present");
        assert_eq!(&s[dd + 1..], &["codex", "exec", "--json"]);
    }

    #[test]
    fn maybe_wrap_is_noop_for_none() {
        let inner = tokio::process::Command::new("codex");
        let wrapped = maybe_wrap(crate::config::SandboxBackend::None, None, &[], inner);
        assert_eq!(wrapped.as_std().get_program().to_string_lossy(), "codex");
    }

    #[test]
    fn maybe_wrap_uses_bwrap_for_bubblewrap() {
        let inner = tokio::process::Command::new("codex");
        let ws = PathBuf::from("/work/x");
        let wrapped = maybe_wrap(crate::config::SandboxBackend::Bubblewrap, Some(ws.as_path()), &[], inner);
        assert_eq!(wrapped.as_std().get_program().to_string_lossy(), "bwrap");
    }

    // Real bwrap probe — needs `bwrap` on the host. Run with:
    //   cargo test --features e2e_smoke --lib sandbox::tests::real_bwrap_denies_write_and_read -- --nocapture
    #[cfg(feature = "e2e_smoke")]
    #[test]
    fn real_bwrap_denies_write_and_read() {
        let ws = std::env::temp_dir().join(format!("llm-bridge-ws-{}", std::process::id()));
        std::fs::create_dir_all(&ws).unwrap();
        let outcome = run_probes(Some(&ws), &[]).expect("bwrap ran");
        let _ = std::fs::remove_dir_all(&ws);
        assert!(outcome.write_denied, "write must be denied outside the workspace");
        assert!(outcome.read_denied, "planted secret must be unreadable inside the sandbox");
        assert!(canary_decision(&outcome).is_ok());
    }
}
