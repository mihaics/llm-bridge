//! Startup validation. Returns Err(Vec<message>) listing every problem (so the operator sees all
//! issues at once). Phase 1 enforces the rules it owns; Phase 3 adds the sandbox canary probes.
use crate::config::{Config, EngineKind, Mode, SandboxBackend};
use std::path::{Path, PathBuf};

/// Tokens we refuse for a non-loopback bind even though they are non-empty.
const PLACEHOLDER_TOKENS: &[&str] = &["sk-change-me", "sk-noauth", "changeme", "secret", "token"];
const MIN_TOKEN_LEN: usize = 16;

pub fn validate_config(cfg: &Config) -> Result<(), Vec<String>> {
    let mut errs = Vec::new();

    // Bind / auth.
    if !is_loopback_bind(&cfg.server.bind) {
        let token = cfg.server.bearer_token.as_deref().unwrap_or("");
        if token.is_empty() {
            errs.push(format!(
                "server.bind {} is non-loopback but no bearer_token is set; refusing to expose unauthenticated",
                cfg.server.bind
            ));
        } else if PLACEHOLDER_TOKENS.contains(&token) || token.len() < MIN_TOKEN_LEN {
            errs.push(format!(
                "server.bind {} is non-loopback but bearer_token is a placeholder/too short; set a strong, non-default token (>= {} chars)",
                cfg.server.bind, MIN_TOKEN_LEN
            ));
        }
    }

    // Phase-1 ceiling: only `none` sandbox backend is implemented.
    if cfg.defaults.sandbox_backend != SandboxBackend::None {
        errs.push(format!(
            "defaults.sandbox_backend={:?} is not implemented in Phase 1 (use `none`; sandbox backends arrive in Phase 3)",
            cfg.defaults.sandbox_backend
        ));
    }

    let cred_dirs: Vec<PathBuf> = [
        cfg.credentials.claude_config_dir.clone(),
        cfg.credentials.codex_home.clone(),
        cfg.credentials.agy_config_dir.clone(),
    ]
    .into_iter()
    .flatten()
    .collect();

    for m in &cfg.models {
        if m.engine != EngineKind::Claude {
            errs.push(format!(
                "model '{}': only the claude engine is implemented in Phase 1 (got {:?})",
                m.id, m.engine
            ));
        }

        if m.mode == Mode::Agentic {
            if !m.trusted_caller_only {
                errs.push(format!(
                    "model '{}': agentic models require trusted_caller_only: true when sandbox_backend is none",
                    m.id
                ));
            }
            if m.workspace.is_none() {
                errs.push(format!("model '{}': agentic models require a workspace", m.id));
            }
        }

        if let Some(ws) = &m.workspace {
            let ws_r = resolve_path(ws);
            for cred in &cred_dirs {
                if paths_overlap(&ws_r, &resolve_path(cred)) {
                    errs.push(format!(
                        "model '{}': workspace {} overlaps credential dir {}",
                        m.id, ws.display(), cred.display()
                    ));
                }
            }
        }
    }

    if errs.is_empty() { Ok(()) } else { Err(errs) }
}

fn is_loopback_bind(bind: &str) -> bool {
    let host = bind.rsplit_once(':').map(|(h, _)| h).unwrap_or(bind);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>().map(|ip| ip.is_loopback()).unwrap_or(false)
}

/// Canonicalize if the path exists; otherwise make it lexically absolute.
fn resolve_path(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf()))
}

fn paths_overlap(a: &Path, b: &Path) -> bool {
    a == b || a.starts_with(b) || b.starts_with(a)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use std::path::PathBuf;

    fn base() -> Config {
        Config {
            server: ServerConfig { bind: "127.0.0.1:8088".into(), bearer_token: None,
                                   progress_channel: ProgressChannel::ReasoningContent },
            defaults: Defaults::default(),
            credentials: Credentials::default(),
            models: vec![],
        }
    }

    fn agentic(ws: &str, trusted: bool) -> ModelEntry {
        ModelEntry { id: "a".into(), engine: EngineKind::Claude, model: Some("opus".into()),
            workspace: Some(PathBuf::from(ws)), mode: Mode::Agentic, permissions: None,
            trusted_caller_only: trusted }
    }

    #[test]
    fn loopback_without_token_is_ok() {
        assert!(validate_config(&base()).is_ok());
    }

    #[test]
    fn non_loopback_without_token_is_refused() {
        let mut cfg = base();
        cfg.server.bind = "0.0.0.0:8088".into();
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("non-loopback")), "{err:?}");
    }

    #[test]
    fn non_loopback_with_placeholder_token_is_refused() {
        let mut cfg = base();
        cfg.server.bind = "0.0.0.0:8088".into();
        cfg.server.bearer_token = Some("sk-change-me".into());
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("non-default")), "{err:?}");
    }

    #[test]
    fn non_loopback_with_strong_token_is_ok() {
        let mut cfg = base();
        cfg.server.bind = "0.0.0.0:8088".into();
        cfg.server.bearer_token = Some("k7Qe2vR9mZ1pX4nL8wTciuY3".into());
        assert!(validate_config(&cfg).is_ok());
    }

    #[test]
    fn agentic_without_trusted_is_refused() {
        let mut cfg = base();
        cfg.models = vec![agentic("/work/repoA", false)];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("trusted_caller_only")), "{err:?}");
    }

    #[test]
    fn agentic_with_trusted_is_ok() {
        let mut cfg = base();
        cfg.models = vec![agentic("/work/repoA", true)];
        assert!(validate_config(&cfg).is_ok());
    }

    #[test]
    fn sandbox_backend_other_than_none_is_refused_in_phase1() {
        let mut cfg = base();
        cfg.defaults.sandbox_backend = SandboxBackend::Bubblewrap;
        cfg.models = vec![agentic("/work/repoA", true)];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("sandbox_backend")), "{err:?}");
    }

    #[test]
    fn non_claude_engine_is_refused_in_phase1() {
        let mut cfg = base();
        cfg.models = vec![ModelEntry { id: "c".into(), engine: EngineKind::Codex,
            model: None, workspace: None, mode: Mode::Text, permissions: None,
            trusted_caller_only: false }];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("only the claude engine")), "{err:?}");
    }

    #[test]
    fn workspace_containing_credential_dir_is_refused() {
        let mut cfg = base();
        cfg.credentials.claude_config_dir = Some(PathBuf::from("/home/u/cred/claude"));
        cfg.models = vec![agentic("/home/u", true)];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("overlaps")), "{err:?}");
    }
}
