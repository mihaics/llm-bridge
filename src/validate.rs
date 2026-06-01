//! Startup validation. Returns Err(Vec<message>) listing every problem at once. Phase 3 allows all
//! three engines + the bubblewrap sandbox; the real write/read canary probes run at startup in main.
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

    // bubblewrap is implemented; container is not yet (the enum value stays valid in config).
    if cfg.defaults.sandbox_backend == SandboxBackend::Container {
        errs.push(
            "defaults.sandbox_backend=container is not yet implemented; use `bubblewrap` or `none`".to_string(),
        );
    }
    let sandboxed = cfg.defaults.sandbox_backend != SandboxBackend::None;

    let cred_dirs: Vec<PathBuf> = [
        cfg.credentials.claude_config_dir.clone(),
        cfg.credentials.codex_home.clone(),
        cfg.credentials.agy_config_dir.clone(),
    ]
    .into_iter()
    .flatten()
    .collect();

    for m in &cfg.models {
        // (All three engines are supported in Phase 3 — no engine refusal here.)

        if m.mode == Mode::Agentic {
            if !sandboxed && !m.trusted_caller_only {
                errs.push(format!(
                    "model '{}': agentic models require trusted_caller_only: true when sandbox_backend is none (no native sandbox passes the read-denial probe)",
                    m.id
                ));
            }
            if m.workspace.is_none() {
                errs.push(format!("model '{}': agentic models require a workspace", m.id));
            }
            // API-key env auth leaks into model-run tool shells for claude/agy (only codex scrubs);
            // disallow it for those engines unless an external sandbox contains the leak.
            if !sandboxed && matches!(m.engine, EngineKind::Claude | EngineKind::Agy) {
                let leaked: Vec<&str> = api_key_vars(m.engine)
                    .iter()
                    .copied()
                    .filter(|k| cfg.defaults.env_passthrough.iter().any(|p| p == k))
                    .collect();
                if !leaked.is_empty() {
                    errs.push(format!(
                        "model '{}': API-key env auth ({}) is disallowed for a {} agentic model without a sandbox_backend (it leaks to model-run tool shells); use file-based login or set sandbox_backend",
                        m.id, leaked.join(", "), m.engine.as_str()
                    ));
                }
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

/// API-key env vars that leak into model-run tool shells. codex scrubs via shell_environment_policy
/// (so it is exempt); claude/agy expose no equivalent.
fn api_key_vars(engine: EngineKind) -> &'static [&'static str] {
    match engine {
        EngineKind::Claude => &["ANTHROPIC_API_KEY"],
        EngineKind::Agy => &["GEMINI_API_KEY", "GOOGLE_API_KEY", "GOOGLE_APPLICATION_CREDENTIALS"],
        EngineKind::Codex => &[],
    }
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
            server: ServerConfig { bind: "127.0.0.1:8090".into(), bearer_token: None,
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

    fn codex_agentic(ws: &str, trusted: bool) -> ModelEntry {
        ModelEntry { id: "c".into(), engine: EngineKind::Codex, model: Some("gpt-5".into()),
            workspace: Some(PathBuf::from(ws)), mode: Mode::Agentic, permissions: Some("workspace-write".into()),
            trusted_caller_only: trusted }
    }

    #[test]
    fn loopback_without_token_is_ok() {
        assert!(validate_config(&base()).is_ok());
    }

    #[test]
    fn non_loopback_without_token_is_refused() {
        let mut cfg = base();
        cfg.server.bind = "0.0.0.0:8090".into();
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("non-loopback")), "{err:?}");
    }

    #[test]
    fn non_loopback_with_placeholder_token_is_refused() {
        let mut cfg = base();
        cfg.server.bind = "0.0.0.0:8090".into();
        cfg.server.bearer_token = Some("sk-change-me".into());
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("non-default")), "{err:?}");
    }

    #[test]
    fn non_loopback_with_strong_token_is_ok() {
        let mut cfg = base();
        cfg.server.bind = "0.0.0.0:8090".into();
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
    fn codex_and_agy_engines_are_allowed() {
        let mut cfg = base();
        cfg.models = vec![
            ModelEntry { id: "ct".into(), engine: EngineKind::Codex, model: None, workspace: None,
                mode: Mode::Text, permissions: None, trusted_caller_only: false },
            ModelEntry { id: "at".into(), engine: EngineKind::Agy, model: None, workspace: None,
                mode: Mode::Text, permissions: None, trusted_caller_only: false },
        ];
        assert!(validate_config(&cfg).is_ok(), "{:?}", validate_config(&cfg));
    }

    #[test]
    fn codex_agentic_without_trusted_and_no_sandbox_is_refused() {
        let mut cfg = base();
        cfg.models = vec![codex_agentic("/work/repoB", false)];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("trusted_caller_only")), "{err:?}");
    }

    #[test]
    fn bubblewrap_backend_is_allowed_and_drops_trusted_requirement() {
        let mut cfg = base();
        cfg.defaults.sandbox_backend = SandboxBackend::Bubblewrap;
        cfg.models = vec![agentic("/work/repoA", false)]; // not trusted, but sandboxed -> OK
        assert!(validate_config(&cfg).is_ok(), "{:?}", validate_config(&cfg));
    }

    #[test]
    fn container_backend_is_refused_as_not_implemented() {
        let mut cfg = base();
        cfg.defaults.sandbox_backend = SandboxBackend::Container;
        cfg.models = vec![agentic("/work/repoA", true)];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("container") && m.contains("not yet implemented")), "{err:?}");
    }

    #[test]
    fn claude_agentic_with_api_key_env_and_no_sandbox_is_refused() {
        let mut cfg = base();
        cfg.defaults.env_passthrough = vec!["PATH".into(), "ANTHROPIC_API_KEY".into()];
        cfg.models = vec![agentic("/work/repoA", true)];
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.iter().any(|m| m.contains("API-key env")), "{err:?}");
    }

    #[test]
    fn codex_agentic_with_api_key_env_is_allowed() {
        // codex scrubs via shell_environment_policy, so env-key auth is fine for it.
        let mut cfg = base();
        cfg.defaults.env_passthrough = vec!["PATH".into(), "OPENAI_API_KEY".into()];
        cfg.models = vec![codex_agentic("/work/repoB", true)];
        assert!(validate_config(&cfg).is_ok(), "{:?}", validate_config(&cfg));
    }

    #[test]
    fn claude_api_key_env_with_bubblewrap_is_allowed() {
        let mut cfg = base();
        cfg.defaults.sandbox_backend = SandboxBackend::Bubblewrap;
        cfg.defaults.env_passthrough = vec!["PATH".into(), "ANTHROPIC_API_KEY".into()];
        cfg.models = vec![agentic("/work/repoA", false)];
        assert!(validate_config(&cfg).is_ok(), "{:?}", validate_config(&cfg));
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
