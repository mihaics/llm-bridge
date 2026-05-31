//! Configuration schema, YAML loader, and leading-tilde path expansion.
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub credentials: Credentials,
    #[serde(default)]
    pub models: Vec<ModelEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
    #[serde(default)]
    pub bearer_token: Option<String>,
    #[serde(default)]
    pub progress_channel: ProgressChannel,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Defaults {
    #[serde(default = "default_timeout_s")]
    pub timeout_s: u64,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
    #[serde(default)]
    pub sandbox_backend: SandboxBackend,
    #[serde(default = "default_env_passthrough")]
    pub env_passthrough: Vec<String>,
}

impl Default for Defaults {
    fn default() -> Self {
        Defaults {
            timeout_s: default_timeout_s(),
            max_concurrency: default_max_concurrency(),
            sandbox_backend: SandboxBackend::default(),
            env_passthrough: default_env_passthrough(),
        }
    }
}

fn default_timeout_s() -> u64 { 600 }
fn default_max_concurrency() -> usize { 4 }
/// Non-secret vars the spawned CLI needs to run. Everything else is scrubbed (env_clear).
pub fn default_env_passthrough() -> Vec<String> {
    ["PATH", "HOME", "LANG", "LC_ALL", "TERM"].iter().map(|s| s.to_string()).collect()
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Credentials {
    #[serde(default)]
    pub claude_config_dir: Option<PathBuf>,
    #[serde(default)]
    pub codex_home: Option<PathBuf>,
    #[serde(default)]
    pub agy_config_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelEntry {
    pub id: String,
    pub engine: EngineKind,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub workspace: Option<PathBuf>,
    pub mode: Mode,
    #[serde(default)]
    pub permissions: Option<String>,
    #[serde(default)]
    pub trusted_caller_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EngineKind { Claude, Codex, Agy }

impl EngineKind {
    /// Canonical lowercase name (matches `rename_all = "lowercase"` and the config YAML), for
    /// operator-facing messages — avoids leaking the PascalCase `Debug` form.
    pub fn as_str(&self) -> &'static str {
        match self {
            EngineKind::Claude => "claude",
            EngineKind::Codex => "codex",
            EngineKind::Agy => "agy",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode { Agentic, Text }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxBackend { #[default] None, Bubblewrap, Container }

impl SandboxBackend {
    /// Canonical lowercase name (matches `rename_all = "lowercase"`); used for the session-key
    /// runtime fingerprint, which must stay stable against future enum renames.
    pub fn as_str(&self) -> &'static str {
        match self {
            SandboxBackend::None => "none",
            SandboxBackend::Bubblewrap => "bubblewrap",
            SandboxBackend::Container => "container",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressChannel { #[default] ReasoningContent, Omit }

/// Expand a leading `~/` to `$HOME`. Other paths returned unchanged.
pub fn expand_tilde(p: &Path) -> PathBuf {
    if let Ok(rest) = p.strip_prefix("~") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    p.to_path_buf()
}

fn expand_paths(cfg: &mut Config) {
    for slot in [
        &mut cfg.credentials.claude_config_dir,
        &mut cfg.credentials.codex_home,
        &mut cfg.credentials.agy_config_dir,
    ] {
        if let Some(p) = slot.as_ref() {
            *slot = Some(expand_tilde(p));
        }
    }
    for m in &mut cfg.models {
        if let Some(ws) = m.workspace.as_ref() {
            m.workspace = Some(expand_tilde(ws));
        }
    }
}

/// Parse config from a YAML string (no tilde expansion — used by tests).
pub fn parse_config(yaml: &str) -> anyhow::Result<Config> {
    Ok(serde_yaml::from_str(yaml)?)
}

/// Load config from a file path, expanding leading `~/` in credential and workspace paths.
pub fn load_config(path: &Path) -> anyhow::Result<Config> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("reading config {}: {e}", path.display()))?;
    let mut cfg = parse_config(&text)?;
    expand_paths(&mut cfg);
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    const YAML: &str = r#"
server:
  bind: "127.0.0.1:8088"
  bearer_token: "sk-test"
  progress_channel: reasoning_content
defaults:
  timeout_s: 300
  max_concurrency: 2
  sandbox_backend: none
  env_passthrough: ["PATH", "HOME"]
credentials:
  claude_config_dir: /home/u/.llm-bridge/cred/claude
models:
  - id: "claude-text"
    engine: claude
    model: sonnet
    mode: text
  - id: "claude-agentic"
    engine: claude
    model: opus
    workspace: /work/repoA
    mode: agentic
    permissions: workspace-write
    trusted_caller_only: true
"#;

    #[test]
    fn parses_full_config() {
        let cfg = parse_config(YAML).unwrap();
        assert_eq!(cfg.server.bind, "127.0.0.1:8088");
        assert_eq!(cfg.server.bearer_token.as_deref(), Some("sk-test"));
        assert_eq!(cfg.defaults.timeout_s, 300);
        assert_eq!(cfg.defaults.sandbox_backend, SandboxBackend::None);
        assert_eq!(cfg.defaults.env_passthrough, vec!["PATH".to_string(), "HOME".to_string()]);
        assert_eq!(cfg.models.len(), 2);
        assert_eq!(cfg.models[0].mode, Mode::Text);
        assert_eq!(cfg.models[1].engine, EngineKind::Claude);
        assert!(cfg.models[1].trusted_caller_only);
    }

    #[test]
    fn applies_defaults_for_optional_fields() {
        let yaml = r#"
server: { bind: "127.0.0.1:9000" }
models:
  - { id: "m", engine: claude, mode: text }
"#;
        let cfg = parse_config(yaml).unwrap();
        assert_eq!(cfg.defaults.timeout_s, 600);
        assert_eq!(cfg.defaults.max_concurrency, 4);
        assert_eq!(cfg.defaults.env_passthrough, default_env_passthrough());
        assert!(!cfg.models[0].trusted_caller_only);
        assert_eq!(cfg.server.progress_channel, ProgressChannel::ReasoningContent);
    }

    #[test]
    fn expands_leading_tilde() {
        std::env::set_var("HOME", "/home/test");
        assert_eq!(expand_tilde(&PathBuf::from("~/.llm-bridge/x")), PathBuf::from("/home/test/.llm-bridge/x"));
        assert_eq!(expand_tilde(&PathBuf::from("/abs/path")), PathBuf::from("/abs/path"));
    }
}
