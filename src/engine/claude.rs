//! ClaudeAdapter (full impl in Task 8).
use super::{AgentEvent, EngineError, Turn};
use std::path::PathBuf;

pub struct ClaudeAdapter { pub bin: String, pub config_dir: Option<PathBuf> }

impl ClaudeAdapter {
    pub fn new(bin: &str, config_dir: Option<PathBuf>) -> Self {
        ClaudeAdapter { bin: bin.into(), config_dir }
    }
    pub fn build_command(&self, _turn: &Turn, _env_passthrough: &[String]) -> (tokio::process::Command, Option<String>) {
        (tokio::process::Command::new(&self.bin), None)
    }
    pub fn parse_output(&self, _stdout: &str) -> Result<Vec<AgentEvent>, EngineError> {
        Ok(vec![])
    }
}
