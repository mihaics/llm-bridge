//! CodexAdapter (full impl in a later task).
use super::{AgentEvent, Turn};
use std::path::PathBuf;
use tokio::process::Command;

pub struct CodexAdapter {
    pub bin: String,
    pub codex_home: Option<PathBuf>,
}

impl CodexAdapter {
    pub fn new(bin: &str, codex_home: Option<PathBuf>) -> Self {
        CodexAdapter { bin: bin.into(), codex_home }
    }
    pub fn build_stream_command(&self, _turn: &Turn, _env_passthrough: &[String]) -> (Command, Option<String>) {
        (Command::new(&self.bin), None)
    }
    pub fn parse_stream_line(&self, _line: &str) -> Vec<AgentEvent> {
        Vec::new()
    }
}
