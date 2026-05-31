//! AgyAdapter (full impl in a later task).
use super::{AgentEvent, Turn};
use std::path::PathBuf;
use tokio::process::Command;

pub struct AgyAdapter {
    pub bin: String,
    pub config_dir: Option<PathBuf>,
}

impl AgyAdapter {
    pub fn new(bin: &str, config_dir: Option<PathBuf>) -> Self {
        AgyAdapter { bin: bin.into(), config_dir }
    }
    pub fn build_stream_command(&self, _turn: &Turn, _env_passthrough: &[String]) -> (Command, Option<String>) {
        (Command::new(&self.bin), None)
    }
    pub fn parse_stream_line(&self, _line: &str) -> Vec<AgentEvent> {
        Vec::new()
    }
}
