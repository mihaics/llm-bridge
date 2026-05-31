//! In-memory session index: maps a content-hash key to the native claude session id, so a
//! follow-up request can resume the agent's own on-disk session. Restart loses the index and
//! correctly degrades to the full-transcript miss path (spec §4.5). The key folds in the
//! conversation projection plus ModelEntry, system-prompt, tool-config, and runtime fingerprints.
use crate::config::ModelEntry;
use crate::openai::{ChatMessage, Role};
use crate::registry::model_entry_fingerprint;
use std::collections::HashMap;
use std::sync::Mutex;

/// Runtime context a native session id only makes sense within (cred dir + sandbox backend).
#[derive(Debug, Clone)]
pub struct RuntimeFingerprint {
    pub claude_config_dir: Option<String>,
    pub sandbox_backend: String,
}

impl RuntimeFingerprint {
    fn canon(&self) -> String {
        format!("cfg={:?};sandbox={}", self.claude_config_dir, self.sandbox_backend)
    }
}

#[derive(Default)]
pub struct SessionStore {
    map: Mutex<HashMap<String, String>>,
}

impl SessionStore {
    pub fn new() -> Self {
        SessionStore { map: Mutex::new(HashMap::new()) }
    }
    pub fn get(&self, key: &str) -> Option<String> {
        self.map.lock().unwrap().get(key).cloned()
    }
    pub fn insert(&self, key: String, session_id: String) {
        self.map.lock().unwrap().insert(key, session_id);
    }
}

/// Canonical projection of a message list: role + flattened content per message, newline-joined.
/// (Phase 2 has no client-supplied tool_calls; Phase 4 will extend this with tool_call ids.)
fn project(messages: &[ChatMessage]) -> String {
    messages
        .iter()
        .map(|m| format!("{:?}:{}", m.role, m.text()))
        .collect::<Vec<_>>()
        .join("\n")
}

fn hash(parts: &[&str]) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for p in parts {
        for &b in p.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h ^= 0xff; // separator so ["ab","c"] != ["a","bc"]
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{h:016x}")
}

fn key_for(prefix: &[ChatMessage], entry: &ModelEntry, system_prompt: Option<&str>, rt: &RuntimeFingerprint) -> String {
    hash(&[
        &project(prefix),
        &model_entry_fingerprint(entry),
        system_prompt.unwrap_or(""),
        // tool-config fingerprint: empty in Phase 2 (no client tools); reserved slot for Phase 4.
        "",
        &rt.canon(),
    ])
}

/// Index of the final user message in the list (the live instruction), if any.
fn final_user_idx(messages: &[ChatMessage]) -> Option<usize> {
    messages.iter().rposition(|m| m.role == Role::User)
}

/// Key to LOOK UP on an incoming request: hash of the prefix BEFORE the final user turn.
pub fn lookup_key(messages: &[ChatMessage], entry: &ModelEntry, system_prompt: Option<&str>, rt: &RuntimeFingerprint) -> String {
    let cut = final_user_idx(messages).unwrap_or(messages.len());
    key_for(&messages[..cut], entry, system_prompt, rt)
}

/// Key to STORE after a turn completes: the prefix the NEXT request will look up = the current
/// messages plus the assistant reply we just produced.
pub fn stored_key_after(messages: &[ChatMessage], assistant_text: &str, entry: &ModelEntry, system_prompt: Option<&str>, rt: &RuntimeFingerprint) -> String {
    let mut extended = messages.to_vec();
    extended.push(ChatMessage {
        role: Role::Assistant,
        content: Some(crate::openai::MessageContent::Text(assistant_text.to_string())),
        tool_call_id: None,
    });
    key_for(&extended, entry, system_prompt, rt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EngineKind, Mode, ModelEntry};
    use crate::openai::{ChatMessage, MessageContent, Role};

    fn entry() -> ModelEntry {
        ModelEntry { id: "m".into(), engine: EngineKind::Claude, model: Some("opus".into()),
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false }
    }
    fn rt() -> RuntimeFingerprint {
        RuntimeFingerprint { claude_config_dir: Some("/cred".into()), sandbox_backend: "none".into() }
    }
    fn msg(role: Role, t: &str) -> ChatMessage {
        ChatMessage { role, content: Some(MessageContent::Text(t.into())), tool_call_id: None }
    }

    #[test]
    fn store_roundtrips() {
        let s = SessionStore::new();
        s.insert("k".into(), "sid-1".into());
        assert_eq!(s.get("k"), Some("sid-1".to_string()));
        assert_eq!(s.get("missing"), None);
    }

    #[test]
    fn lookup_prefix_excludes_final_user_turn() {
        // First turn: only one user message -> lookup prefix is empty.
        let m1 = vec![msg(Role::User, "first")];
        let k1 = lookup_key(&m1, &entry(), None, &rt());
        // After the turn, the stored key includes the user turn + assistant reply.
        let stored = stored_key_after(&m1, "answer one", &entry(), None, &rt());
        // Second turn resends history; its lookup prefix == the stored prefix -> SAME key.
        let m2 = vec![msg(Role::User, "first"), msg(Role::Assistant, "answer one"), msg(Role::User, "second")];
        let k2 = lookup_key(&m2, &entry(), None, &rt());
        assert_ne!(k1, k2);
        assert_eq!(stored, k2, "stored key must equal next turn's lookup key (resume hit)");
    }

    #[test]
    fn key_changes_with_system_prompt_and_runtime_and_tools() {
        let m = vec![msg(Role::User, "x")];
        let base = lookup_key(&m, &entry(), None, &rt());
        let with_sys = lookup_key(&m, &entry(), Some("sys"), &rt());
        let mut rt2 = rt(); rt2.sandbox_backend = "bubblewrap".into();
        let with_rt = lookup_key(&m, &entry(), None, &rt2);
        assert_ne!(base, with_sys);
        assert_ne!(base, with_rt);
    }
}
