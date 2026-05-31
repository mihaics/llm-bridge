//! In-memory session index: maps a content-hash key to the native claude session id, so a
//! follow-up request can resume the agent's own on-disk session. Restart loses the index and
//! correctly degrades to the full-transcript miss path (spec §4.5). The key folds in the
//! conversation projection plus ModelEntry, system-prompt, tool-config, and runtime fingerprints.
use crate::config::ModelEntry;
use crate::openai::{ChatMessage, Role};
use crate::registry::model_entry_fingerprint;
use std::collections::HashMap;
use std::sync::Mutex;

/// Runtime context a native session id only makes sense within (the engine's cred/home dir +
/// sandbox backend). `engine_home` is `CLAUDE_CONFIG_DIR` / `CODEX_HOME` / agy's config context.
#[derive(Debug, Clone)]
pub struct RuntimeFingerprint {
    pub engine_home: Option<String>,
    pub sandbox_backend: String,
}

impl RuntimeFingerprint {
    fn canon(&self) -> String {
        format!("home={:?};sandbox={}", self.engine_home, self.sandbox_backend)
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
/// Tool calls (name + canonicalized args) and tool_call_id are folded in so two threads that differ
/// only in tool activity hash differently (spec §4.5).
fn project(messages: &[ChatMessage]) -> String {
    messages
        .iter()
        .map(|m| {
            let mut s = format!("{:?}:{}", m.role, m.text());
            // Fold the assistant's tool_calls (name + canonicalized args) so two threads that differ
            // only in tool activity don't hash identically (spec §4.5).
            if let Some(calls) = &m.tool_calls {
                for c in calls {
                    s.push_str(&format!("|tc:{}:{}:{}", c.id, c.function.name, c.function.arguments));
                }
            }
            if let Some(id) = &m.tool_call_id {
                s.push_str(&format!("|tcid:{id}"));
            }
            s
        })
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

/// Key to LOOK UP on an incoming request: hash of the prefix BEFORE the final user turn. (Tool-result
/// follow-ups — threads ending in a `role:"tool"` suffix — are routed to the suspended-session
/// registry in the HTTP layer and never reach this function.)
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
        tool_calls: None,
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
        RuntimeFingerprint { engine_home: Some("/cred".into()), sandbox_backend: "none".into() }
    }
    fn msg(role: Role, t: &str) -> ChatMessage {
        ChatMessage { role, content: Some(MessageContent::Text(t.into())), tool_call_id: None, tool_calls: None }
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

    #[test]
    fn key_folds_tool_calls_so_post_tool_threads_differ() {
        // A follow-up USER turn after past tool use: the two threads' PREFIXES (the history the
        // lookup key hashes) differ only in the historical assistant tool_calls / tool result, so
        // `project()` folding those must make the keys differ (else two distinct post-tool threads
        // would resume the same native session — spec §4.5).
        let mut a = vec![msg(Role::User, "do it")];
        a.push(ChatMessage { role: Role::Assistant, content: None, tool_call_id: None,
            tool_calls: Some(vec![crate::openai::ToolCall { id: "c1".into(), kind: "function".into(),
                function: crate::openai::FunctionCall { name: "f".into(), arguments: "{\"x\":1}".into() } }]) });
        a.push(ChatMessage { role: Role::Tool, content: Some(MessageContent::Text("R1".into())),
            tool_call_id: Some("c1".into()), tool_calls: None });
        a.push(msg(Role::User, "and now?")); // ends with a user turn -> lookup_key keys the prefix above
        let mut b = vec![msg(Role::User, "do it")];
        b.push(ChatMessage { role: Role::Assistant, content: None, tool_call_id: None,
            tool_calls: Some(vec![crate::openai::ToolCall { id: "c1".into(), kind: "function".into(),
                function: crate::openai::FunctionCall { name: "f".into(), arguments: "{\"x\":2}".into() } }]) });
        b.push(ChatMessage { role: Role::Tool, content: Some(MessageContent::Text("R2".into())),
            tool_call_id: Some("c1".into()), tool_calls: None });
        b.push(msg(Role::User, "and now?"));
        assert_ne!(lookup_key(&a, &entry(), None, &rt()), lookup_key(&b, &entry(), None, &rt()));
    }
}
