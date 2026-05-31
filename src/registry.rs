//! Model registry: resolve a request `model` id to its `ModelEntry`, serve `/v1/models`,
//! and compute the ModelEntry fingerprint that (in Phase 2) feeds the session key.
use crate::config::ModelEntry;
use serde_json::json;

pub struct Registry {
    models: Vec<ModelEntry>,
}

impl Registry {
    pub fn new(models: Vec<ModelEntry>) -> Self {
        Registry { models }
    }

    pub fn resolve(&self, id: &str) -> Option<&ModelEntry> {
        self.models.iter().find(|m| m.id == id)
    }

    pub fn models(&self) -> &[ModelEntry] {
        &self.models
    }

    /// OpenAI `/v1/models` body: `{ "object": "list", "data": [ { id, object, owned_by } ] }`.
    pub fn models_json(&self) -> serde_json::Value {
        let data: Vec<_> = self
            .models
            .iter()
            .map(|m| json!({ "id": m.id, "object": "model", "owned_by": "llm-bridge" }))
            .collect();
        json!({ "object": "list", "data": data })
    }
}

/// Stable fingerprint over the resolution-relevant fields of a ModelEntry. Defined and tested
/// here so Phase 2's session key can rely on it.
pub fn model_entry_fingerprint(m: &ModelEntry) -> String {
    let canon = json!({
        "engine": format!("{:?}", m.engine),
        "model": m.model,
        "workspace": m.workspace,
        "mode": format!("{:?}", m.mode),
        "permissions": m.permissions,
    });
    let s = serde_json::to_string(&canon).expect("fingerprint serialize");
    format!("{:016x}", fnv1a(s.as_bytes()))
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EngineKind, Mode, ModelEntry};

    fn entry(id: &str) -> ModelEntry {
        ModelEntry {
            id: id.into(), engine: EngineKind::Claude, model: Some("opus".into()),
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false,
        }
    }

    #[test]
    fn resolves_by_id() {
        let reg = Registry::new(vec![entry("a"), entry("b")]);
        assert_eq!(reg.resolve("b").unwrap().id, "b");
        assert!(reg.resolve("missing").is_none());
    }

    #[test]
    fn models_json_has_openai_list_shape() {
        let reg = Registry::new(vec![entry("a")]);
        let v = reg.models_json();
        assert_eq!(v["object"], "list");
        assert_eq!(v["data"][0]["id"], "a");
        assert_eq!(v["data"][0]["object"], "model");
    }

    #[test]
    fn fingerprint_changes_with_workspace() {
        let mut a = entry("x");
        let mut b = entry("x");
        b.workspace = Some("/different".into());
        assert_ne!(model_entry_fingerprint(&a), model_entry_fingerprint(&b));
        a.workspace = Some("/different".into());
        assert_eq!(model_entry_fingerprint(&a), model_entry_fingerprint(&b));
    }
}
