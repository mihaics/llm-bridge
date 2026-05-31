//! Tool-result routing by message-list SHAPE (spec §4.3 step 2). A request is a tool-result
//! follow-up ONLY when its trailing messages are a `role:"tool"` suffix (no user message after).
//! Historical tool messages before a final user turn are an ordinary turn, not a follow-up.
use crate::openai::{ChatMessage, Role};

/// If `messages` ends with one or more `role:"tool"` messages (a tool-result suffix), return the
/// `(tool_call_id, content)` pair for each in original order. Otherwise `None` (an ordinary turn).
pub fn tool_result_suffix(messages: &[ChatMessage]) -> Option<Vec<(String, String)>> {
    if messages.last().map(|m| m.role) != Some(Role::Tool) {
        return None;
    }
    let mut out: Vec<(String, String)> = Vec::new();
    for m in messages.iter().rev() {
        if m.role != Role::Tool {
            break;
        }
        // A missing tool_call_id collapses to "" — which matches no live group, so the registry's
        // `deliver` returns `Unknown` (HTTP 400). We surface the shape here; validity is its concern.
        out.push((m.tool_call_id.clone().unwrap_or_default(), m.text()));
    }
    out.reverse();
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openai::{ChatMessage, MessageContent, Role};

    fn msg(role: Role, text: &str, tcid: Option<&str>) -> ChatMessage {
        ChatMessage {
            role,
            content: Some(MessageContent::Text(text.into())),
            tool_call_id: tcid.map(String::from),
            tool_calls: None,
        }
    }

    #[test]
    fn trailing_tool_suffix_is_a_followup() {
        let msgs = vec![
            msg(Role::User, "do it", None),
            msg(Role::Assistant, "", None),
            msg(Role::Tool, "result A", Some("call_a")),
            msg(Role::Tool, "result B", Some("call_b")),
        ];
        let got = tool_result_suffix(&msgs).unwrap();
        assert_eq!(got, vec![("call_a".to_string(), "result A".to_string()),
                             ("call_b".to_string(), "result B".to_string())]);
    }

    #[test]
    fn historical_tool_then_user_turn_is_not_a_followup() {
        // Past tool exchange, but the thread ENDS with a new user message -> ordinary turn.
        let msgs = vec![
            msg(Role::User, "first", None),
            msg(Role::Assistant, "", None),
            msg(Role::Tool, "old result", Some("call_old")),
            msg(Role::Assistant, "answer", None),
            msg(Role::User, "second", None),
        ];
        assert!(tool_result_suffix(&msgs).is_none());
    }

    #[test]
    fn ends_with_assistant_or_empty_is_not_a_followup() {
        assert!(tool_result_suffix(&[msg(Role::Assistant, "hi", None)]).is_none());
        assert!(tool_result_suffix(&[]).is_none());
        assert!(tool_result_suffix(&[msg(Role::User, "hi", None)]).is_none());
    }
}
