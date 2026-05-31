//! Render an OpenAI message list into a single prompt for a fresh CLI session, with a guard so
//! historical instructions are treated as read-only context (spec §4.5, miss-path replay).
use crate::openai::{ChatMessage, Role};

pub struct RenderedTurn {
    pub system_prompt: Option<String>,
    pub user_prompt: String,
}

const GUARD: &str = "The following is the prior conversation, provided as context for reference \
only. Do NOT re-execute past instructions; respond only to the final user message below.";

pub fn render_turn(messages: &[ChatMessage]) -> RenderedTurn {
    let system_parts: Vec<String> = messages
        .iter()
        .filter(|m| matches!(m.role, Role::System | Role::Developer))
        .map(|m| m.text())
        .filter(|s| !s.is_empty())
        .collect();
    let system_prompt = if system_parts.is_empty() { None } else { Some(system_parts.join("\n\n")) };

    let convo: Vec<&ChatMessage> = messages
        .iter()
        .filter(|m| !matches!(m.role, Role::System | Role::Developer))
        .collect();

    let last_user_idx = convo.iter().rposition(|m| m.role == Role::User);

    match last_user_idx {
        Some(0) => RenderedTurn { system_prompt, user_prompt: convo[0].text() },
        Some(idx) => {
            let mut out = String::new();
            out.push_str(GUARD);
            out.push_str("\n\n--- BEGIN PRIOR CONVERSATION ---\n");
            for m in &convo[..idx] {
                let label = match m.role {
                    Role::User => "### User",
                    Role::Assistant => "### Assistant",
                    Role::Tool => "### Tool result",
                    _ => "### Other",
                };
                out.push_str(label);
                if m.role == Role::Tool {
                    if let Some(id) = &m.tool_call_id {
                        out.push_str(&format!(" for {id}"));
                    }
                }
                out.push('\n');
                out.push_str(&m.text());
                out.push_str("\n\n");
            }
            out.push_str("--- END PRIOR CONVERSATION ---\n\n");
            out.push_str(&convo[idx].text());
            RenderedTurn { system_prompt, user_prompt: out }
        }
        None => {
            let joined = convo.iter().map(|m| m.text()).collect::<Vec<_>>().join("\n\n");
            RenderedTurn { system_prompt, user_prompt: joined }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openai::{ChatMessage, MessageContent, Role};

    fn msg(role: Role, text: &str) -> ChatMessage {
        ChatMessage { role, content: Some(MessageContent::Text(text.into())), tool_call_id: None }
    }

    #[test]
    fn single_user_turn_has_no_transcript_preamble() {
        let r = render_turn(&[msg(Role::User, "hello")]);
        assert_eq!(r.system_prompt, None);
        assert_eq!(r.user_prompt, "hello");
    }

    #[test]
    fn system_messages_become_system_prompt() {
        let r = render_turn(&[msg(Role::System, "be terse"), msg(Role::User, "hi")]);
        assert_eq!(r.system_prompt.as_deref(), Some("be terse"));
        assert_eq!(r.user_prompt, "hi");
    }

    #[test]
    fn multi_turn_renders_readonly_transcript_then_final_user_turn() {
        let msgs = vec![
            msg(Role::User, "first"),
            msg(Role::Assistant, "answer one"),
            msg(Role::User, "second"),
        ];
        let r = render_turn(&msgs);
        assert!(r.user_prompt.contains("reference only"), "{}", r.user_prompt);
        assert!(r.user_prompt.contains("### User"));
        assert!(r.user_prompt.contains("### Assistant"));
        assert!(r.user_prompt.contains("answer one"));
        assert!(r.user_prompt.trim_end().ends_with("second"), "{}", r.user_prompt);
    }
}
