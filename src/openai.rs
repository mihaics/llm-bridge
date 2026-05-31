//! OpenAI Chat Completions wire types (request, response, error).
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    Developer,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ContentPart {
    #[serde(default)]
    pub text: String,
}

/// OpenAI message `content` is either a plain string or an array of parts.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    Parts(Vec<ContentPart>),
}

impl MessageContent {
    pub fn flatten(&self) -> String {
        match self {
            MessageContent::Text(s) => s.clone(),
            MessageContent::Parts(parts) => parts
                .iter()
                .map(|p| p.text.as_str())
                .collect::<Vec<_>>()
                .join(" "),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatMessage {
    pub role: Role,
    #[serde(default)]
    pub content: Option<MessageContent>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub tool_calls: Option<Vec<ToolCall>>,
}

impl ChatMessage {
    /// Flattened text content ("" if none).
    pub fn text(&self) -> String {
        self.content.as_ref().map(|c| c.flatten()).unwrap_or_default()
    }
}

/// An assistant tool call (OpenAI function-calling). `arguments` is a JSON-encoded string.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type", default = "function_kind")]
    pub kind: String, // "function"
    pub function: FunctionCall,
}

/// The called function. `arguments` is a JSON-encoded **string** (OpenAI's wire shape), not a
/// structured value — pass it through verbatim.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: String,
}

fn function_kind() -> String { "function".to_string() }

#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    #[serde(default)]
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: Option<bool>,
    /// Accepted but unused in Phase 1 (MCP bridge is Phase 4).
    #[serde(default)]
    pub tools: Option<serde_json::Value>,
}

impl ChatCompletionRequest {
    pub fn is_streaming(&self) -> bool {
        self.stream.unwrap_or(false)
    }
    pub fn has_tools(&self) -> bool {
        matches!(&self.tools, Some(serde_json::Value::Array(a)) if !a.is_empty())
    }
}

// ---- Response ----

#[derive(Debug, Clone, Serialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub object: &'static str, // "chat.completion"
    pub created: u64,
    pub model: String,
    pub choices: Vec<Choice>,
    pub usage: Usage,
}

#[derive(Debug, Clone, Serialize)]
pub struct Choice {
    pub index: u32,
    pub message: ResponseMessage,
    pub finish_reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResponseMessage {
    pub role: &'static str, // "assistant"
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

// ---- Streaming chunk ----

#[derive(Debug, Clone, Serialize)]
pub struct ChatCompletionChunk {
    pub id: String,
    pub object: &'static str, // "chat.completion.chunk"
    pub created: u64,
    pub model: String,
    pub choices: Vec<ChunkChoice>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChunkChoice {
    pub index: u32,
    pub delta: Delta,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Delta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<DeltaToolCall>>,
}

/// A streaming tool-call delta. `function` is always present (per OpenAI's schema); our bridge
/// emits a complete call in a single delta, so it always carries both `name` and `arguments`
/// (we never produce an empty `function: {}`).
#[derive(Debug, Clone, Serialize)]
pub struct DeltaToolCall {
    pub index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub kind: Option<&'static str>, // "function"
    pub function: DeltaFunctionCall,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeltaFunctionCall {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<String>,
}

// ---- Error (OpenAI shape) ----

#[derive(Debug, Clone, Serialize)]
pub struct ApiError {
    pub error: ApiErrorBody,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiErrorBody {
    pub message: String,
    #[serde(rename = "type")]
    pub kind: String,
}

impl ApiError {
    pub fn new(message: impl Into<String>, kind: impl Into<String>) -> Self {
        ApiError { error: ApiErrorBody { message: message.into(), kind: kind.into() } }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_string_content() {
        let json = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.model, "m");
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0].text(), "hi");
        assert!(!req.is_streaming());
    }

    #[test]
    fn deserializes_content_parts() {
        let json = r#"{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"a"},{"type":"text","text":"b"}]}]}"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.messages[0].text(), "a b");
    }

    #[test]
    fn detects_stream_and_tools() {
        let json = r#"{"model":"m","stream":true,"tools":[{"x":1}],"messages":[]}"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert!(req.is_streaming());
        assert!(req.has_tools());
    }

    #[test]
    fn serializes_error_in_openai_shape() {
        let e = ApiError::new("nope", "invalid_request_error");
        let s = serde_json::to_string(&e).unwrap();
        assert_eq!(s, r#"{"error":{"message":"nope","type":"invalid_request_error"}}"#);
    }

    #[test]
    fn parses_tool_result_followup_with_assistant_tool_calls() {
        // A follow-up: assistant message carrying tool_calls, then a role:"tool" result.
        let json = r#"{"model":"m","messages":[
            {"role":"user","content":"do it"},
            {"role":"assistant","content":null,"tool_calls":[
                {"id":"call_abc","type":"function","function":{"name":"search","arguments":"{\"q\":\"x\"}"}}]},
            {"role":"tool","tool_call_id":"call_abc","content":"result text"}
        ]}"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        let asst = &req.messages[1];
        let calls = asst.tool_calls.as_ref().unwrap();
        assert_eq!(calls[0].id, "call_abc");
        assert_eq!(calls[0].function.name, "search");
        assert_eq!(calls[0].function.arguments, r#"{"q":"x"}"#);
        let tool = &req.messages[2];
        assert_eq!(tool.role, Role::Tool);
        assert_eq!(tool.tool_call_id.as_deref(), Some("call_abc"));
        assert_eq!(tool.text(), "result text");
    }

    #[test]
    fn serializes_tool_calls_in_response_message() {
        let msg = ResponseMessage {
            role: "assistant",
            content: String::new(),
            tool_calls: Some(vec![ToolCall {
                id: "call_1".into(), kind: "function".into(),
                function: FunctionCall { name: "edit".into(), arguments: "{}".into() },
            }]),
        };
        let s = serde_json::to_string(&msg).unwrap();
        assert!(s.contains(r#""tool_calls":[{"id":"call_1","type":"function","function":{"name":"edit","arguments":"{}"}}]"#), "{s}");
        // No tool_calls -> field omitted.
        let plain = ResponseMessage { role: "assistant", content: "hi".into(), tool_calls: None };
        assert!(!serde_json::to_string(&plain).unwrap().contains("tool_calls"));
    }

    #[test]
    fn serializes_delta_tool_call() {
        let d = Delta { role: None, content: None, reasoning_content: None, tool_calls: Some(vec![DeltaToolCall {
            index: 0, id: Some("call_1".into()), kind: Some("function"),
            function: DeltaFunctionCall { name: Some("edit".into()), arguments: Some("{}".into()) },
        }])};
        let s = serde_json::to_string(&d).unwrap();
        assert!(s.contains(r#""tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"edit","arguments":"{}"}}]"#), "{s}");
    }
}
