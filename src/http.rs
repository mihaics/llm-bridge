//! HTTP layer: shared state, router, health/models, bearer auth, and the chat-completions handler
//! (SSE when `stream:true`, aggregated otherwise). Both paths consume the same event stream.
use crate::config::{Credentials, Defaults, EngineKind, ProgressChannel};
use crate::engine::AgentEvent;
use crate::openai::{
    ApiError, ChatCompletionChunk, ChatCompletionRequest, ChunkChoice, Delta,
};
use crate::orchestrator::{response_from_events, run_request, runtime_fingerprint, TurnRunner};
use crate::registry::Registry;
use crate::session::SessionStore;
use axum::{
    extract::State,
    http::{header::AUTHORIZATION, Request, StatusCode},
    middleware::{from_fn_with_state, Next},
    response::{sse::{Event, Sse}, IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use futures::{Stream, StreamExt};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    pub bearer_token: Option<String>,
    pub runner: Arc<dyn TurnRunner>,
    pub sessions: Arc<SessionStore>,
    pub defaults: Defaults,                // used for sandbox_backend in the runtime fingerprint
    pub progress_channel: ProgressChannel, // from cfg.server.progress_channel (NOT on Defaults)
    pub credentials: Credentials, // per-engine home dirs feed the runtime fingerprint
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/v1/models", get(models))
        .route("/v1/chat/completions", post(chat_completions))
        .route_layer(from_fn_with_state(state.clone(), auth_middleware))
        .route("/health", get(health))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn models(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.registry.models_json())
}

async fn auth_middleware(State(state): State<AppState>, req: Request<axum::body::Body>, next: Next) -> Response {
    let Some(expected) = state.bearer_token.as_deref().filter(|t| !t.is_empty()) else {
        return next.run(req).await;
    };
    let presented = req.headers().get(AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    if presented == Some(expected) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, Json(ApiError::new("missing or invalid bearer token", "invalid_request_error"))).into_response()
    }
}

async fn chat_completions(State(state): State<AppState>, Json(req): Json<ChatCompletionRequest>) -> Response {
    if req.has_tools() {
        return err(StatusCode::BAD_REQUEST, "the `tools` field is not implemented yet (Phase 4)", "invalid_request_error");
    }
    let Some(entry) = state.registry.resolve(&req.model) else {
        return err(StatusCode::NOT_FOUND, &format!("unknown model '{}'", req.model), "model_not_found");
    };
    let entry = entry.clone();
    let model_id = req.model.clone();
    let streaming = req.is_streaming();
    let progress = state.progress_channel;

    let engine_home = match entry.engine {
        EngineKind::Claude => state.credentials.claude_config_dir.clone(),
        EngineKind::Codex => state.credentials.codex_home.clone(),
        EngineKind::Agy => state.credentials.agy_config_dir.clone(),
    };
    let rt = runtime_fingerprint(&engine_home, state.defaults.sandbox_backend.as_str());
    let events = run_request(state.runner.clone(), state.sessions.clone(), &entry, &req, &rt);

    if streaming {
        sse_response(events, model_id, progress).into_response()
    } else {
        let collected: Vec<AgentEvent> = events.collect().await;
        match response_from_events(collected, &model_id) {
            Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
            Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "engine_error"),
        }
    }
}

/// Map the event stream to OpenAI `chat.completion.chunk` SSE frames.
fn sse_response(
    events: crate::engine::EventStream,
    model_id: String,
    progress: ProgressChannel,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let id = format!("chatcmpl-{}", unix_now());
    let created = unix_now();
    let mut role_sent = false;

    let stream = async_stream::stream! {
        futures::pin_mut!(events);
        while let Some(ev) = events.next().await {
            let (delta, finish) = match ev {
                AgentEvent::AssistantText(t) => {
                    let role = (!role_sent).then(|| { role_sent = true; "assistant" });
                    (Some(Delta { role, content: Some(t), reasoning_content: None, tool_calls: None }), None)
                }
                AgentEvent::Reasoning(t) | AgentEvent::ToolStart { name: t, .. } | AgentEvent::ToolResult { summary: t } => {
                    match progress {
                        ProgressChannel::ReasoningContent => (Some(Delta { role: None, content: None, reasoning_content: Some(t), tool_calls: None }), None),
                        ProgressChannel::Omit => (None, None),
                    }
                }
                AgentEvent::Done { finish_reason } => (Some(Delta::default()), Some(finish_reason)),
                AgentEvent::Error(m) => {
                    // Surface as a final assistant note + stop; SSE has no error frame.
                    (Some(Delta { role: None, content: Some(format!("[error: {m}]")), reasoning_content: None, tool_calls: None }), Some("stop".to_string()))
                }
                AgentEvent::SessionId(_) => (None, None),
                AgentEvent::ToolCall { .. } => (None, None), // mapped to tool_calls in a later task
            };
            if let Some(delta) = delta {
                let chunk = ChatCompletionChunk {
                    id: id.clone(), object: "chat.completion.chunk", created, model: model_id.clone(),
                    choices: vec![ChunkChoice { index: 0, delta, finish_reason: finish.clone() }],
                };
                yield Ok(Event::default().data(serde_json::to_string(&chunk).unwrap()));
            }
        }
        yield Ok(Event::default().data("[DONE]"));
    };
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(15)))
}

fn err(status: StatusCode, message: &str, kind: &str) -> Response {
    (status, Json(ApiError::new(message, kind))).into_response()
}

fn unix_now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}
