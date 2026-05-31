use http_body_util::BodyExt;
use llm_bridge::config::{EngineKind, Mode, ModelEntry};
use llm_bridge::engine::{AgentEvent, Turn};
use llm_bridge::http::{build_router, AppState};
use llm_bridge::orchestrator::{RunFuture, TurnRunner};
use llm_bridge::registry::Registry;
use std::sync::Arc;
use tower::ServiceExt; // oneshot

/// A canned runner so handler tests are hermetic (no real claude).
struct FakeRunner {
    events: Vec<AgentEvent>,
}
impl TurnRunner for FakeRunner {
    fn run<'a>(&'a self, _entry: &'a ModelEntry, _turn: Turn) -> RunFuture<'a> {
        let events = self.events.clone();
        Box::pin(async move { Ok(events) })
    }
}

fn state_with(token: Option<&str>, runner: Arc<dyn TurnRunner>) -> AppState {
    let models = vec![ModelEntry {
        id: "claude-text".into(), engine: EngineKind::Claude, model: Some("sonnet".into()),
        workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false,
    }];
    AppState { registry: Arc::new(Registry::new(models)), bearer_token: token.map(String::from), runner }
}

fn state(token: Option<&str>) -> AppState {
    state_with(token, Arc::new(FakeRunner { events: vec![] }))
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn health_ok() {
    let app = build_router(state(None));
    let resp = app.oneshot(axum::http::Request::get("/health").body(axum::body::Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn models_lists_registered_models() {
    let app = build_router(state(None));
    let resp = app.oneshot(axum::http::Request::get("/v1/models").body(axum::body::Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert!(body_string(resp).await.contains("\"claude-text\""));
}

#[tokio::test]
async fn missing_bearer_token_is_401() {
    let app = build_router(state(Some("sk-secret-strong-token-1234")));
    let resp = app.oneshot(axum::http::Request::get("/v1/models").body(axum::body::Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn correct_bearer_token_passes() {
    let app = build_router(state(Some("sk-secret-strong-token-1234")));
    let resp = app.oneshot(
        axum::http::Request::get("/v1/models")
            .header("authorization", "Bearer sk-secret-strong-token-1234")
            .body(axum::body::Body::empty()).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn chat_success_returns_completion() {
    let runner = Arc::new(FakeRunner { events: vec![
        AgentEvent::SessionId("s".into()),
        AgentEvent::AssistantText("pong".into()),
        AgentEvent::Done { finish_reason: "stop".into() },
    ]});
    let app = build_router(state_with(None, runner));
    let body = r#"{"model":"claude-text","messages":[{"role":"user","content":"hi"}]}"#;
    let resp = app.oneshot(
        axum::http::Request::post("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body)).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), 200);
    let b = body_string(resp).await;
    assert!(b.contains("\"pong\""), "{b}");
    assert!(b.contains("chat.completion"), "{b}");
}

#[tokio::test]
async fn chat_unknown_model_is_404() {
    let app = build_router(state(None));
    let body = r#"{"model":"nope","messages":[{"role":"user","content":"hi"}]}"#;
    let resp = app.oneshot(
        axum::http::Request::post("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body)).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), 404);
    assert!(body_string(resp).await.contains("unknown model"));
}

#[tokio::test]
async fn chat_streaming_is_rejected_in_phase1() {
    let app = build_router(state(None));
    let body = r#"{"model":"claude-text","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let resp = app.oneshot(
        axum::http::Request::post("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body)).unwrap(),
    ).await.unwrap();
    assert_eq!(resp.status(), 400);
    assert!(body_string(resp).await.contains("streaming"));
}
