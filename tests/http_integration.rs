use http_body_util::BodyExt;
use llm_bridge::config::{Defaults, EngineKind, Mode, ModelEntry, ProgressChannel};
use llm_bridge::engine::{AgentEvent, EventStream, Turn};
use llm_bridge::http::{build_router, AppState};
use llm_bridge::orchestrator::TurnRunner;
use llm_bridge::registry::Registry;
use llm_bridge::session::SessionStore;
use std::sync::Arc;
use tower::ServiceExt;

struct FakeRunner { events: Vec<AgentEvent> }
impl TurnRunner for FakeRunner {
    fn run_stream(&self, _turn: Turn) -> EventStream {
        Box::pin(futures::stream::iter(self.events.clone()))
    }
}

fn state_with(token: Option<&str>, runner: Arc<dyn TurnRunner>) -> AppState {
    let models = vec![ModelEntry {
        id: "claude-text".into(), engine: EngineKind::Claude, model: Some("sonnet".into()),
        workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false,
    }];
    AppState {
        registry: Arc::new(Registry::new(models)),
        bearer_token: token.map(String::from),
        runner,
        sessions: Arc::new(SessionStore::new()),
        defaults: Defaults::default(),
        progress_channel: ProgressChannel::ReasoningContent,
        claude_config_dir: None,
    }
}
fn fake(events: Vec<AgentEvent>) -> Arc<dyn TurnRunner> { Arc::new(FakeRunner { events }) }
fn state(token: Option<&str>) -> AppState { state_with(token, fake(vec![])) }

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn health_ok() {
    let app = build_router(state(None));
    let r = app.oneshot(axum::http::Request::get("/health").body(axum::body::Body::empty()).unwrap()).await.unwrap();
    assert_eq!(r.status(), 200);
}

#[tokio::test]
async fn models_lists_registered() {
    let app = build_router(state(None));
    let r = app.oneshot(axum::http::Request::get("/v1/models").body(axum::body::Body::empty()).unwrap()).await.unwrap();
    assert!(body_string(r).await.contains("claude-text"));
}

#[tokio::test]
async fn auth_enforced_on_v1() {
    let app = build_router(state(Some("sk-secret-strong-token-1234")));
    let r = app.oneshot(axum::http::Request::get("/v1/models").body(axum::body::Body::empty()).unwrap()).await.unwrap();
    assert_eq!(r.status(), 401);
}

#[tokio::test]
async fn nonstreaming_success() {
    let runner = fake(vec![
        AgentEvent::SessionId("s".into()),
        AgentEvent::Reasoning("think".into()),
        AgentEvent::AssistantText("pong".into()),
        AgentEvent::Done { finish_reason: "stop".into() },
    ]);
    let app = build_router(state_with(None, runner));
    let body = r#"{"model":"claude-text","messages":[{"role":"user","content":"hi"}]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    assert_eq!(r.status(), 200);
    let b = body_string(r).await;
    assert!(b.contains("\"pong\""), "{b}");
    assert!(b.contains("chat.completion"));
    assert!(!b.contains("think")); // reasoning not in non-streaming content
}

#[tokio::test]
async fn streaming_emits_sse_chunks_and_done() {
    let runner = fake(vec![
        AgentEvent::Reasoning("thinking".into()),
        AgentEvent::AssistantText("po".into()),
        AgentEvent::AssistantText("ng".into()),
        AgentEvent::Done { finish_reason: "stop".into() },
    ]);
    let app = build_router(state_with(None, runner));
    let body = r#"{"model":"claude-text","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers().get("content-type").unwrap(), "text/event-stream");
    let b = body_string(r).await;
    assert!(b.contains("chat.completion.chunk"), "{b}");
    assert!(b.contains("\"content\":\"po\""), "{b}");
    assert!(b.contains("\"reasoning_content\":\"thinking\""), "{b}");
    assert!(b.contains("\"finish_reason\":\"stop\""), "{b}");
    assert!(b.trim_end().ends_with("data: [DONE]"), "{b}");
}

#[tokio::test]
async fn streaming_omit_profile_drops_reasoning() {
    let runner = fake(vec![AgentEvent::Reasoning("secret-thoughts".into()), AgentEvent::AssistantText("hi".into()), AgentEvent::Done { finish_reason: "stop".into() }]);
    let mut st = state_with(None, runner);
    st.progress_channel = ProgressChannel::Omit;
    let app = build_router(st);
    let body = r#"{"model":"claude-text","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    let b = body_string(r).await;
    assert!(!b.contains("secret-thoughts"), "{b}");
    assert!(b.contains("\"content\":\"hi\""), "{b}");
}

#[tokio::test]
async fn unknown_model_404_and_tools_400() {
    let app = build_router(state(None));
    let r1 = app.clone().oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(r#"{"model":"nope","messages":[]}"#)).unwrap()).await.unwrap();
    assert_eq!(r1.status(), 404);
    let r2 = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(r#"{"model":"claude-text","tools":[{"x":1}],"messages":[]}"#)).unwrap()).await.unwrap();
    assert_eq!(r2.status(), 400);
}
