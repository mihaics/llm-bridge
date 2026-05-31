use http_body_util::BodyExt;
use llm_bridge::config::{Credentials, Defaults, EngineKind, Mode, ModelEntry, ProgressChannel};
use llm_bridge::engine::{AgentEvent, EventStream, Turn};
use llm_bridge::http::{build_router, AppState};
use llm_bridge::orchestrator::TurnRunner;
use llm_bridge::process::ProcessSupervisor;
use llm_bridge::registry::Registry;
use llm_bridge::session::SessionStore;
use llm_bridge::suspend::SuspendedSessions;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

struct FakeRunner { events: Vec<AgentEvent> }
impl TurnRunner for FakeRunner {
    fn run_stream(&self, _turn: Turn) -> EventStream {
        Box::pin(futures::stream::iter(self.events.clone()))
    }
}

fn state_with(token: Option<&str>, runner: Arc<dyn TurnRunner>) -> AppState {
    let models = vec![
        ModelEntry { id: "claude-text".into(), engine: EngineKind::Claude, model: Some("sonnet".into()),
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false },
        ModelEntry { id: "codex-text".into(), engine: EngineKind::Codex, model: Some("gpt-5".into()),
            workspace: None, mode: Mode::Text, permissions: None, trusted_caller_only: false },
    ];
    AppState {
        registry: Arc::new(Registry::new(models)),
        bearer_token: token.map(String::from),
        runner,
        sessions: Arc::new(SessionStore::new()),
        defaults: Defaults::default(),
        progress_channel: ProgressChannel::ReasoningContent,
        credentials: Credentials::default(),
        suspended: Arc::new(SuspendedSessions::new(8)),
        tool_result_timeout: Duration::from_secs(120),
        // Intentionally independent of the runner: FakeRunner bypasses the supervisor, so no
        // permits flow through it. In `main` ONE ProcessSupervisor is cloned into both the runner
        // and AppState (verified by the Arc::ptr_eq test in process.rs). A future hermetic
        // happy-path tools-turn test would need to share a real runner's supervisor here.
        supervisor: ProcessSupervisor::new(4),
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
async fn streaming_emits_tool_calls_chunk() {
    let runner = fake(vec![
        AgentEvent::ToolCall { id: "call_1".into(), name: "search".into(), args: "{}".into() },
        AgentEvent::Done { finish_reason: "tool_calls".into() },
    ]);
    let app = build_router(state_with(None, runner));
    let body = r#"{"model":"claude-text","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    let b = body_string(r).await;
    assert!(b.contains(r#""tool_calls":[{"index":0,"id":"call_1","type":"function""#), "{b}");
    assert!(b.contains(r#""name":"search""#), "{b}");
    assert!(b.contains(r#""finish_reason":"tool_calls""#), "{b}");
    assert!(b.trim_end().ends_with("data: [DONE]"), "{b}");
}

#[tokio::test]
async fn unknown_model_404_and_tools_400() {
    let app = build_router(state(None));
    let r1 = app.clone().oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(r#"{"model":"nope","messages":[]}"#)).unwrap()).await.unwrap();
    assert_eq!(r1.status(), 404);
    // codex does not support tools — still 400 even after Phase 4b wiring.
    let r2 = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(r#"{"model":"codex-text","tools":[{"x":1}],"messages":[]}"#)).unwrap()).await.unwrap();
    assert_eq!(r2.status(), 400);
}

#[tokio::test]
async fn tools_to_codex_rejected_unsupported() {
    let app = build_router(state(None));
    let body = r#"{"model":"codex-text","tools":[{"type":"function","function":{"name":"f"}}],"messages":[{"role":"user","content":"hi"}]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    assert_eq!(r.status(), 400);
    assert!(body_string(r).await.contains("not supported for engine 'codex'"));
}

#[tokio::test]
async fn tools_to_claude_503_when_pool_full() {
    // Fill the suspended pool to capacity, then a claude tools-turn must 503 (early is_full guard),
    // WITHOUT spawning claude.
    let st = state(None); // SuspendedSessions::new(8) in state_with
    for i in 0..8 {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let cont: EventStream = Box::pin(futures::stream::iter(vec![AgentEvent::Done { finish_reason: "stop".into() }]));
        st.suspended.register(vec![(format!("c{i}"), tx)], cont).unwrap();
    }
    assert!(st.suspended.is_full());
    let app = build_router(st);
    let body = r#"{"model":"claude-text","tools":[{"type":"function","function":{"name":"f"}}],"messages":[{"role":"user","content":"hi"}]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    assert_eq!(r.status(), 503);
}

fn state_with_suspension(ids: &[&str]) -> (AppState, Vec<tokio::sync::oneshot::Receiver<String>>) {
    let st = state(None);
    let mut pairs = Vec::new();
    let mut rxs = Vec::new();
    for id in ids {
        let (tx, rx) = tokio::sync::oneshot::channel();
        pairs.push(((*id).to_string(), tx));
        rxs.push(rx);
    }
    let cont: EventStream = Box::pin(futures::stream::iter(vec![
        AgentEvent::AssistantText("resumed".into()),
        AgentEvent::Done { finish_reason: "stop".into() },
    ]));
    st.suspended.register(pairs, cont).unwrap();
    (st, rxs)
}

fn followup_body(results: &[(&str, &str)]) -> String {
    // The assistant message echoes the tool_calls; each result is a trailing role:"tool" message.
    let assistant_tool_calls: Vec<String> = results.iter().map(|(id, _)|
        format!(r#"{{"id":"{id}","type":"function","function":{{"name":"f","arguments":"{{}}"}}}}"#)).collect();
    let tool_result_msgs: Vec<String> = results.iter().map(|(id, res)|
        format!(r#"{{"role":"tool","tool_call_id":"{id}","content":"{res}"}}"#)).collect();
    format!(r#"{{"model":"claude-text","messages":[
        {{"role":"user","content":"do it"}},
        {{"role":"assistant","content":null,"tool_calls":[{}]}},
        {}
    ]}}"#, assistant_tool_calls.join(","), tool_result_msgs.join(","))
}

#[tokio::test]
async fn followup_with_stray_tools_field_still_routes_to_suspension() {
    // A follow-up that also carries a top-level `tools` field must STILL route by shape to the
    // parked suspension (routing precedes the tools gate), not get rejected by the tools gate.
    let (st, _rxs) = state_with_suspension(&["call_a"]);
    let app = build_router(st);
    let body = r#"{"model":"claude-text","tools":[{"type":"function","function":{"name":"f"}}],"messages":[
        {"role":"user","content":"do it"},
        {"role":"assistant","content":null,"tool_calls":[{"id":"call_a","type":"function","function":{"name":"f","arguments":"{}"}}]},
        {"role":"tool","tool_call_id":"call_a","content":"RES"}
    ]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    assert_eq!(r.status(), 200); // resumed, NOT a 400 from the tools gate
    assert!(body_string(r).await.contains("resumed"));
}

#[tokio::test]
async fn tool_result_followup_resumes_suspension() {
    let (st, _rxs) = state_with_suspension(&["call_a"]);
    let app = build_router(st);
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(followup_body(&[("call_a","RES")]))).unwrap()).await.unwrap();
    assert_eq!(r.status(), 200);
    assert!(body_string(r).await.contains("resumed"));
}

#[tokio::test]
async fn streaming_forces_tool_calls_finish_reason() {
    // Even if the engine's terminal Done says "stop", a turn that emitted a tool_call must finish
    // "tool_calls" on the SSE transport (matching the aggregate path).
    let runner = fake(vec![
        AgentEvent::ToolCall { id: "call_1".into(), name: "f".into(), args: "{}".into() },
        AgentEvent::Done { finish_reason: "stop".into() },
    ]);
    let app = build_router(state_with(None, runner));
    let body = r#"{"model":"claude-text","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(body)).unwrap()).await.unwrap();
    let b = body_string(r).await;
    assert!(b.contains(r#""finish_reason":"tool_calls""#), "{b}");
    assert!(!b.contains(r#""finish_reason":"stop""#), "{b}");
}

#[tokio::test]
async fn partial_tool_results_409() {
    let (st, _rxs) = state_with_suspension(&["call_a", "call_b"]);
    let app = build_router(st);
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(followup_body(&[("call_a","RES")]))).unwrap()).await.unwrap();
    assert_eq!(r.status(), 409);
    assert!(body_string(r).await.contains("call_b"));
}

#[tokio::test]
async fn unknown_tool_result_400() {
    let app = build_router(state(None));
    let r = app.oneshot(axum::http::Request::post("/v1/chat/completions").header("content-type","application/json").body(axum::body::Body::from(followup_body(&[("call_x","RES")]))).unwrap()).await.unwrap();
    assert_eq!(r.status(), 400);
}
