//! HTTP layer: shared state, router, health/models endpoints, bearer-auth middleware, and the
//! non-streaming chat-completions handler.
use crate::openai::{ApiError, ChatCompletionRequest};
use crate::orchestrator::{response_from_events, turn_from_request, RunError, TurnRunner};
use crate::engine::EngineError;
use crate::registry::Registry;
use axum::{
    extract::State,
    http::{header::AUTHORIZATION, Request, StatusCode},
    middleware::{from_fn_with_state, Next},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    pub bearer_token: Option<String>,
    pub runner: Arc<dyn TurnRunner>,
}

pub fn build_router(state: AppState) -> Router {
    // `route_layer` applies auth ONLY to routes declared before it, so `/v1/*` require a token
    // while `/health` (declared after) stays open.
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

async fn auth_middleware(
    State(state): State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let Some(expected) = state.bearer_token.as_deref().filter(|t| !t.is_empty()) else {
        return next.run(req).await; // no token configured -> auth disabled (loopback/trusted default)
    };
    let presented = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if presented == Some(expected) {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(ApiError::new("missing or invalid bearer token", "invalid_request_error")),
        )
            .into_response()
    }
}

/// POST /v1/chat/completions (non-streaming only in Phase 1).
async fn chat_completions(
    State(state): State<AppState>,
    Json(req): Json<ChatCompletionRequest>,
) -> Response {
    if req.is_streaming() {
        return err(StatusCode::BAD_REQUEST, "streaming is not implemented yet (Phase 2)", "invalid_request_error");
    }
    if req.has_tools() {
        return err(StatusCode::BAD_REQUEST, "the `tools` field is not implemented yet (Phase 4)", "invalid_request_error");
    }
    let Some(entry) = state.registry.resolve(&req.model) else {
        return err(StatusCode::NOT_FOUND, &format!("unknown model '{}'", req.model), "model_not_found");
    };
    let entry = entry.clone();
    let turn = turn_from_request(&req, &entry);

    match state.runner.run(&entry, turn).await {
        Ok(events) => match response_from_events(events, &req.model) {
            Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
            Err(EngineError::Reported(m)) => err(StatusCode::INTERNAL_SERVER_ERROR, &m, "engine_error"),
            Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string(), "engine_error"),
        },
        Err(RunError::Timeout) => err(StatusCode::GATEWAY_TIMEOUT, "claude timed out", "timeout"),
        Err(RunError::Spawn(m)) => err(StatusCode::INTERNAL_SERVER_ERROR, &format!("spawn failed: {m}"), "engine_error"),
        Err(RunError::Engine(m)) => err(StatusCode::INTERNAL_SERVER_ERROR, &m, "engine_error"),
    }
}

fn err(status: StatusCode, message: &str, kind: &str) -> Response {
    (status, Json(ApiError::new(message, kind))).into_response()
}
