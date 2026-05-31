//! llm-bridge entrypoint: load config, validate, build the shared runner + router, serve.
use llm_bridge::config::load_config;
use llm_bridge::http::{build_router, AppState};
use llm_bridge::orchestrator::EngineProcessRunner;
use llm_bridge::process::ProcessSupervisor;
use llm_bridge::registry::Registry;
use llm_bridge::validate::validate_config;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let config_path = std::env::args().nth(1).unwrap_or_else(|| "config.yaml".to_string());
    let cfg = load_config(std::path::Path::new(&config_path))?;

    if let Err(problems) = validate_config(&cfg) {
        for p in &problems {
            tracing::error!("config: {p}");
        }
        anyhow::bail!("{} config validation error(s); refusing to start", problems.len());
    }

    // One shared supervisor -> a GLOBAL concurrency cap across all requests.
    let supervisor = ProcessSupervisor::new(cfg.defaults.max_concurrency);
    let runner = Arc::new(EngineProcessRunner {
        supervisor,
        credentials: cfg.credentials.clone(),
        env_passthrough: cfg.defaults.env_passthrough.clone(),
        timeout: Duration::from_secs(cfg.defaults.timeout_s),
    });

    let bind = cfg.server.bind.clone();
    let state = AppState {
        registry: Arc::new(Registry::new(cfg.models.clone())),
        bearer_token: cfg.server.bearer_token.clone(),
        runner,
        sessions: Arc::new(llm_bridge::session::SessionStore::new()),
        defaults: cfg.defaults.clone(),
        progress_channel: cfg.server.progress_channel,
        credentials: cfg.credentials.clone(),
    };

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!("llm-bridge listening on {bind} ({} models)", cfg.models.len());
    axum::serve(listener, app).await?;
    Ok(())
}
