//! llm-bridge entrypoint: load config, validate, build the shared runner + router, serve.
use llm_bridge::config::{load_config, EngineKind, SandboxBackend};
use llm_bridge::http::{build_router, AppState};
use llm_bridge::orchestrator::EngineProcessRunner;
use llm_bridge::process::ProcessSupervisor;
use llm_bridge::registry::Registry;
use llm_bridge::validate::validate_config;
use std::path::PathBuf;
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

    // For an external sandbox, prove the posture before serving: each agentic model's effective
    // sandbox must DENY both a write outside the workspace and a read of a planted secret (spec §4.8).
    // Probes are per-model (a few `bwrap` spawns each); `run_probes` is blocking std::process, which
    // is fine here because serving hasn't started yet. De-dup per (engine, workspace) only if a large
    // model list ever makes startup latency a concern.
    if cfg.defaults.sandbox_backend != SandboxBackend::None {
        for m in cfg.models.iter().filter(|m| m.mode == llm_bridge::config::Mode::Agentic) {
            let home = match m.engine {
                EngineKind::Claude => cfg.credentials.claude_config_dir.clone(),
                EngineKind::Codex => cfg.credentials.codex_home.clone(),
                EngineKind::Agy => cfg.credentials.agy_config_dir.clone(),
            };
            let ro: Vec<PathBuf> = home.into_iter().collect();
            let outcome = llm_bridge::sandbox::run_probes(m.workspace.as_deref(), &ro)
                .map_err(|e| anyhow::anyhow!("model '{}': sandbox canary probe failed to run ({e}); is bwrap installed?", m.id))?;
            if let Err(why) = llm_bridge::sandbox::canary_decision(&outcome) {
                anyhow::bail!("model '{}': sandbox_backend posture rejected — {}", m.id, why);
            }
            tracing::info!("model '{}': sandbox canary probes passed (write + read denied)", m.id);
        }
    }

    // One shared supervisor -> a GLOBAL concurrency cap across all requests (ordinary + tools turns).
    let supervisor = ProcessSupervisor::new(cfg.defaults.max_concurrency);
    let runner = Arc::new(EngineProcessRunner {
        supervisor: supervisor.clone(),
        credentials: cfg.credentials.clone(),
        env_passthrough: cfg.defaults.env_passthrough.clone(),
        timeout: Duration::from_secs(cfg.defaults.timeout_s),
        sandbox_backend: cfg.defaults.sandbox_backend,
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
        suspended: Arc::new(llm_bridge::suspend::SuspendedSessions::new(cfg.defaults.max_suspended_sessions)),
        tool_result_timeout: Duration::from_secs(cfg.defaults.tool_result_timeout_s),
        supervisor,
    };

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!("llm-bridge listening on {bind} ({} models)", cfg.models.len());
    axum::serve(listener, app).await?;
    Ok(())
}
