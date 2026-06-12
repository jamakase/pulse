use std::sync::Arc;

use pulse::{AppState, compactor, config::Config, query::QueryEngine, wal::Wal};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pulse=info,info".into()),
        )
        .init();

    let config = Arc::new(Config::from_env()?);
    std::fs::create_dir_all(config.wal_dir())?;
    std::fs::create_dir_all(config.events_dir())?;

    let compaction_lock = Arc::new(tokio::sync::RwLock::new(()));
    let wal = Arc::new(Wal::new(config.wal_dir())?);
    let engine = Arc::new(QueryEngine::new(
        config.events_dir(),
        config.wal_dir(),
        compaction_lock.clone(),
    ));

    {
        let (config, wal, lock) = (config.clone(), wal.clone(), compaction_lock.clone());
        tokio::spawn(async move {
            compactor::run_loop(config, wal, lock).await;
        });
    }

    let state = AppState {
        config: config.clone(),
        wal,
        engine,
    };
    let app = pulse::build_router(state);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], config.port));
    tracing::info!(%addr, "pulse listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
