//! Runnable LunarBase realtime indexer and quote service.

mod api;
mod checkpoint;
mod config;
mod metrics;
mod runtime;

use checkpoint::RedisCheckpointStore;
use clap::Parser;
use config::{Cli, Config};
use metrics::Metrics;
use std::{error::Error, sync::Arc};
use tokio::{sync::watch, time::timeout};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    init_tracing();
    if let Err(error) = run().await {
        tracing::error!(error = %error, "lunarbase-indexer stopped with an error");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;
    let metrics = Arc::new(Metrics::default());
    let signal = wait_for_signal();
    tokio::pin!(signal);
    let store = config
        .redis_url
        .as_ref()
        .map(|url| RedisCheckpointStore::new(url.clone(), &config.client.deployment));
    let checkpoint = tokio::select! {
        result = runtime::load_checkpoint(store.as_ref(), &metrics) => result,
        result = &mut signal => {
            result?;
            tracing::info!("shutdown received before checkpoint load completed");
            return Ok(());
        }
    };
    let client = tokio::select! {
        result = runtime::connect_client(&config, checkpoint) => Arc::new(result?),
        result = &mut signal => {
            result?;
            tracing::info!("shutdown received during client bootstrap");
            return Ok(());
        }
    };

    tracing::info!(
        network = ?config.client.deployment.network,
        chain_id = config.client.deployment.chain_id,
        core = %config.client.deployment.core,
        router = %config.client.deployment.router,
        bind = %config.bind,
        redis = store.is_some(),
        "lunarbase-indexer is ready"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let checkpoint_task = runtime::spawn_checkpoint_loop(
        client.clone(),
        store.clone(),
        config.checkpoint_interval,
        metrics.clone(),
        shutdown_rx.clone(),
    );
    let api_task = tokio::spawn(api::serve(
        config.bind,
        client.clone(),
        metrics.clone(),
        wait_for_shutdown(shutdown_rx),
    ));

    signal.await?;
    tracing::info!("graceful shutdown started");
    let _ = shutdown_tx.send(true);

    if let Some(store) = &store {
        runtime::flush_checkpoint(&client, store, &metrics).await;
    }
    client.shutdown_gracefully(config.shutdown_timeout).await?;

    let joined = timeout(config.shutdown_timeout, async {
        checkpoint_task.await?;
        api_task.await??;
        Ok::<(), Box<dyn Error>>(())
    })
    .await;
    match joined {
        Ok(result) => result?,
        Err(_) => {
            return Err("service tasks exceeded graceful shutdown timeout".into());
        }
    }
    tracing::info!("graceful shutdown complete");
    Ok(())
}

async fn wait_for_shutdown(mut receiver: watch::Receiver<bool>) {
    while !*receiver.borrow() {
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(unix)]
async fn wait_for_signal() -> Result<(), std::io::Error> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("lunarbase_indexer=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}
