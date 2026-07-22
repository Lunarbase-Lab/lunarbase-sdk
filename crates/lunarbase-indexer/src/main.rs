//! Runnable LunarBase realtime indexer and quote service.

mod api;
mod checkpoint;
mod config;
mod metrics;
mod runtime;

use checkpoint::RedisCheckpointStore;
use clap::Parser;
use config::{Cli, Config};
use lunarbase_client_core::indexer::errors::ClientRuntimeEvent;
use metrics::Metrics;
use std::{
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{broadcast, watch},
    task::JoinHandle,
    time::timeout,
};
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
    let mut api_task = tokio::spawn(api::serve(
        config.bind,
        client.clone(),
        metrics.clone(),
        wait_for_shutdown(shutdown_rx),
    ));
    let mut runtime_events = client.subscribe_runtime_events();
    let mut api_finished = false;
    let mut failure = tokio::select! {
        result = &mut signal => {
            result?;
            None
        }
        result = &mut api_task => {
            api_finished = true;
            Some(match result {
                Ok(Ok(())) => "HTTP API stopped unexpectedly".into(),
                Ok(Err(error)) => format!("HTTP API failed: {error}"),
                Err(error) => format!("HTTP API task failed: {error}"),
            })
        }
        detail = wait_for_runtime_failure(&mut runtime_events) => Some(detail),
    };
    tracing::info!(failure = failure.as_deref(), "graceful shutdown started");
    let _ = shutdown_tx.send(true);
    let deadline = Instant::now() + config.shutdown_timeout;

    if let Some(store) = &store
        && timeout(
            remaining(deadline),
            runtime::flush_checkpoint(&client, store, &metrics),
        )
        .await
        .is_err()
    {
        metrics.checkpoint_failure();
        tracing::warn!("final Redis checkpoint exceeded shutdown deadline");
    }
    if let Err(error) = client.shutdown_gracefully(remaining(deadline)).await {
        failure.get_or_insert_with(|| format!("client shutdown failed: {error}"));
    }
    if !join_unit_task(checkpoint_task, deadline).await {
        failure.get_or_insert_with(|| "checkpoint task exceeded shutdown deadline".into());
    }
    if !api_finished && !join_api_task(api_task, deadline).await {
        failure.get_or_insert_with(|| "HTTP API task exceeded shutdown deadline".into());
    }
    tracing::info!("graceful shutdown complete");
    if let Some(detail) = failure {
        Err(std::io::Error::other(detail).into())
    } else {
        Ok(())
    }
}

async fn wait_for_runtime_failure(
    receiver: &mut broadcast::Receiver<ClientRuntimeEvent>,
) -> String {
    loop {
        match receiver.recv().await {
            Ok(ClientRuntimeEvent::BackgroundTaskStopped { task }) => {
                return format!("required client task `{task}` stopped");
            }
            Ok(ClientRuntimeEvent::BackgroundTaskPanicked { task, detail }) => {
                return format!("required client task `{task}` panicked: {detail}");
            }
            Ok(event) => log_runtime_event(&event),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(
                    skipped,
                    "runtime event consumer lagged behind its bounded channel"
                );
            }
            Err(broadcast::error::RecvError::Closed) => {
                return "client runtime event channel closed".into();
            }
        }
    }
}

fn log_runtime_event(event: &ClientRuntimeEvent) {
    let code = event.code();
    let detail = event.detail();
    match event {
        ClientRuntimeEvent::RecoveryCompleted => {
            tracing::info!(event = code, detail, "client runtime event");
        }
        ClientRuntimeEvent::RecoveryStarted => {
            tracing::warn!(event = code, detail, "client runtime event");
        }
        _ => tracing::warn!(event = code, detail, "client runtime event"),
    }
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

async fn join_unit_task(mut task: JoinHandle<()>, deadline: Instant) -> bool {
    if timeout(remaining(deadline), &mut task).await.is_ok() {
        return true;
    }
    task.abort();
    let _ = task.await;
    false
}

async fn join_api_task(
    mut task: JoinHandle<Result<(), std::io::Error>>,
    deadline: Instant,
) -> bool {
    if timeout(remaining(deadline), &mut task).await.is_ok() {
        return true;
    }
    task.abort();
    let _ = task.await;
    false
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
