//! Standalone durable Core event ingestion service.

mod config;
mod event;
mod http;
mod metrics;
mod pump;
mod redis_store;
mod runtime;
mod source;

use clap::Parser;
use config::Cli;
use metrics::Metrics;
use redis_store::{RedisEventStore, RedisWriter};
use std::{error::Error, sync::Arc, time::Instant};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Duration, timeout},
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    init_tracing();
    if let Err(error) = run().await {
        tracing::error!(error = %error, "lunarbase-event-worker stopped with an error");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let config = Arc::new(Cli::parse().validate()?);
    let metrics = Arc::new(Metrics::new(
        config.source_queue_bound,
        config.redis_queue_bound,
    ));
    let (store, writer) = RedisEventStore::start(
        config.redis_url.clone(),
        &config.redis_namespace,
        config.consumer_group.clone(),
        config.chain_id,
        config.core,
        config.redis_timeout,
        config.redis_queue_bound,
        metrics.clone(),
    )?;
    tracing::info!(
        network = ?config.network,
        chain_id = config.chain_id,
        core = %config.core,
        minimum_commitment = ?config.minimum_commitment,
        redis_stream = store.keys().stream,
        redis_cursor = store.keys().cursor,
        consumer_group = config.consumer_group,
        bind = %config.bind,
        "durable event worker starting"
    );

    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let mut worker_task = tokio::spawn(source::run_selected(
        config.clone(),
        store.clone(),
        metrics.clone(),
        shutdown_receiver.clone(),
    ));
    let mut api_task = tokio::spawn(http::serve(
        config.bind,
        metrics.clone(),
        wait_for_shutdown(shutdown_receiver),
    ));
    let mut signal = Box::pin(wait_for_signal());
    let mut worker_finished = false;
    let mut api_finished = false;
    let mut failure = tokio::select! {
        result = &mut signal => {
            result?;
            None
        }
        result = &mut worker_task => {
            worker_finished = true;
            Some(match result {
                Ok(Ok(())) => "event worker stopped unexpectedly".into(),
                Ok(Err(error)) => format!("event worker failed: {error}"),
                Err(error) => format!("event worker task failed: {error}"),
            })
        }
        result = &mut api_task => {
            api_finished = true;
            Some(match result {
                Ok(Ok(())) => "event worker API stopped unexpectedly".into(),
                Ok(Err(error)) => format!("event worker API failed: {error}"),
                Err(error) => format!("event worker API task failed: {error}"),
            })
        }
    };

    metrics.set_ready(false);
    let _ = shutdown_sender.send(true);
    let deadline = Instant::now() + config.shutdown_timeout;
    if !worker_finished && let Err(detail) = join_worker(worker_task, deadline).await {
        failure.get_or_insert(detail);
    }
    if !api_finished && let Err(detail) = join_api(api_task, deadline).await {
        failure.get_or_insert(detail);
    }
    drop(store);
    if let Err(detail) = join_redis(writer, deadline).await {
        failure.get_or_insert(detail);
    }
    if let Some(detail) = failure {
        Err(std::io::Error::other(detail).into())
    } else {
        tracing::info!("durable event worker stopped cleanly");
        Ok(())
    }
}

async fn join_worker(
    mut task: JoinHandle<Result<(), runtime::RuntimeError>>,
    deadline: Instant,
) -> Result<(), String> {
    match timeout(remaining(deadline), &mut task).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(format!("event worker shutdown failed: {error}")),
        Ok(Err(error)) => Err(format!("event worker task failed during shutdown: {error}")),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("event worker exceeded shutdown deadline".into())
        }
    }
}

async fn join_api(
    mut task: JoinHandle<Result<(), std::io::Error>>,
    deadline: Instant,
) -> Result<(), String> {
    match timeout(remaining(deadline), &mut task).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(format!("event worker API shutdown failed: {error}")),
        Ok(Err(error)) => Err(format!("event worker API task failed: {error}")),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("event worker API exceeded shutdown deadline".into())
        }
    }
}

async fn join_redis(writer: RedisWriter, deadline: Instant) -> Result<(), String> {
    match timeout(
        remaining(deadline),
        tokio::task::spawn_blocking(move || writer.join()),
    )
    .await
    {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(format!("Redis writer shutdown failed: {error}")),
        Ok(Err(error)) => Err(format!("Redis writer join failed: {error}")),
        Err(_) => Err("Redis writer exceeded shutdown deadline".into()),
    }
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

async fn wait_for_shutdown(mut receiver: watch::Receiver<bool>) {
    while !*receiver.borrow() && receiver.changed().await.is_ok() {}
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
        .unwrap_or_else(|_| EnvFilter::new("lunarbase_event_worker=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}
