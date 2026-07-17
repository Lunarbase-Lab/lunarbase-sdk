//! LunarBase indexing and quote service entry point.

#[cfg(not(any(feature = "base", feature = "monad", feature = "arbitrum")))]
compile_error!("lunarbase-indexer requires at least one network feature");

mod alerts;
mod api;
mod config;
mod metrics;
mod runtime;

use alerts::{AlertSeverity, AlertSink};
use clap::Parser;
use config::IndexerConfig;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "lunarbase-indexer")]
#[command(about = "Run the LunarBase indexer and quote HTTP API")]
struct Arguments {
    /// TOML configuration file.
    #[arg(short, long, default_value = "config/base.toml")]
    config: PathBuf,
}

#[derive(Debug, Error)]
enum MainError {
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    #[error(transparent)]
    Runtime(#[from] runtime::RuntimeError),
    #[error("HTTP server failed: {0}")]
    Http(#[from] std::io::Error),
    #[error("service lifecycle failed: {0}")]
    Lifecycle(String),
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("lunarbase_indexer=info")),
        )
        .init();
    let (panic_sender, panic_receiver) = mpsc::unbounded_channel();
    alerts::install_panic_hook(panic_sender);

    match run(panic_receiver).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(failure) => {
            error!(
                alert = true,
                severity = "critical",
                code = "process_failed",
                error = %failure,
                "lunarbase-indexer exited with an error"
            );
            ExitCode::FAILURE
        }
    }
}

async fn run(
    panic_receiver: mpsc::UnboundedReceiver<alerts::ProcessPanic>,
) -> Result<(), MainError> {
    let arguments = Arguments::parse();
    let config = IndexerConfig::load(&arguments.config)?.validate()?;
    let metrics = metrics::ServiceMetrics::default();
    let alert_sink = AlertSink::new(&config, metrics.clone());
    info!(
        network = ?config.deployment.network,
        chain_id = config.deployment.chain_id,
        core = %config.deployment.core,
        bind = %config.bind,
        "starting LunarBase indexer"
    );

    let signal = SignalListener::spawn();
    let store = match runtime::build_store(&config) {
        Ok(store) => store,
        Err(failure) => {
            alert_sink
                .emit(
                    AlertSeverity::Critical,
                    "runtime_startup_failed",
                    &failure.to_string(),
                )
                .await;
            signal.stop().await;
            return Err(failure.into());
        }
    };
    let runtime = runtime::RuntimeHandle::new();
    let runtime_events = runtime.subscribe_events();
    let metrics_events = runtime.subscribe_events();
    let (alert_stop, alert_shutdown) = watch::channel(false);
    let mut alert_task = alerts::spawn_supervisor(
        alert_sink.clone(),
        runtime.clone(),
        runtime_events,
        panic_receiver,
        alert_shutdown,
    );
    let mut metrics_task =
        metrics::spawn_event_collector(metrics.clone(), metrics_events, alert_stop.subscribe());

    let runtime_config = config.clone();
    let runtime_handle = runtime.clone();
    let runtime_shutdown = signal.subscribe();
    let mut runtime_task = tokio::spawn(async move {
        runtime::supervise(&runtime_config, store, runtime_handle, runtime_shutdown).await
    });
    let api_runtime = runtime.clone();
    let api_metrics = metrics.clone();
    let api_shutdown = signal.subscribe();
    let bind = config.bind;
    let mut api_task = tokio::spawn(async move {
        api::serve(
            bind,
            api_runtime,
            api_metrics,
            shutdown_requested(api_shutdown),
        )
        .await
    });

    enum FirstExit {
        Signal,
        Runtime(Result<Result<(), runtime::RuntimeError>, tokio::task::JoinError>),
        Api(Result<Result<(), std::io::Error>, tokio::task::JoinError>),
    }
    let first_exit = tokio::select! {
        biased;
        () = shutdown_requested(signal.subscribe()) => FirstExit::Signal,
        result = &mut runtime_task => FirstExit::Runtime(result),
        result = &mut api_task => FirstExit::Api(result),
    };
    let shutdown_started = Instant::now();
    signal.request_shutdown();
    let mut service_result = match &first_exit {
        FirstExit::Signal => Ok(()),
        FirstExit::Runtime(Ok(Ok(()))) => Ok(()),
        FirstExit::Runtime(Ok(Err(failure))) => Err(MainError::Runtime(
            runtime::RuntimeError::Redis(format!("runtime supervisor failed: {failure}")),
        )),
        FirstExit::Runtime(Err(failure)) => Err(MainError::Lifecycle(format!(
            "runtime supervisor task failed: {failure}"
        ))),
        FirstExit::Api(Ok(Ok(()))) => Ok(()),
        FirstExit::Api(Ok(Err(failure))) => Err(MainError::Http(std::io::Error::new(
            failure.kind(),
            failure.to_string(),
        ))),
        FirstExit::Api(Err(failure)) => Err(MainError::Lifecycle(format!(
            "HTTP server task failed: {failure}"
        ))),
    };
    if let Err(failure) = &service_result {
        alert_sink
            .emit(
                AlertSeverity::Critical,
                "service_task_failed",
                &failure.to_string(),
            )
            .await;
    }

    let remaining_tasks = async {
        match first_exit {
            FirstExit::Signal => {
                merge_runtime_result(&mut service_result, (&mut runtime_task).await);
                merge_api_result(&mut service_result, (&mut api_task).await);
            }
            FirstExit::Runtime(_) => {
                merge_api_result(&mut service_result, (&mut api_task).await);
            }
            FirstExit::Api(_) => {
                merge_runtime_result(&mut service_result, (&mut runtime_task).await);
            }
        }
    };
    if timeout(
        remaining_timeout(shutdown_started, config.shutdown_timeout),
        remaining_tasks,
    )
    .await
    .is_err()
    {
        runtime_task.abort();
        api_task.abort();
        let _ = runtime_task.await;
        let _ = api_task.await;
        service_result = Err(MainError::Lifecycle(
            "service tasks exceeded the shutdown deadline and were aborted".into(),
        ));
        metrics.record_shutdown_failure();
    }

    let _ = alert_stop.send(true);
    match timeout(
        remaining_timeout(shutdown_started, config.shutdown_timeout),
        &mut metrics_task,
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(join_error)) => {
            metrics.record_shutdown_failure();
            service_result = Err(MainError::Lifecycle(format!(
                "metrics collector task failed: {join_error}"
            )));
        }
        Err(_) => {
            metrics_task.abort();
            let _ = metrics_task.await;
            metrics.record_shutdown_failure();
            service_result = Err(MainError::Lifecycle(
                "metrics collector exceeded the shutdown deadline".into(),
            ));
        }
    }
    match timeout(
        remaining_timeout(shutdown_started, config.shutdown_timeout),
        &mut alert_task,
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(join_error)) => {
            metrics.record_shutdown_failure();
            service_result = Err(MainError::Lifecycle(format!(
                "alert supervisor task failed: {join_error}"
            )));
        }
        Err(_) => {
            alert_task.abort();
            let _ = alert_task.await;
            service_result = Err(MainError::Lifecycle(
                "alert supervisor exceeded the shutdown deadline".into(),
            ));
            metrics.record_shutdown_failure();
        }
    }

    signal.stop().await;
    info!("LunarBase indexer stopped");
    service_result
}

fn merge_runtime_result(
    service_result: &mut Result<(), MainError>,
    task_result: Result<Result<(), runtime::RuntimeError>, tokio::task::JoinError>,
) {
    if service_result.is_err() {
        return;
    }
    *service_result = match task_result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(failure)) => Err(MainError::Runtime(failure)),
        Err(failure) => Err(MainError::Lifecycle(format!(
            "runtime supervisor task failed: {failure}"
        ))),
    };
}

fn merge_api_result(
    service_result: &mut Result<(), MainError>,
    task_result: Result<Result<(), std::io::Error>, tokio::task::JoinError>,
) {
    if service_result.is_err() {
        return;
    }
    *service_result = match task_result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(failure)) => Err(MainError::Http(failure)),
        Err(failure) => Err(MainError::Lifecycle(format!(
            "HTTP server task failed: {failure}"
        ))),
    };
}

struct SignalListener {
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl SignalListener {
    fn spawn() -> Self {
        let (shutdown, _) = watch::channel(false);
        let signal_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            shutdown_signal().await;
            let _ = signal_shutdown.send(true);
        });
        Self {
            shutdown,
            task: Some(task),
        }
    }

    fn subscribe(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    fn request_shutdown(&self) {
        let _ = self.shutdown.send(true);
    }

    async fn stop(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for SignalListener {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn shutdown_requested(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    loop {
        if shutdown.changed().await.is_err() || *shutdown.borrow() {
            return;
        }
    }
}

fn remaining_timeout(started_at: Instant, deadline: Duration) -> Duration {
    deadline.saturating_sub(started_at.elapsed())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to install Ctrl-C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => warn!(%error, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
