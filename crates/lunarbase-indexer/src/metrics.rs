//! Lock-free service counters and Prometheus text exposition.

use crate::runtime::{RuntimeHandle, RuntimeRole, ServiceRuntimeEvent};
use lunarbase_client_core::{ClientRuntimeStatsSnapshot, Commitment};
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

const QUOTE_BUCKET_MICROSECONDS: [u64; 13] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000,
];

#[derive(Default)]
struct MetricsInner {
    quote_requests: AtomicU64,
    quote_errors: AtomicU64,
    quote_latency_microseconds: AtomicU64,
    quote_buckets: [AtomicU64; QUOTE_BUCKET_MICROSECONDS.len()],
    lease_acquired: AtomicU64,
    lease_lost: AtomicU64,
    lease_failures: AtomicU64,
    runtime_connect_failures: AtomicU64,
    alert_failures: AtomicU64,
    shutdown_failures: AtomicU64,
    current_lag_blocks: AtomicU64,
}

/// Shared metrics recorder used by API, alerts, and lifecycle supervision.
#[derive(Clone, Default)]
pub struct ServiceMetrics {
    inner: Arc<MetricsInner>,
}

impl ServiceMetrics {
    /// Records one quote request and its complete handler latency.
    pub fn observe_quote(&self, elapsed: Duration, success: bool) {
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.inner.quote_requests.fetch_add(1, Ordering::Relaxed);
        self.inner
            .quote_latency_microseconds
            .fetch_add(micros, Ordering::Relaxed);
        if !success {
            self.inner.quote_errors.fetch_add(1, Ordering::Relaxed);
        }
        for (index, upper_bound) in QUOTE_BUCKET_MICROSECONDS.iter().enumerate() {
            if micros <= *upper_bound {
                self.inner.quote_buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Updates the observed distance between a quote execution block and the
    /// reducer cursor.
    pub fn observe_lag(&self, execution_block: u64, indexed_block: u64) {
        self.inner.current_lag_blocks.store(
            execution_block.saturating_sub(indexed_block),
            Ordering::Relaxed,
        );
    }

    /// Records a webhook delivery or timeout failure.
    pub fn record_alert_failure(&self) {
        self.inner.alert_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a process shutdown failure or forced task abort.
    pub fn record_shutdown_failure(&self) {
        self.inner.shutdown_failures.fetch_add(1, Ordering::Relaxed);
    }

    fn record_event(&self, event: &ServiceRuntimeEvent) {
        match event {
            ServiceRuntimeEvent::LeaseAcquired => {
                self.inner.lease_acquired.fetch_add(1, Ordering::Relaxed);
            }
            ServiceRuntimeEvent::LeaseLost => {
                self.inner.lease_lost.fetch_add(1, Ordering::Relaxed);
            }
            ServiceRuntimeEvent::LeaseAcquireFailed { .. }
            | ServiceRuntimeEvent::LeaseRenewFailed { .. }
            | ServiceRuntimeEvent::LeaseReleaseFailed { .. } => {
                self.inner.lease_failures.fetch_add(1, Ordering::Relaxed);
            }
            ServiceRuntimeEvent::RuntimeConnectFailed { .. } => {
                self.inner
                    .runtime_connect_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
            ServiceRuntimeEvent::Client(_) | ServiceRuntimeEvent::RuntimeEventsLagged { .. } => {}
        }
    }

    /// Renders a self-contained Prometheus text snapshot.
    pub async fn render(&self, runtime: &RuntimeHandle) -> String {
        let status = runtime.status().await;
        let client = runtime.client().await;
        let health = match &client {
            Some(client) if client.is_ready() => Some(client.health().await),
            None => None,
            Some(_) => None,
        };
        let stats = runtime.runtime_stats().await;
        let ready = health.as_ref().is_some_and(|health| health.ready);
        let block = health
            .as_ref()
            .and_then(|health| health.cursor.as_ref())
            .map_or(0, |cursor| cursor.block_number);
        let commitment = health
            .as_ref()
            .map_or(Commitment::Realtime, |health| health.commitment);
        let mut output = String::with_capacity(5_000);

        metric_help(
            &mut output,
            "lunarbase_indexer_ready",
            "Quote readiness gauge",
        );
        metric_type(&mut output, "lunarbase_indexer_ready", "gauge");
        metric(&mut output, "lunarbase_indexer_ready", u64::from(ready));
        metric_help(
            &mut output,
            "lunarbase_indexer_current_block",
            "Latest indexed block",
        );
        metric_type(&mut output, "lunarbase_indexer_current_block", "gauge");
        metric(&mut output, "lunarbase_indexer_current_block", block);
        metric_help(
            &mut output,
            "lunarbase_indexer_lag_blocks",
            "Last observed execution-to-indexed block lag",
        );
        metric_type(&mut output, "lunarbase_indexer_lag_blocks", "gauge");
        metric(
            &mut output,
            "lunarbase_indexer_lag_blocks",
            self.inner.current_lag_blocks.load(Ordering::Relaxed),
        );

        for role in [
            RuntimeRole::Starting,
            RuntimeRole::Standby,
            RuntimeRole::Active,
            RuntimeRole::LeaseLost,
            RuntimeRole::Stopping,
        ] {
            let _ = writeln!(
                output,
                "lunarbase_indexer_role{{role=\"{}\"}} {}",
                role.as_str(),
                u64::from(status.role == role)
            );
        }
        for level in [
            Commitment::Realtime,
            Commitment::Canonical,
            Commitment::Finalized,
        ] {
            let _ = writeln!(
                output,
                "lunarbase_indexer_commitment{{level=\"{}\"}} {}",
                commitment_name(level),
                u64::from(health.is_some() && commitment == level)
            );
        }

        render_runtime_stats(&mut output, stats);
        self.render_quote_metrics(&mut output);
        counter(
            &mut output,
            "lunarbase_writer_lease_acquired_total",
            self.inner.lease_acquired.load(Ordering::Relaxed),
        );
        counter(
            &mut output,
            "lunarbase_writer_lease_lost_total",
            self.inner.lease_lost.load(Ordering::Relaxed),
        );
        counter(
            &mut output,
            "lunarbase_writer_lease_failures_total",
            self.inner.lease_failures.load(Ordering::Relaxed),
        );
        counter(
            &mut output,
            "lunarbase_runtime_connect_failures_total",
            self.inner.runtime_connect_failures.load(Ordering::Relaxed),
        );
        counter(
            &mut output,
            "lunarbase_alert_failures_total",
            self.inner.alert_failures.load(Ordering::Relaxed),
        );
        counter(
            &mut output,
            "lunarbase_shutdown_failures_total",
            self.inner.shutdown_failures.load(Ordering::Relaxed),
        );
        output
    }

    fn render_quote_metrics(&self, output: &mut String) {
        let count = self.inner.quote_requests.load(Ordering::Relaxed);
        counter(output, "lunarbase_quote_requests_total", count);
        counter(
            output,
            "lunarbase_quote_errors_total",
            self.inner.quote_errors.load(Ordering::Relaxed),
        );
        for (index, upper_bound) in QUOTE_BUCKET_MICROSECONDS.iter().enumerate() {
            let _ = writeln!(
                output,
                "lunarbase_quote_latency_seconds_bucket{{le=\"{}\"}} {}",
                (*upper_bound as f64) / 1_000_000.0,
                self.inner.quote_buckets[index].load(Ordering::Relaxed)
            );
        }
        let _ = writeln!(
            output,
            "lunarbase_quote_latency_seconds_bucket{{le=\"+Inf\"}} {count}"
        );
        let _ = writeln!(
            output,
            "lunarbase_quote_latency_seconds_sum {}",
            (self
                .inner
                .quote_latency_microseconds
                .load(Ordering::Relaxed) as f64)
                / 1_000_000.0
        );
        let _ = writeln!(output, "lunarbase_quote_latency_seconds_count {count}");
    }
}

/// Consumes runtime events until shutdown so lifecycle counters are retained
/// independently from the alert consumer.
pub fn spawn_event_collector(
    metrics: ServiceMetrics,
    mut events: broadcast::Receiver<ServiceRuntimeEvent>,
    mut stop: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                () = stop_requested(&mut stop) => break,
                event = events.recv() => match event {
                    Ok(event) => metrics.record_event(&event),
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    })
}

fn render_runtime_stats(output: &mut String, stats: ClientRuntimeStatsSnapshot) {
    metric(output, "lunarbase_queue_depth", stats.queue_depth);
    metric(output, "lunarbase_queue_capacity", stats.queue_capacity);
    let utilization = if stats.queue_capacity == 0 {
        0.0
    } else {
        (stats.queue_depth as f64) / (stats.queue_capacity as f64)
    };
    metric(output, "lunarbase_queue_utilization_ratio", utilization);
    counter(
        output,
        "lunarbase_source_reconnects_total",
        stats.source_reconnects,
    );
    counter(output, "lunarbase_source_gaps_total", stats.gaps);
    counter(output, "lunarbase_recoveries_total", stats.recoveries);
    counter(
        output,
        "lunarbase_recovery_failures_total",
        stats.recovery_failures,
    );
    counter(
        output,
        "lunarbase_redis_checkpoint_commits_total",
        stats.checkpoint_commits,
    );
    counter(
        output,
        "lunarbase_redis_failures_total",
        stats.checkpoint_failures,
    );
    let checkpoint_operations = stats
        .checkpoint_commits
        .saturating_add(stats.checkpoint_failures);
    let average = if checkpoint_operations == 0 {
        0.0
    } else {
        (stats.checkpoint_latency_nanoseconds as f64)
            / (checkpoint_operations as f64)
            / 1_000_000_000.0
    };
    let _ = writeln!(output, "lunarbase_redis_latency_seconds {average}");
}

fn metric_help(output: &mut String, name: &str, help: &str) {
    let _ = writeln!(output, "# HELP {name} {help}");
}

fn metric_type(output: &mut String, name: &str, kind: &str) {
    let _ = writeln!(output, "# TYPE {name} {kind}");
}

fn metric(output: &mut String, name: &str, value: impl std::fmt::Display) {
    let _ = writeln!(output, "{name} {value}");
}

fn counter(output: &mut String, name: &str, value: u64) {
    metric_type(output, name, "counter");
    metric(output, name, value);
}

const fn commitment_name(commitment: Commitment) -> &'static str {
    match commitment {
        Commitment::Realtime => "realtime",
        Commitment::Canonical => "canonical",
        Commitment::Finalized => "finalized",
    }
}

async fn stop_requested(stop: &mut watch::Receiver<bool>) {
    if *stop.borrow() {
        return;
    }
    loop {
        if stop.changed().await.is_err() || *stop.borrow() {
            return;
        }
    }
}
