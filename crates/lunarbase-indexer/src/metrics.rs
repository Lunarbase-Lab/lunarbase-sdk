//! Small Prometheus exposition surface for runtime-critical signals.

use lunarbase_client::indexer::client::ConnectedQuoteClient;
use lunarbase_client::model::Commitment;
use std::{
    fmt::Write,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Default)]
/// Lock-free process metrics recorder.
pub struct Metrics {
    /// Total HTTP quote requests, including batch requests as one request.
    quote_count: AtomicU64,
    /// Quote requests that returned a transport-level or runtime error.
    quote_errors: AtomicU64,
    /// Accumulated quote handler latency used to expose a low-cost average.
    quote_latency_nanos: AtomicU64,
    /// Quote requests submitted through the batch endpoint.
    quote_batches: AtomicU64,
    /// Best-effort Redis checkpoint writes completed successfully.
    checkpoint_success: AtomicU64,
    /// Redis checkpoint load or write attempts that failed.
    checkpoint_failure: AtomicU64,
}

impl Metrics {
    /// Records one quote or batch request.
    pub fn record_quote(&self, latency: Duration, error: bool, batch: bool) {
        self.quote_count.fetch_add(1, Ordering::Relaxed);
        self.quote_latency_nanos
            .fetch_add(saturating_nanos(latency), Ordering::Relaxed);
        if error {
            self.quote_errors.fetch_add(1, Ordering::Relaxed);
        }
        if batch {
            self.quote_batches.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a successful best-effort checkpoint write.
    pub fn checkpoint_success(&self) {
        self.checkpoint_success.fetch_add(1, Ordering::Relaxed);
    }

    /// Records Redis load/write failure without changing readiness.
    pub fn checkpoint_failure(&self) {
        self.checkpoint_failure.fetch_add(1, Ordering::Relaxed);
    }

    /// Renders current gauges and counters in Prometheus text format.
    pub fn render(&self, client: &ConnectedQuoteClient) -> String {
        let health = client.health().ok();
        let stats = client.runtime_stats();
        let ready = health.as_ref().is_some_and(|health| health.ready) as u8;
        let head = health
            .as_ref()
            .and_then(|health| health.cursor.as_ref())
            .map_or(0, |cursor| cursor.block_number);
        let execution = health
            .as_ref()
            .and_then(|health| health.execution_block_number)
            .unwrap_or(0);
        let execution_context_delta = head.saturating_sub(execution);
        let source_update_age = source_update_age(stats.last_source_update_unix_millis);
        let commitment = health.as_ref().map_or(0, |health| match health.commitment {
            Commitment::Realtime => 0,
            Commitment::Canonical => 1,
            Commitment::Finalized => 2,
        });
        let quote_count = self.quote_count.load(Ordering::Relaxed);
        let average_latency = if quote_count == 0 {
            0.0
        } else {
            self.quote_latency_nanos.load(Ordering::Relaxed) as f64
                / quote_count as f64
                / 1_000_000_000.0
        };
        let mut output = String::new();
        gauge(&mut output, "lunarbase_ready", ready);
        gauge(&mut output, "lunarbase_head_block", head);
        gauge(&mut output, "lunarbase_execution_block", execution);
        gauge(
            &mut output,
            "lunarbase_execution_context_delta_blocks",
            execution_context_delta,
        );
        gauge(
            &mut output,
            "lunarbase_source_update_age_seconds",
            source_update_age,
        );
        gauge(&mut output, "lunarbase_commitment", commitment);
        gauge(&mut output, "lunarbase_queue_depth", stats.queue_depth);
        gauge(
            &mut output,
            "lunarbase_queue_capacity",
            stats.queue_capacity,
        );
        counter(
            &mut output,
            "lunarbase_source_reconnects_total",
            stats.source_reconnects,
        );
        counter(&mut output, "lunarbase_gaps_total", stats.gaps);
        counter(&mut output, "lunarbase_recoveries_total", stats.recoveries);
        counter(
            &mut output,
            "lunarbase_recovery_failures_total",
            stats.recovery_failures,
        );
        counter(&mut output, "lunarbase_quotes_total", quote_count);
        counter(
            &mut output,
            "lunarbase_quote_errors_total",
            self.quote_errors.load(Ordering::Relaxed),
        );
        counter(
            &mut output,
            "lunarbase_quote_batches_total",
            self.quote_batches.load(Ordering::Relaxed),
        );
        gauge(
            &mut output,
            "lunarbase_quote_latency_seconds_average",
            average_latency,
        );
        counter(
            &mut output,
            "lunarbase_checkpoint_success_total",
            self.checkpoint_success.load(Ordering::Relaxed),
        );
        counter(
            &mut output,
            "lunarbase_checkpoint_failure_total",
            self.checkpoint_failure.load(Ordering::Relaxed),
        );
        output
    }
}

fn gauge(output: &mut String, name: &str, value: impl std::fmt::Display) {
    let _ = writeln!(output, "# TYPE {name} gauge");
    let _ = writeln!(output, "{name} {value}");
}

fn counter(output: &mut String, name: &str, value: impl std::fmt::Display) {
    let _ = writeln!(output, "# TYPE {name} counter");
    let _ = writeln!(output, "{name} {value}");
}

fn saturating_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn source_update_age(last_update_millis: u64) -> f64 {
    if last_update_millis == 0 {
        return u64::MAX as f64 / 1_000.0;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    now.saturating_sub(last_update_millis) as f64 / 1_000.0
}
