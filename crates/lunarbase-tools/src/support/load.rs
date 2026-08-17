//! Concurrent quote/load harness with latency and process-memory reporting.

mod metrics;
mod request;
mod vectors;

use clap::Parser;
use metrics::{fetch_metrics, memory_delta, metric_delta, process_memory, wait_for_ready};
use request::{percentile, run_phase};
use serde::Serialize;
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use vectors::{load_vectors, prepare_bodies};

pub use vectors::lane_address;

const REPORT_SCHEMA_VERSION: u32 = 2;

/// Load target and requested topology.
#[derive(Clone, Debug, Parser)]
#[command(name = "lunarbase-load")]
pub struct LoadArguments {
    /// Base HTTP URL of the indexer under test.
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub indexer_url: String,
    /// JSON array of quote request objects. Synthetic requests are used when omitted.
    #[arg(long)]
    pub vectors: Option<PathBuf>,
    /// Number of distinct lane states represented by the test topology.
    #[arg(long, default_value_t = 15)]
    pub lanes: usize,
    /// Number of distinct directed quote pairs represented by request vectors.
    #[arg(long, default_value_t = 100)]
    pub pairs: usize,
    /// Quotes sent in one HTTP call; one uses `/v1/quote`, larger values use `/v1/quotes`.
    #[arg(long, default_value_t = 1)]
    pub batch_size: usize,
    /// Total measured HTTP calls issued during the run.
    #[arg(long, default_value_t = 20_000)]
    pub requests: usize,
    /// HTTP calls issued before memory and latency measurement.
    #[arg(long, default_value_t = 1_024)]
    pub warmup_requests: usize,
    /// Maximum number of in-flight HTTP calls.
    #[arg(long, default_value_t = 128)]
    pub concurrency: usize,
    /// Number of events submitted to the optional test-source control endpoint.
    #[arg(long, default_value_t = 1_000)]
    pub burst_size: usize,
    /// Optional test-source control endpoint accepting an event burst request.
    #[arg(long)]
    pub event_burst_url: Option<String>,
    /// Indexer process ID used for Linux VmRSS and VmHWM measurements.
    #[arg(long)]
    pub pid: Option<u32>,
    /// Maximum wait for `/readyz` to report ready before the run fails.
    #[arg(long, default_value_t = 15)]
    pub readiness_timeout_seconds: u64,
}

#[derive(Debug, Error)]
/// Configuration, input, or transport failure in the load harness.
pub enum LoadError {
    /// Vector input or operating-system metrics could not be read.
    #[error("I/O failure: {0}")]
    Io(#[from] std::io::Error),
    /// Quote, metrics, or event-control HTTP request failed.
    #[error("HTTP failure: {0}")]
    Http(#[from] reqwest::Error),
    /// Supplied quote vectors are not valid JSON.
    #[error("invalid vector JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// A requested load dimension is empty or internally inconsistent.
    #[error("invalid load settings: {0}")]
    Invalid(String),
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
/// Versioned machine-readable result of one HTTP load scenario.
pub struct LoadReport {
    schema_version: u32,
    kind: &'static str,
    indexer_url: String,
    process_id: Option<u32>,
    lanes: usize,
    pairs: usize,
    batch_size: usize,
    concurrency: usize,
    warmup_http_requests: usize,
    measured_http_requests: usize,
    measured_quotes: usize,
    successful_http_requests: usize,
    failed_http_requests: usize,
    first_error: Option<String>,
    readiness_seconds: f64,
    duration_seconds: f64,
    throughput_http_requests_per_second: f64,
    throughput_quotes_per_second: f64,
    p50_milliseconds: f64,
    p95_milliseconds: f64,
    p99_milliseconds: f64,
    rss_bytes_ready: Option<u64>,
    rss_bytes_before: Option<u64>,
    rss_bytes_after: Option<u64>,
    rss_delta_bytes: Option<i64>,
    peak_rss_bytes_ready: Option<u64>,
    peak_rss_bytes_before: Option<u64>,
    peak_rss_bytes_after: Option<u64>,
    peak_rss_delta_bytes: Option<i64>,
    indexed_block_delta: f64,
    checkpoint_commits_delta: f64,
    checkpoint_commits_per_second: f64,
    event_burst_requested: bool,
}

impl LoadReport {
    /// Fails when any measured response was unavailable, malformed, or unsuccessful.
    pub fn ensure_success(&self) -> Result<(), LoadError> {
        if self.failed_http_requests == 0 {
            return Ok(());
        }
        Err(LoadError::Invalid(format!(
            "{} of {} measured HTTP requests failed; first error: {}",
            self.failed_http_requests,
            self.measured_http_requests,
            self.first_error.as_deref().unwrap_or("unknown")
        )))
    }
}

/// Executes bounded concurrent requests and prints one machine-readable report.
pub async fn run(arguments: LoadArguments) -> Result<(), LoadError> {
    let report = execute(arguments).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    report.ensure_success()
}

/// Executes one scenario without printing, allowing a process-level matrix to aggregate reports.
pub async fn execute(arguments: LoadArguments) -> Result<LoadReport, LoadError> {
    validate_arguments(&arguments)?;
    let vectors = load_vectors(&arguments)?;
    let bodies = Arc::new(prepare_bodies(&vectors, arguments.batch_size)?);
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(arguments.concurrency)
        .timeout(Duration::from_secs(10))
        .build()?;
    let readiness_started = Instant::now();
    wait_for_ready(
        &client,
        &arguments.indexer_url,
        Duration::from_secs(arguments.readiness_timeout_seconds),
    )
    .await?;
    let readiness_seconds = readiness_started.elapsed().as_secs_f64();
    let memory_ready = process_memory(arguments.pid)?;
    let event_burst_requested = request_event_burst(&client, &arguments).await?;
    let endpoint = Arc::new(quote_endpoint(&arguments.indexer_url, arguments.batch_size));
    let warmup = run_phase(
        client.clone(),
        endpoint.clone(),
        bodies.clone(),
        arguments.batch_size,
        arguments.warmup_requests,
        arguments.concurrency,
    )
    .await?;
    warmup.ensure_success("warmup")?;

    let metrics_before = fetch_metrics(&client, &arguments.indexer_url).await?;
    let memory_before = process_memory(arguments.pid)?;
    let measured = run_phase(
        client.clone(),
        endpoint,
        bodies,
        arguments.batch_size,
        arguments.requests,
        arguments.concurrency,
    )
    .await?;
    let memory_after = process_memory(arguments.pid)?;
    let metrics_after = fetch_metrics(&client, &arguments.indexer_url).await?;
    let elapsed_seconds = measured.elapsed.as_secs_f64();
    let checkpoint_delta = metric_delta(
        &metrics_before,
        &metrics_after,
        "lunarbase_checkpoint_success_total",
    )?;
    let indexed_block_delta =
        metric_delta(&metrics_before, &metrics_after, "lunarbase_head_block")?;
    Ok(LoadReport {
        schema_version: REPORT_SCHEMA_VERSION,
        kind: "lunarbase-indexer-http-load",
        indexer_url: arguments.indexer_url,
        process_id: arguments.pid,
        lanes: arguments.lanes,
        pairs: arguments.pairs,
        batch_size: arguments.batch_size,
        concurrency: arguments.concurrency,
        warmup_http_requests: arguments.warmup_requests,
        measured_http_requests: arguments.requests,
        measured_quotes: arguments.requests.saturating_mul(arguments.batch_size),
        successful_http_requests: measured.successful,
        failed_http_requests: arguments.requests.saturating_sub(measured.successful),
        first_error: measured.first_error,
        readiness_seconds,
        duration_seconds: elapsed_seconds,
        throughput_http_requests_per_second: arguments.requests as f64 / elapsed_seconds,
        throughput_quotes_per_second: arguments.requests.saturating_mul(arguments.batch_size)
            as f64
            / elapsed_seconds,
        p50_milliseconds: percentile(&measured.latencies, 0.50),
        p95_milliseconds: percentile(&measured.latencies, 0.95),
        p99_milliseconds: percentile(&measured.latencies, 0.99),
        rss_bytes_ready: memory_ready.map(|sample| sample.rss_bytes),
        rss_bytes_before: memory_before.map(|sample| sample.rss_bytes),
        rss_bytes_after: memory_after.map(|sample| sample.rss_bytes),
        rss_delta_bytes: memory_delta(memory_before, memory_after, |sample| sample.rss_bytes),
        peak_rss_bytes_ready: memory_ready.map(|sample| sample.peak_rss_bytes),
        peak_rss_bytes_before: memory_before.map(|sample| sample.peak_rss_bytes),
        peak_rss_bytes_after: memory_after.map(|sample| sample.peak_rss_bytes),
        peak_rss_delta_bytes: memory_delta(memory_before, memory_after, |sample| {
            sample.peak_rss_bytes
        }),
        indexed_block_delta,
        checkpoint_commits_delta: checkpoint_delta,
        checkpoint_commits_per_second: checkpoint_delta / elapsed_seconds,
        event_burst_requested,
    })
}

fn validate_arguments(arguments: &LoadArguments) -> Result<(), LoadError> {
    if arguments.lanes == 0
        || arguments.pairs == 0
        || arguments.requests == 0
        || arguments.warmup_requests == 0
        || arguments.concurrency == 0
        || arguments.burst_size == 0
        || arguments.readiness_timeout_seconds == 0
        || !(1..=256).contains(&arguments.batch_size)
    {
        return Err(LoadError::Invalid(
            "lanes, pairs, requests, warmup requests, concurrency, burst size, and readiness timeout must be non-zero; batch size must be 1..=256".into(),
        ));
    }
    let available_pairs = arguments.lanes.saturating_mul(arguments.lanes);
    if arguments.vectors.is_none() && arguments.pairs > available_pairs {
        return Err(LoadError::Invalid(format!(
            "{} lanes provide only {available_pairs} distinct directed pairs",
            arguments.lanes
        )));
    }
    Ok(())
}

async fn request_event_burst(
    client: &reqwest::Client,
    arguments: &LoadArguments,
) -> Result<bool, LoadError> {
    let Some(url) = &arguments.event_burst_url else {
        return Ok(false);
    };
    client
        .post(url)
        .json(&json!({
            "events": arguments.burst_size,
            "lanes": arguments.lanes,
            "pairs": arguments.pairs,
        }))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    Ok(true)
}

fn quote_endpoint(indexer_url: &str, batch_size: usize) -> String {
    let path = if batch_size == 1 {
        "/v1/quote"
    } else {
        "/v1/quotes"
    };
    format!("{}{path}", indexer_url.trim_end_matches('/'))
}
