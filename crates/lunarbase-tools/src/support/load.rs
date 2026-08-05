//! Concurrent quote/load harness with latency and memory reporting.

use clap::Parser;
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Semaphore;

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
    /// Number of distinct quote pairs represented by request vectors.
    #[arg(long, default_value_t = 100)]
    pub pairs: usize,
    /// Total HTTP quote requests issued during the run.
    #[arg(long, default_value_t = 20_000)]
    pub requests: usize,
    /// Maximum number of in-flight quote requests.
    #[arg(long, default_value_t = 128)]
    pub concurrency: usize,
    /// Number of requests released in each scheduling burst.
    #[arg(long, default_value_t = 1_000)]
    pub burst_size: usize,
    /// Optional test-source control endpoint accepting an event burst request.
    #[arg(long)]
    pub event_burst_url: Option<String>,
    /// Indexer process ID used for RSS and per-lane/per-pair memory estimates.
    #[arg(long)]
    pub pid: Option<u32>,
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
struct LoadReport {
    lanes: usize,
    pairs: usize,
    requests: usize,
    successful: usize,
    failed: usize,
    duration_seconds: f64,
    throughput_requests_per_second: f64,
    p50_milliseconds: f64,
    p95_milliseconds: f64,
    p99_milliseconds: f64,
    rss_bytes_before: Option<u64>,
    rss_bytes_after: Option<u64>,
    bytes_per_lane: Option<u64>,
    bytes_per_pair: Option<u64>,
    indexed_block_delta: f64,
    checkpoint_commits_delta: f64,
    checkpoint_commits_per_second: f64,
    event_burst_requested: bool,
}

/// Executes bounded concurrent requests and prints one machine-readable report.
pub async fn run(arguments: LoadArguments) -> Result<(), LoadError> {
    if arguments.lanes == 0
        || arguments.pairs == 0
        || arguments.requests == 0
        || arguments.concurrency == 0
        || arguments.burst_size == 0
    {
        return Err(LoadError::Invalid(
            "lanes, pairs, requests, concurrency, and burst size must be non-zero".into(),
        ));
    }
    let vectors = load_vectors(&arguments)?;
    if vectors.is_empty() {
        return Err(LoadError::Invalid(
            "at least one quote vector is required".into(),
        ));
    }
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(arguments.concurrency)
        .timeout(Duration::from_secs(10))
        .build()?;
    let metrics_before = fetch_metrics(&client, &arguments.indexer_url).await;
    let rss_before = process_rss(arguments.pid).await;

    let mut event_burst_requested = false;
    if let Some(url) = &arguments.event_burst_url {
        client
            .post(url)
            .json(&json!({
                "events": arguments.burst_size,
                "lanes": arguments.lanes,
                "pairs": arguments.pairs,
            }))
            .send()
            .await?
            .error_for_status()?;
        event_burst_requested = true;
    }

    let semaphore = std::sync::Arc::new(Semaphore::new(arguments.concurrency));
    let endpoint = format!("{}/v1/quote", arguments.indexer_url.trim_end_matches('/'));
    let started = Instant::now();
    let mut requests = FuturesUnordered::new();
    for index in 0..arguments.requests {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| LoadError::Invalid(error.to_string()))?;
        let client = client.clone();
        let endpoint = endpoint.clone();
        let vector = vectors[index % vectors.len()].clone();
        requests.push(tokio::spawn(async move {
            let _permit = permit;
            let request_started = Instant::now();
            let success = client
                .post(endpoint)
                .json(&vector)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success());
            (request_started.elapsed(), success)
        }));
        if (index + 1) % arguments.burst_size == 0 {
            tokio::task::yield_now().await;
        }
    }

    let mut latencies = Vec::with_capacity(arguments.requests);
    let mut successful = 0usize;
    while let Some(result) = requests.next().await {
        let (latency, success) =
            result.map_err(|error| LoadError::Invalid(format!("load worker failed: {error}")))?;
        latencies.push(latency.as_secs_f64() * 1_000.0);
        successful += usize::from(success);
    }
    let elapsed = started.elapsed();
    latencies.sort_by(f64::total_cmp);
    let metrics_after = fetch_metrics(&client, &arguments.indexer_url).await;
    let rss_after = process_rss(arguments.pid).await;
    let checkpoint_delta = metric_delta(
        &metrics_before,
        &metrics_after,
        "lunarbase_checkpoint_success_total",
    );
    let report = LoadReport {
        lanes: arguments.lanes,
        pairs: arguments.pairs,
        requests: arguments.requests,
        successful,
        failed: arguments.requests.saturating_sub(successful),
        duration_seconds: elapsed.as_secs_f64(),
        throughput_requests_per_second: (arguments.requests as f64) / elapsed.as_secs_f64(),
        p50_milliseconds: percentile(&latencies, 0.50),
        p95_milliseconds: percentile(&latencies, 0.95),
        p99_milliseconds: percentile(&latencies, 0.99),
        rss_bytes_before: rss_before,
        rss_bytes_after: rss_after,
        bytes_per_lane: rss_after.map(|rss| rss / arguments.lanes as u64),
        bytes_per_pair: rss_after.map(|rss| rss / arguments.pairs as u64),
        indexed_block_delta: metric_delta(&metrics_before, &metrics_after, "lunarbase_head_block"),
        checkpoint_commits_delta: checkpoint_delta,
        checkpoint_commits_per_second: checkpoint_delta / elapsed.as_secs_f64(),
        event_burst_requested,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    if successful != arguments.requests {
        return Err(LoadError::Invalid(format!(
            "{} of {} quote requests failed",
            arguments.requests - successful,
            arguments.requests
        )));
    }
    Ok(())
}

fn load_vectors(arguments: &LoadArguments) -> Result<Vec<Value>, LoadError> {
    if let Some(path) = &arguments.vectors {
        return Ok(serde_json::from_slice(&std::fs::read(path)?)?);
    }
    let cash = address(1);
    Ok((0..arguments.pairs)
        .map(|index| {
            let asset = address(u64::try_from(index % arguments.lanes).unwrap_or(0) + 10);
            json!({
                "assetIn": cash,
                "assetOut": asset,
                "amount": (1_000 + index).to_string(),
                "mode": "exactIn"
            })
        })
        .collect())
}

async fn fetch_metrics(client: &reqwest::Client, indexer_url: &str) -> String {
    let url = format!("{}/metrics", indexer_url.trim_end_matches('/'));
    match client.get(url).send().await {
        Ok(response) => response.text().await.unwrap_or_default(),
        Err(_) => String::new(),
    }
}

fn metric_delta(before: &str, after: &str, name: &str) -> f64 {
    metric_value(after, name) - metric_value(before, name)
}

fn metric_value(metrics: &str, name: &str) -> f64 {
    metrics
        .lines()
        .find_map(|line| {
            let (metric, value) = line.split_once(' ')?;
            (metric == name)
                .then(|| value.parse::<f64>().ok())
                .flatten()
        })
        .unwrap_or(0.0)
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = (((sorted.len() - 1) as f64) * quantile).round() as usize;
    sorted[index]
}

async fn process_rss(pid: Option<u32>) -> Option<u64> {
    let pid = pid?;
    let output = tokio::process::Command::new("ps")
        .arg("-o")
        .arg("rss=")
        .arg("-p")
        .arg(pid.to_string())
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let kibibytes = String::from_utf8(output.stdout)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(kibibytes.saturating_mul(1024))
}

fn address(value: u64) -> String {
    format!("0x{value:040x}")
}
