//! Live Monad parser/indexer/RPC soak validation.

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use lunarbase_math::U256;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::watch;
use tokio::time::{interval, timeout, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// Live validation endpoints and soak bounds.
#[derive(Clone, Debug, Parser)]
#[command(name = "lunarbase-monad-validate")]
pub struct MonadArguments {
    #[arg(long, default_value = "http://127.0.0.1:8081")]
    pub indexer_url: String,
    #[arg(long, default_value = "ws://127.0.0.1:8080/ws/subscriptions")]
    pub parser_ws_url: String,
    #[arg(long, default_value = "http://127.0.0.1:8080/readyz")]
    pub parser_ready_url: String,
    #[arg(long, default_value = "http://127.0.0.1:8545")]
    pub rpc_url: String,
    /// Quote plus optional Solidity eth_call comparison vectors.
    #[arg(long)]
    pub vectors: Option<PathBuf>,
    #[arg(long, default_value_t = 3_600)]
    pub duration_seconds: u64,
    #[arg(long, default_value_t = 1_000)]
    pub sample_interval_milliseconds: u64,
    /// Optional JSON report destination; the report is always printed.
    #[arg(long)]
    pub report: Option<PathBuf>,
}

#[derive(Debug, Error)]
pub enum MonadError {
    #[error("I/O failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP failure: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON failure: {0}")]
    Json(#[from] serde_json::Error),
    #[error("WebSocket failure: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("validation failed: {0}")]
    Validation(String),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ValidationVector {
    quote: Value,
    #[serde(default)]
    solidity: Option<SolidityCall>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SolidityCall {
    to: String,
    data: String,
    #[serde(default = "latest")]
    block_tag: String,
    /// `amountIn` or `amountOut` in the indexer quote outcome.
    quote_field: String,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct ParserReport {
    messages: u64,
    heads: u64,
    health_messages: u64,
    alerts: u64,
    explicit_gaps: u64,
    sequence_regressions: u64,
    commitment_regressions: u64,
    last_sequence: Option<u64>,
    last_block: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MonadReport {
    duration_seconds: f64,
    samples: u64,
    parser: ParserReport,
    parser_ready_failures: u64,
    indexer_readiness_failures: u64,
    rpc_failures: u64,
    maximum_indexer_lag_blocks: u64,
    quote_comparisons: u64,
    quote_mismatches: u64,
    reconnects_delta: f64,
    gaps_delta: f64,
    recoveries_delta: f64,
    recovery_failures_delta: f64,
    status: &'static str,
}

/// Monitors parser sequencing/commitments and repeatedly compares indexer
/// quotes with direct Solidity `eth_call` results.
pub async fn run(arguments: MonadArguments) -> Result<(), MonadError> {
    if arguments.duration_seconds == 0 || arguments.sample_interval_milliseconds == 0 {
        return Err(MonadError::Validation(
            "duration and sample interval must be non-zero".into(),
        ));
    }
    let vectors = match &arguments.vectors {
        Some(path) => serde_json::from_slice::<Vec<ValidationVector>>(&std::fs::read(path)?)?,
        None => Vec::new(),
    };
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    require_success(&http, &arguments.parser_ready_url, "Monad parser").await?;
    require_success(
        &http,
        &format!(
            "{}/health/ready",
            arguments.indexer_url.trim_end_matches('/')
        ),
        "LunarBase indexer",
    )
    .await?;

    let metrics_before = metrics(&http, &arguments.indexer_url).await;
    let (stop, parser_stop) = watch::channel(false);
    let parser_url = arguments.parser_ws_url.clone();
    let parser_task = tokio::spawn(async move { monitor_parser(&parser_url, parser_stop).await });
    let started = Instant::now();
    let mut ticker = interval(Duration::from_millis(
        arguments.sample_interval_milliseconds,
    ));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut samples = 0u64;
    let mut parser_ready_failures = 0u64;
    let mut indexer_readiness_failures = 0u64;
    let mut rpc_failures = 0u64;
    let mut maximum_lag = 0u64;
    let mut quote_comparisons = 0u64;
    let mut quote_mismatches = 0u64;

    while started.elapsed() < Duration::from_secs(arguments.duration_seconds) {
        ticker.tick().await;
        samples = samples.saturating_add(1);
        if !is_success(&http, &arguments.parser_ready_url).await {
            parser_ready_failures = parser_ready_failures.saturating_add(1);
        }
        let indexer_health_url = format!(
            "{}/health/ready",
            arguments.indexer_url.trim_end_matches('/')
        );
        let indexer_health = http.get(&indexer_health_url).send().await;
        let indexed_block = match indexer_health {
            Ok(response) if response.status().is_success() => {
                response.json::<Value>().await.ok().and_then(|value| {
                    value
                        .pointer("/cursor/blockNumber")?
                        .as_str()?
                        .parse::<u64>()
                        .ok()
                })
            }
            _ => {
                indexer_readiness_failures = indexer_readiness_failures.saturating_add(1);
                None
            }
        };
        match rpc_block_number(&http, &arguments.rpc_url).await {
            Ok(rpc_block) => {
                if let Some(indexed_block) = indexed_block {
                    maximum_lag = maximum_lag.max(rpc_block.saturating_sub(indexed_block));
                    if indexed_block > rpc_block {
                        quote_mismatches = quote_mismatches.saturating_add(1);
                    }
                }
            }
            Err(_) => rpc_failures = rpc_failures.saturating_add(1),
        }

        for vector in &vectors {
            let Some(solidity) = &vector.solidity else {
                continue;
            };
            quote_comparisons = quote_comparisons.saturating_add(1);
            if !compare_vector(&http, &arguments, vector, solidity).await {
                quote_mismatches = quote_mismatches.saturating_add(1);
            }
        }
    }

    let _ = stop.send(true);
    let parser = timeout(Duration::from_secs(5), parser_task)
        .await
        .map_err(|_| MonadError::Validation("parser monitor did not stop".into()))?
        .map_err(|error| MonadError::Validation(format!("parser monitor panicked: {error}")))??;
    let metrics_after = metrics(&http, &arguments.indexer_url).await;
    let clean = parser.explicit_gaps == 0
        && parser.sequence_regressions == 0
        && parser.commitment_regressions == 0
        && parser_ready_failures == 0
        && indexer_readiness_failures == 0
        && rpc_failures == 0
        && quote_mismatches == 0;
    let report = MonadReport {
        duration_seconds: started.elapsed().as_secs_f64(),
        samples,
        parser,
        parser_ready_failures,
        indexer_readiness_failures,
        rpc_failures,
        maximum_indexer_lag_blocks: maximum_lag,
        quote_comparisons,
        quote_mismatches,
        reconnects_delta: metric_delta(
            &metrics_before,
            &metrics_after,
            "lunarbase_source_reconnects_total",
        ),
        gaps_delta: metric_delta(
            &metrics_before,
            &metrics_after,
            "lunarbase_source_gaps_total",
        ),
        recoveries_delta: metric_delta(
            &metrics_before,
            &metrics_after,
            "lunarbase_recoveries_total",
        ),
        recovery_failures_delta: metric_delta(
            &metrics_before,
            &metrics_after,
            "lunarbase_recovery_failures_total",
        ),
        status: if clean { "ok" } else { "failed" },
    };
    let serialized = serde_json::to_string_pretty(&report)?;
    println!("{serialized}");
    if let Some(path) = arguments.report {
        std::fs::write(path, &serialized)?;
    }
    if !clean {
        return Err(MonadError::Validation(
            "Monad live validation reported sequencing, readiness, RPC, or quote mismatches".into(),
        ));
    }
    Ok(())
}

async fn monitor_parser(
    url: &str,
    mut stop: watch::Receiver<bool>,
) -> Result<ParserReport, MonadError> {
    let (socket, _) = connect_async(url).await?;
    let (mut writer, mut reader) = socket.split();
    writer
        .send(Message::Text(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "subscribe",
                "params": ["all"],
            })
            .to_string(),
        ))
        .await?;
    let mut report = ParserReport::default();
    let mut block_commitments = BTreeMap::<u64, u8>::new();
    loop {
        let message = tokio::select! {
            biased;
            () = stop_requested(&mut stop) => break,
            message = reader.next() => message,
        };
        let Some(message) = message else {
            break;
        };
        let message = message?;
        let payload = match message {
            Message::Text(text) => text.to_string(),
            Message::Binary(bytes) => String::from_utf8(bytes.to_vec())
                .map_err(|error| MonadError::Validation(error.to_string()))?,
            Message::Ping(bytes) => {
                writer.send(Message::Pong(bytes)).await?;
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => break,
            _ => continue,
        };
        let value: Value = serde_json::from_str(&payload)?;
        report.messages = report.messages.saturating_add(1);
        if value.get("method").and_then(Value::as_str) == Some("subscriptionGap") {
            report.explicit_gaps = report.explicit_gaps.saturating_add(1);
            continue;
        }
        let Some(result) = value.get("result").and_then(Value::as_object) else {
            continue;
        };
        match result.get("type").and_then(Value::as_str) {
            Some("newHead") => {
                report.heads = report.heads.saturating_add(1);
                let sequence = parse_u64(result.get("seqno"));
                if let Some(sequence) = sequence {
                    if report
                        .last_sequence
                        .is_some_and(|previous| sequence < previous)
                    {
                        report.sequence_regressions = report.sequence_regressions.saturating_add(1);
                    }
                    report.last_sequence = Some(
                        report
                            .last_sequence
                            .map_or(sequence, |previous| previous.max(sequence)),
                    );
                }
                if let Some(block) = parse_u64(result.get("blockNumber")) {
                    report.last_block =
                        Some(report.last_block.map_or(block, |last| last.max(block)));
                    let commitment = commitment_rank(
                        result
                            .get("commitment")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    );
                    if block_commitments
                        .get(&block)
                        .is_some_and(|previous| commitment < *previous)
                    {
                        report.commitment_regressions =
                            report.commitment_regressions.saturating_add(1);
                    }
                    block_commitments
                        .entry(block)
                        .and_modify(|previous| *previous = (*previous).max(commitment))
                        .or_insert(commitment);
                    while block_commitments.len() > 512 {
                        block_commitments.pop_first();
                    }
                }
            }
            Some("health") => {
                report.health_messages = report.health_messages.saturating_add(1);
                if result.get("stalled").and_then(Value::as_bool) == Some(true) {
                    report.explicit_gaps = report.explicit_gaps.saturating_add(1);
                }
            }
            Some("alert") => {
                report.alerts = report.alerts.saturating_add(1);
                let message = result
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                if ["gap", "expired", "stalled", "ring"]
                    .iter()
                    .any(|needle| message.contains(needle))
                {
                    report.explicit_gaps = report.explicit_gaps.saturating_add(1);
                }
            }
            _ => {}
        }
    }
    Ok(report)
}

async fn compare_vector(
    http: &reqwest::Client,
    arguments: &MonadArguments,
    vector: &ValidationVector,
    solidity: &SolidityCall,
) -> bool {
    let quote = http
        .post(format!(
            "{}/v1/quote",
            arguments.indexer_url.trim_end_matches('/')
        ))
        .json(&vector.quote)
        .send()
        .await;
    let Ok(quote) = quote else {
        return false;
    };
    let Ok(quote) = quote.json::<Value>().await else {
        return false;
    };
    let Some(quote_amount) = quote
        .pointer(&format!("/outcome/{}", solidity.quote_field))
        .and_then(Value::as_str)
        .and_then(|value| U256::from_str(value).ok())
    else {
        return false;
    };
    let rpc = http
        .post(&arguments.rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [{
                "to": solidity.to,
                "data": solidity.data,
            }, solidity.block_tag],
        }))
        .send()
        .await;
    let Ok(rpc) = rpc else {
        return false;
    };
    let Ok(rpc) = rpc.json::<Value>().await else {
        return false;
    };
    rpc.get("result")
        .and_then(Value::as_str)
        .and_then(parse_u256_hex)
        .is_some_and(|solidity_amount| solidity_amount == quote_amount)
}

async fn rpc_block_number(http: &reqwest::Client, rpc_url: &str) -> Result<u64, MonadError> {
    let response: Value = http
        .post(rpc_url)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_blockNumber",
            "params": [],
        }))
        .send()
        .await?
        .json()
        .await?;
    let value = response
        .get("result")
        .and_then(Value::as_str)
        .ok_or_else(|| MonadError::Validation("eth_blockNumber result is missing".into()))?;
    u64::from_str_radix(value.trim_start_matches("0x"), 16)
        .map_err(|error| MonadError::Validation(error.to_string()))
}

async fn require_success(
    client: &reqwest::Client,
    url: &str,
    component: &str,
) -> Result<(), MonadError> {
    if is_success(client, url).await {
        Ok(())
    } else {
        Err(MonadError::Validation(format!(
            "{component} is not ready at {url}"
        )))
    }
}

async fn is_success(client: &reqwest::Client, url: &str) -> bool {
    client
        .get(url)
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())
}

async fn metrics(client: &reqwest::Client, indexer_url: &str) -> String {
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

fn parse_u64(value: Option<&Value>) -> Option<u64> {
    value?.as_u64().or_else(|| value?.as_str()?.parse().ok())
}

fn parse_u256_hex(value: &str) -> Option<U256> {
    let value = value.trim_start_matches("0x");
    if value.is_empty() || value.len() > 64 {
        return None;
    }
    let padded = format!("{value:0>64}");
    let mut bytes = [0u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&padded[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(U256::from_be_bytes(bytes))
}

const fn commitment_rank(commitment: &str) -> u8 {
    match commitment.as_bytes() {
        b"proposed" => 0,
        b"finalized" => 1,
        b"verified" => 2,
        _ => 0,
    }
}

fn latest() -> String {
    "latest".into()
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
