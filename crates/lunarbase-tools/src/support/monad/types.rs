use crate::support::monad::helpers::latest;
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use thiserror::Error;

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
pub(super) struct ValidationVector {
    pub(super) quote: Value,
    #[serde(default)]
    pub(super) solidity: Option<SolidityCall>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SolidityCall {
    pub(super) to: String,
    pub(super) data: String,
    #[serde(default = "latest")]
    pub(super) block_tag: String,
    /// `amountIn` or `amountOut` in the indexer quote outcome.
    pub(super) quote_field: String,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ParserReport {
    pub(super) messages: u64,
    pub(super) heads: u64,
    pub(super) health_messages: u64,
    pub(super) alerts: u64,
    pub(super) explicit_gaps: u64,
    pub(super) sequence_regressions: u64,
    pub(super) commitment_regressions: u64,
    pub(super) last_sequence: Option<u64>,
    pub(super) last_block: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MonadReport {
    pub(super) duration_seconds: f64,
    pub(super) samples: u64,
    pub(super) parser: ParserReport,
    pub(super) parser_ready_failures: u64,
    pub(super) indexer_readiness_failures: u64,
    pub(super) rpc_failures: u64,
    pub(super) maximum_indexer_lag_blocks: u64,
    pub(super) quote_comparisons: u64,
    pub(super) quote_mismatches: u64,
    pub(super) reconnects_delta: f64,
    pub(super) gaps_delta: f64,
    pub(super) recoveries_delta: f64,
    pub(super) recovery_failures_delta: f64,
    pub(super) status: &'static str,
}
