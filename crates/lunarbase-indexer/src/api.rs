//! Minimal HTTP quote, health, and Prometheus API.

use crate::metrics::Metrics;
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use lunarbase_client_core::indexer::client::ConnectedQuoteClient;
use lunarbase_client_core::indexer::errors::IndexerError;
use lunarbase_client_core::indexer::quote_types::{ClientBatchQuote, ClientQuote};
use lunarbase_client_core::model::{ChainCursor, Commitment};
use lunarbase_math::state::{
    QuoteMode, QuoteOutcome, QuoteRequest, QuoteResult, UnavailableReason,
};
use lunarbase_math::types::{Address, B256, U256};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{future::Future, net::SocketAddr, str::FromStr, sync::Arc, time::Instant};

#[derive(Clone)]
struct ApiState {
    client: Arc<ConnectedQuoteClient>,
    metrics: Arc<Metrics>,
}

/// Serves all HTTP endpoints until `shutdown` resolves.
pub async fn serve(
    bind: SocketAddr,
    client: Arc<ConnectedQuoteClient>,
    metrics: Arc<Metrics>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), std::io::Error> {
    let router = Router::new()
        .route("/healthz", get(live))
        .route("/readyz", get(ready))
        .route("/metrics", get(prometheus))
        .route("/v1/quote", post(quote))
        .route("/v1/quotes", post(quotes))
        .with_state(ApiState { client, metrics });
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
}

async fn live() -> impl IntoResponse {
    Json(json!({"live": true}))
}

async fn ready(State(state): State<ApiState>) -> Response {
    match state.client.health() {
        Ok(health) if health.ready => (
            StatusCode::OK,
            Json(json!({
                "ready": true,
                "cursor": health.cursor.as_ref().map(ApiCursor::from),
                "executionBlockNumber": health.execution_block_number,
                "contractCodeHash": hash_hex(health.code_hash),
                "mathCompatibilityVersion": health.math_compatibility_version,
            })),
        )
            .into_response(),
        Ok(health) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "ready": false,
                "cursor": health.cursor.as_ref().map(ApiCursor::from),
                "contractCodeHash": hash_hex(health.code_hash),
                "mathCompatibilityVersion": health.math_compatibility_version,
            })),
        )
            .into_response(),
        Err(error) => api_error(StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
    }
}

async fn prometheus(State(state): State<ApiState>) -> impl IntoResponse {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "text/plain; version=0.0.4"
            .parse()
            .expect("static content type"),
    );
    (headers, state.metrics.render(&state.client))
}

async fn quote(State(state): State<ApiState>, Json(payload): Json<ApiQuoteRequest>) -> Response {
    let started = Instant::now();
    let result = payload
        .parse()
        .and_then(|request| state.client.quote(&request).map_err(ApiInputError::Runtime));
    state
        .metrics
        .record_quote(started.elapsed(), result.is_err(), false);
    match result {
        Ok(quote) => Json(ApiQuoteResponse::from(quote)).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn quotes(State(state): State<ApiState>, Json(payload): Json<ApiBatchRequest>) -> Response {
    let started = Instant::now();
    let result = payload.parse().and_then(|requests| {
        if requests.len() > 256 {
            return Err(ApiInputError::Invalid(
                "quotes accepts at most 256 requests".into(),
            ));
        }
        state
            .client
            .quote_many(&requests)
            .map_err(ApiInputError::Runtime)
    });
    state
        .metrics
        .record_quote(started.elapsed(), result.is_err(), true);
    match result {
        Ok(batch) => Json(ApiBatchResponse::from(batch)).into_response(),
        Err(error) => error.into_response(),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ApiQuoteRequest {
    asset_in: String,
    asset_out: String,
    amount: String,
    mode: ApiQuoteMode,
}

impl ApiQuoteRequest {
    fn parse(self) -> Result<QuoteRequest, ApiInputError> {
        Ok(QuoteRequest {
            asset_in: Address::from_str(&self.asset_in)
                .map_err(|error| ApiInputError::Invalid(error.to_string()))?,
            asset_out: Address::from_str(&self.asset_out)
                .map_err(|error| ApiInputError::Invalid(error.to_string()))?,
            amount: parse_u256(&self.amount)?,
            mode: self.mode.into(),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum ApiQuoteMode {
    ExactIn,
    ExactOut,
}

impl From<ApiQuoteMode> for QuoteMode {
    fn from(value: ApiQuoteMode) -> Self {
        match value {
            ApiQuoteMode::ExactIn => Self::ExactIn,
            ApiQuoteMode::ExactOut => Self::ExactOut,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ApiBatchRequest {
    Array(Vec<ApiQuoteRequest>),
    Object(ApiBatchObject),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApiBatchObject {
    requests: Vec<ApiQuoteRequest>,
}

impl ApiBatchRequest {
    fn parse(self) -> Result<Vec<QuoteRequest>, ApiInputError> {
        let requests = match self {
            Self::Array(requests) => requests,
            Self::Object(object) => object.requests,
        };
        requests.into_iter().map(ApiQuoteRequest::parse).collect()
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiQuoteResponse {
    cursor: ApiCursor,
    execution_block_number: u64,
    contract_code_hash: String,
    math_compatibility_version: String,
    result: ApiQuoteOutcome,
}

impl From<ClientQuote> for ApiQuoteResponse {
    fn from(quote: ClientQuote) -> Self {
        Self {
            cursor: ApiCursor::from(&quote.cursor),
            execution_block_number: quote.execution_block_number,
            contract_code_hash: hash_hex(quote.contract_code_hash),
            math_compatibility_version: quote.math_compatibility_version,
            result: ApiQuoteOutcome::from(quote.outcome),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiBatchResponse {
    cursor: ApiCursor,
    execution_block_number: u64,
    contract_code_hash: String,
    math_compatibility_version: String,
    results: Vec<ApiQuoteOutcome>,
}

impl From<ClientBatchQuote> for ApiBatchResponse {
    fn from(batch: ClientBatchQuote) -> Self {
        Self {
            cursor: ApiCursor::from(&batch.cursor),
            execution_block_number: batch.execution_block_number,
            contract_code_hash: hash_hex(batch.contract_code_hash),
            math_compatibility_version: batch.math_compatibility_version,
            results: batch
                .outcomes
                .into_iter()
                .map(ApiQuoteOutcome::from)
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiCursor {
    chain_id: u64,
    block_number: u64,
    execution_block_number: u64,
    block_hash: Option<String>,
    transaction_index: Option<u32>,
    log_index: Option<u32>,
    commitment: &'static str,
    source_sequence: Option<u64>,
    source_sub_index: Option<u32>,
}

impl From<&ChainCursor> for ApiCursor {
    fn from(cursor: &ChainCursor) -> Self {
        Self {
            chain_id: cursor.chain_id,
            block_number: cursor.block_number,
            execution_block_number: cursor.execution_block_number,
            block_hash: cursor.block_hash.map(hash_hex),
            transaction_index: cursor.transaction_index,
            log_index: cursor.log_index,
            commitment: match cursor.commitment {
                Commitment::Realtime => "realtime",
                Commitment::Canonical => "canonical",
                Commitment::Finalized => "finalized",
            },
            source_sequence: cursor.source_sequence,
            source_sub_index: cursor.source_sub_index,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(
    tag = "status",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum ApiQuoteOutcome {
    Available {
        amount_in: String,
        amount_out: String,
        fee_asset: String,
        fee_amount: String,
        partner_fee: String,
        treasury_fee: String,
    },
    Unavailable {
        reason: &'static str,
        asset: Option<String>,
    },
}

impl From<QuoteOutcome> for ApiQuoteOutcome {
    fn from(outcome: QuoteOutcome) -> Self {
        match outcome {
            QuoteOutcome::Available(result) => available(result),
            QuoteOutcome::Unavailable(reason) => unavailable(reason),
        }
    }
}

fn available(result: QuoteResult) -> ApiQuoteOutcome {
    ApiQuoteOutcome::Available {
        amount_in: result.amount_in.to_string(),
        amount_out: result.amount_out.to_string(),
        fee_asset: address_hex(result.fee_asset),
        fee_amount: result.fee_amount.to_string(),
        partner_fee: result.partner_fee.to_string(),
        treasury_fee: result.treasury_fee.to_string(),
    }
}

fn unavailable(reason: UnavailableReason) -> ApiQuoteOutcome {
    let (reason, asset) = match reason {
        UnavailableReason::ZeroAmount => ("zeroAmount", None),
        UnavailableReason::EqualAssets => ("equalAssets", None),
        UnavailableReason::MissingLane(asset) => ("missingLane", Some(address_hex(asset))),
        UnavailableReason::PausedLane(asset) => ("pausedLane", Some(address_hex(asset))),
        UnavailableReason::DelayedLane(asset) => ("delayedLane", Some(address_hex(asset))),
        UnavailableReason::ZeroPrice(asset) => ("zeroPrice", Some(address_hex(asset))),
        UnavailableReason::ZeroPrincipal(asset) => ("zeroPrincipal", Some(address_hex(asset))),
        UnavailableReason::ZeroAnchor => ("zeroAnchor", None),
        UnavailableReason::SpreadConsumesAnchor => ("spreadConsumesAnchor", None),
    };
    ApiQuoteOutcome::Unavailable { reason, asset }
}

enum ApiInputError {
    Invalid(String),
    Runtime(IndexerError),
}

impl IntoResponse for ApiInputError {
    fn into_response(self) -> Response {
        match self {
            Self::Invalid(detail) => api_error(StatusCode::BAD_REQUEST, detail),
            Self::Runtime(IndexerError::NotReady) => api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "indexer is not ready".into(),
            ),
            Self::Runtime(error) => api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
        }
    }
}

fn api_error(status: StatusCode, detail: String) -> Response {
    (status, Json(json!({"error": detail}))).into_response()
}

fn parse_u256(value: &str) -> Result<U256, ApiInputError> {
    U256::from_str(value).map_err(|error| ApiInputError::Invalid(error.to_string()))
}

fn address_hex(value: Address) -> String {
    format!("{value:#x}")
}

fn hash_hex(value: B256) -> String {
    format!("{value:#x}")
}

#[cfg(test)]
mod tests {
    use crate::api::{ApiQuoteOutcome, ApiQuoteRequest};
    use serde_json::json;

    #[test]
    fn outcome_fields_are_camel_case() {
        let value = serde_json::to_value(ApiQuoteOutcome::Available {
            amount_in: "1".into(),
            amount_out: "2".into(),
            fee_asset: "0x03".into(),
            fee_amount: "4".into(),
            partner_fee: "5".into(),
            treasury_fee: "6".into(),
        })
        .unwrap();
        assert_eq!(value["status"], "available");
        assert_eq!(value["amountIn"], "1");
        assert_eq!(value["treasuryFee"], "6");
        assert!(value.get("amount_in").is_none());
    }

    #[test]
    fn caller_cannot_override_runtime_policy() {
        let payload = json!({
            "assetIn": "0x0000000000000000000000000000000000000001",
            "assetOut": "0x0000000000000000000000000000000000000002",
            "amount": "1",
            "mode": "exactIn",
            "router": "0x0000000000000000000000000000000000000003"
        });
        assert!(serde_json::from_value::<ApiQuoteRequest>(payload).is_err());
    }
}
