//! HTTP endpoint handlers.

use super::model::{health_json, quote_json, QuoteApiRequest};
use super::ApiState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use lunarbase_client_core::{FreshnessPolicy, IndexerError};
use serde_json::json;
use std::time::Instant;

/// Reports process liveness without depending on RPC, Redis, or reducer state.
pub async fn live(State(state): State<ApiState>) -> impl IntoResponse {
    let status = state.runtime.status().await;
    (
        StatusCode::OK,
        Json(json!({"live": true, "role": status.role.as_str()})),
    )
}

/// Reports whether the reducer currently has a quote-safe state snapshot.
///
/// Gaps, reorg recovery, source failures, and compatibility failures make this
/// endpoint return `503` until the common runtime proves readiness again.
pub async fn ready(State(state): State<ApiState>) -> Response {
    let runtime_status = state.runtime.status().await;
    let Some(client) = state.runtime.client().await else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "ready": false,
                "role": runtime_status.role.as_str(),
                "detail": runtime_status.detail,
            })),
        )
            .into_response();
    };
    if !client.is_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "ready": false,
                "role": runtime_status.role.as_str(),
                "detail": "the active reducer is recovering or shutting down",
            })),
        )
            .into_response();
    }
    let health = client.health().await;
    let status = if health.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let mut body = health_json(health);
    if let Some(object) = body.as_object_mut() {
        object.insert("role".into(), json!(runtime_status.role.as_str()));
    }
    (status, Json(body)).into_response()
}

/// Exposes Prometheus text metrics for this process.
pub async fn metrics(State(state): State<ApiState>) -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.render(&state.runtime).await,
    )
        .into_response()
}

/// Parses and executes one exact-input or exact-output quote request.
///
/// The handler keeps every monetary value in string-encoded `uint256` form,
/// applies the requested commitment/freshness policy, and never serves through
/// an unavailable reducer state.
pub async fn quote(
    State(state): State<ApiState>,
    Json(payload): Json<QuoteApiRequest>,
) -> Response {
    let started_at = Instant::now();
    let (request, execution_block_number, minimum_commitment, max_age_blocks) =
        match payload.parse() {
            Ok(value) => value,
            Err(error) => {
                state.metrics.observe_quote(started_at.elapsed(), false);
                return api_error(StatusCode::BAD_REQUEST, "invalidRequest", error);
            }
        };
    let Some(client) = state.runtime.client().await else {
        state.metrics.observe_quote(started_at.elapsed(), false);
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "writerUnavailable",
            "this replica is not the active writer".into(),
        );
    };
    match client
        .quote_with_policy(
            &request,
            execution_block_number,
            FreshnessPolicy {
                minimum_commitment,
                max_age_blocks,
            },
        )
        .await
    {
        Ok(quote) => {
            if let Ok(execution_block) = execution_block_number.to_string().parse::<u64>() {
                state
                    .metrics
                    .observe_lag(execution_block, quote.cursor.block_number);
            }
            state.metrics.observe_quote(started_at.elapsed(), true);
            (StatusCode::OK, Json(quote_json(quote))).into_response()
        }
        Err(error) => {
            state.metrics.observe_quote(started_at.elapsed(), false);
            indexer_error(error)
        }
    }
}

fn indexer_error(error: IndexerError) -> Response {
    let (status, code) = match error {
        IndexerError::NotReady
        | IndexerError::Gap(_)
        | IndexerError::Source(_)
        | IndexerError::FreshnessUnavailable
        | IndexerError::NoCursor => (StatusCode::SERVICE_UNAVAILABLE, "notReady"),
        IndexerError::Quote(_) => (StatusCode::UNPROCESSABLE_ENTITY, "quoteError"),
        IndexerError::Reducer(_) | IndexerError::Decode(_) | IndexerError::CodeHashMismatch => {
            (StatusCode::INTERNAL_SERVER_ERROR, "internalError")
        }
    };
    api_error(status, code, error.to_string())
}

fn api_error(status: StatusCode, code: &'static str, message: String) -> Response {
    (
        status,
        Json(json!({"error": {"code": code, "message": message}})),
    )
        .into_response()
}
