use super::client::{RpcError, RpcHttpLimits};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) async fn bounded_http_request<Params, Response>(
    http: &reqwest::Client,
    endpoint: &str,
    request_id: &AtomicU64,
    limits: RpcHttpLimits,
    method: &'static str,
    params: Params,
) -> Result<Response, RpcError>
where
    Params: Serialize,
    Response: DeserializeOwned,
{
    let id = request_id.fetch_add(1, Ordering::Relaxed);
    let body = serde_json::to_vec(&StrictRequest {
        jsonrpc: "2.0",
        id,
        method,
        params,
    })
    .map_err(|error| RpcError::Invalid(format!("serialize JSON-RPC request: {error}")))?;
    if body.len() > limits.max_request_bytes {
        return Err(RpcError::Limit(format!(
            "JSON-RPC request body is {} bytes, limit is {}",
            body.len(),
            limits.max_request_bytes,
        )));
    }
    let response = http
        .post(endpoint)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|error| RpcError::Transport(error.to_string()))?
        .error_for_status()
        .map_err(|error| RpcError::Transport(error.to_string()))?;
    if response
        .content_length()
        .is_some_and(|length| length > limits.max_response_bytes as u64)
    {
        return Err(RpcError::Limit(
            "HTTP content-length exceeds the configured response budget".into(),
        ));
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| RpcError::Transport(error.to_string()))?;
        if chunk.len() > limits.max_response_bytes.saturating_sub(bytes.len()) {
            return Err(RpcError::Limit(
                "HTTP response body exceeded the configured byte budget".into(),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let envelope: StrictResponse<Response> = serde_json::from_slice(&bytes)
        .map_err(|error| RpcError::Invalid(format!("invalid JSON-RPC response: {error}")))?;
    if envelope.jsonrpc != "2.0" || envelope.id != id {
        return Err(RpcError::Invalid("JSON-RPC response id mismatch".into()));
    }
    if let Some(error) = envelope.error {
        return Err(RpcError::Transport(format!(
            "JSON-RPC returned an error: {error}"
        )));
    }
    envelope
        .result
        .ok_or_else(|| RpcError::Invalid("JSON-RPC response has no result".into()))
}

#[derive(Serialize)]
struct StrictRequest<Params> {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: Params,
}

#[derive(Deserialize)]
struct StrictResponse<Response> {
    jsonrpc: String,
    id: Value,
    result: Option<Response>,
    error: Option<Value>,
}
