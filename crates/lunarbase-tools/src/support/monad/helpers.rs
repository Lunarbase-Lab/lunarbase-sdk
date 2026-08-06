use crate::support::monad::types::MonadError;
use alloy_primitives::U64;
use lunarbase_math::U256;
use serde_json::{Value, json};
use std::str::FromStr;
use tokio::sync::watch;

pub(super) async fn rpc_block_number(
    http: &reqwest::Client,
    rpc_url: &str,
) -> Result<u64, MonadError> {
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
    U64::from_str(value)
        .map(|value| value.to::<u64>())
        .map_err(|error| MonadError::Validation(error.to_string()))
}

pub(super) async fn require_success(
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

pub(super) async fn is_success(client: &reqwest::Client, url: &str) -> bool {
    client
        .get(url)
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())
}

pub(super) async fn metrics(client: &reqwest::Client, indexer_url: &str) -> String {
    let url = format!("{}/metrics", indexer_url.trim_end_matches('/'));
    match client.get(url).send().await {
        Ok(response) => response.text().await.unwrap_or_default(),
        Err(_) => String::new(),
    }
}

pub(super) fn metric_delta(before: &str, after: &str, name: &str) -> f64 {
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

pub(super) fn parse_u64(value: Option<&Value>) -> Option<u64> {
    value?.as_u64().or_else(|| value?.as_str()?.parse().ok())
}

pub(super) fn parse_u256_hex(value: &str) -> Option<U256> {
    U256::from_str(value).ok()
}

pub(super) const fn commitment_rank(commitment: &str) -> u8 {
    match commitment.as_bytes() {
        b"proposed" => 0,
        b"finalized" => 1,
        b"verified" => 2,
        _ => 0,
    }
}

pub(super) fn latest() -> String {
    "latest".into()
}

pub(super) async fn stop_requested(stop: &mut watch::Receiver<bool>) {
    if *stop.borrow() {
        return;
    }
    loop {
        if stop.changed().await.is_err() || *stop.borrow() {
            return;
        }
    }
}
