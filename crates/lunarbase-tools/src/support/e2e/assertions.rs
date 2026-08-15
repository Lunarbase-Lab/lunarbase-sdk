use crate::support::e2e::environment::E2eError;
use crate::support::e2e::helpers::wait_until;
use crate::support::e2e::{ASSET, CASH, CORE};
use lunarbase_math::Address;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tokio::time::sleep;

pub(super) async fn wait_for_ready(url: &str) -> Result<(), E2eError> {
    let client = reqwest::Client::new();
    let endpoint = format!("{url}/readyz");
    let started = Instant::now();
    let mut last_observation = "no HTTP response".to_owned();
    while started.elapsed() < Duration::from_secs(12) {
        match client.get(&endpoint).send().await {
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                last_observation = format!("status={status}, body={body}");
                let ready = serde_json::from_str::<Value>(&body)
                    .ok()
                    .and_then(|body| body.get("ready").and_then(Value::as_bool));
                if status.is_success() && ready == Some(true) {
                    return Ok(());
                }
            }
            Err(error) => last_observation = error.to_string(),
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(E2eError::Scenario(format!(
        "replica at {url} did not become ready; last observation: {last_observation}"
    )))
}

pub(super) async fn wait_for_not_ready(url: &str) -> Result<(), E2eError> {
    let client = reqwest::Client::new();
    wait_until(Duration::from_secs(3), || {
        let client = client.clone();
        let endpoint = format!("{url}/readyz");
        async move {
            client
                .get(endpoint)
                .send()
                .await
                .is_ok_and(|response| response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE)
        }
    })
    .await
    .map_err(|_| E2eError::Scenario("gap never made readiness return 503".into()))
}

pub(super) async fn wait_for_block(url: &str, expected: u64) -> Result<(), E2eError> {
    let client = reqwest::Client::new();
    wait_until(Duration::from_secs(5), || {
        let client = client.clone();
        let endpoint = format!("{url}/readyz");
        async move {
            let Ok(response) = client.get(endpoint).send().await else {
                return false;
            };
            response
                .json::<Value>()
                .await
                .ok()
                .and_then(|body| {
                    let value = body.pointer("/cursor/blockNumber")?;
                    value
                        .as_u64()
                        .or_else(|| value.as_str()?.parse::<u64>().ok())
                })
                .is_some_and(|block| block >= expected)
        }
    })
    .await
    .map_err(|_| E2eError::Scenario(format!("indexer did not reach block {expected}")))
}

pub(super) async fn fetch_quote(url: &str) -> Result<Value, E2eError> {
    let response = reqwest::Client::new()
        .post(format!("{url}/v1/quote"))
        .json(&json!({
            "assetIn": CASH,
            "assetOut": ASSET,
            "amount": "100",
            "mode": "exactIn"
        }))
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(E2eError::Scenario(format!(
            "quote failed with status {}: {}",
            response.status(),
            response.text().await.unwrap_or_default()
        )));
    }
    let body: Value = response.json().await?;
    if body.pointer("/result/amountOut").is_none()
        || body.pointer("/result/status").and_then(Value::as_str) != Some("available")
    {
        return Err(E2eError::Scenario(format!(
            "quote did not contain an available amount: {body}"
        )));
    }
    Ok(body)
}

pub(super) async fn assert_checkpoint(redis_url: &str) -> Result<(), E2eError> {
    let url = redis_url.to_owned();
    let key = format!(
        "lunarbase:v6:8453:{}",
        CORE.parse::<Address>()
            .map_err(|error| E2eError::Scenario(error.to_string()))?,
    );
    let (exists, ttl) =
        tokio::task::spawn_blocking(move || -> Result<(bool, i64), redis::RedisError> {
            use redis::Commands;
            let client = redis::Client::open(url)?;
            let mut connection = client.get_connection()?;
            let exists = connection.exists(&key)?;
            let ttl = connection.ttl(&key)?;
            Ok((exists, ttl))
        })
        .await
        .map_err(|error| E2eError::Scenario(error.to_string()))??;
    if !exists {
        return Err(E2eError::Scenario(
            "final Redis checkpoint key is absent".into(),
        ));
    }
    if ttl != -1 {
        return Err(E2eError::Scenario(format!(
            "checkpoint must not expire, observed Redis TTL {ttl}"
        )));
    }
    Ok(())
}

pub(super) async fn wait_for_stream_length(
    redis_url: &str,
    expected: usize,
) -> Result<(), E2eError> {
    let url = redis_url.to_owned();
    wait_until(Duration::from_secs(8), || {
        let url = url.clone();
        async move {
            matches!(
                stream_length(&url).await,
                Ok(actual) if actual == expected
            )
        }
    })
    .await
    .map_err(|_| {
        E2eError::Scenario(format!(
            "event stream did not reach exactly {expected} entries"
        ))
    })
}

pub(super) async fn wait_for_metric(url: &str, metric: &str, minimum: u64) -> Result<(), E2eError> {
    let client = reqwest::Client::new();
    wait_until(Duration::from_secs(8), || {
        let client = client.clone();
        let endpoint = format!("{url}/metrics");
        async move {
            let Ok(response) = client.get(endpoint).send().await else {
                return false;
            };
            response
                .text()
                .await
                .ok()
                .and_then(|body| metric_value(&body, metric))
                .is_some_and(|value| value >= minimum)
        }
    })
    .await
    .map_err(|_| E2eError::Scenario(format!("metric {metric} never reached {minimum}")))
}

pub(super) async fn assert_consumer_reclaim(redis_url: &str) -> Result<(), E2eError> {
    let url = redis_url.to_owned();
    tokio::task::spawn_blocking(move || -> Result<(), E2eError> {
        let client = redis::Client::open(url)?;
        let mut connection = client.get_connection()?;
        let key = event_stream_key();
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("lunarbase-e2e-consumers")
            .arg("crashed-consumer")
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(&key)
            .arg(">")
            .query(&mut connection)?;
        let pending = redis::cmd("XPENDING")
            .arg(&key)
            .arg("lunarbase-e2e-consumers")
            .arg("-")
            .arg("+")
            .arg(10)
            .query::<Vec<(String, String, u64, u64)>>(&mut connection)?;
        let Some((stream_id, _, _, _)) = pending.first() else {
            return Err(E2eError::Scenario(
                "consumer crash did not leave a pending event".into(),
            ));
        };
        let _: redis::Value = redis::cmd("XAUTOCLAIM")
            .arg(&key)
            .arg("lunarbase-e2e-consumers")
            .arg("recovery-consumer")
            .arg(0)
            .arg("0-0")
            .arg("COUNT")
            .arg(1)
            .query(&mut connection)?;
        let reclaimed = redis::cmd("XPENDING")
            .arg(&key)
            .arg("lunarbase-e2e-consumers")
            .arg("-")
            .arg("+")
            .arg(10)
            .arg("recovery-consumer")
            .query::<Vec<(String, String, u64, u64)>>(&mut connection)?;
        if !reclaimed.iter().any(|(id, _, _, _)| id == stream_id) {
            return Err(E2eError::Scenario(
                "XAUTOCLAIM did not transfer the pending event".into(),
            ));
        }
        let acknowledged = redis::cmd("XACK")
            .arg(&key)
            .arg("lunarbase-e2e-consumers")
            .arg(stream_id)
            .query::<usize>(&mut connection)?;
        if acknowledged != 1 {
            return Err(E2eError::Scenario(
                "reclaimed consumer event was not acknowledged".into(),
            ));
        }
        Ok(())
    })
    .await
    .map_err(|error| E2eError::Scenario(error.to_string()))?
}

async fn stream_length(redis_url: &str) -> Result<usize, E2eError> {
    let url = redis_url.to_owned();
    tokio::task::spawn_blocking(move || -> Result<usize, E2eError> {
        let client = redis::Client::open(url)?;
        let mut connection = client.get_connection()?;
        Ok(redis::cmd("XLEN")
            .arg(event_stream_key())
            .query(&mut connection)?)
    })
    .await
    .map_err(|error| E2eError::Scenario(error.to_string()))?
}

fn event_stream_key() -> String {
    format!("lunarbase-e2e:event:v1:{{8453:{CORE}}}:stream")
}

fn metric_value(body: &str, metric: &str) -> Option<u64> {
    body.lines().find_map(|line| {
        let value = line.strip_prefix(metric)?.strip_prefix(' ')?;
        value.parse().ok()
    })
}

pub(super) async fn wait_for_redis(url: &str) -> Result<(), E2eError> {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        let url = url.to_owned();
        let ready = tokio::task::spawn_blocking(move || -> bool {
            let Ok(client) = redis::Client::open(url) else {
                return false;
            };
            let Ok(mut connection) = client.get_connection() else {
                return false;
            };
            redis::cmd("PING")
                .query::<String>(&mut connection)
                .is_ok_and(|response| response == "PONG")
        })
        .await
        .unwrap_or(false);
        if ready {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(E2eError::Scenario(format!(
        "Redis at {url} did not become ready"
    )))
}
