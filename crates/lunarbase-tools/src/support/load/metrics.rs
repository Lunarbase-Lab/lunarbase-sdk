use super::LoadError;
use serde_json::Value;
use std::time::{Duration, Instant};
use tokio::time::sleep;

pub(super) async fn wait_for_ready(
    client: &reqwest::Client,
    indexer_url: &str,
    deadline: Duration,
) -> Result<(), LoadError> {
    let endpoint = format!("{}/readyz", indexer_url.trim_end_matches('/'));
    let started = Instant::now();
    let mut last_observation = "no HTTP response".to_owned();
    while started.elapsed() < deadline {
        match client.get(&endpoint).send().await {
            Ok(response) => {
                let status = response.status();
                let body = response.bytes().await.unwrap_or_default();
                let ready = serde_json::from_slice::<Value>(&body)
                    .ok()
                    .and_then(|value| value.get("ready").and_then(Value::as_bool));
                last_observation =
                    format!("status={status}, body={}", String::from_utf8_lossy(&body));
                if status.is_success() && ready == Some(true) {
                    return Ok(());
                }
            }
            Err(error) => last_observation = error.to_string(),
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(LoadError::Invalid(format!(
        "indexer did not become ready; last observation: {last_observation}"
    )))
}

pub(super) async fn fetch_metrics(
    client: &reqwest::Client,
    indexer_url: &str,
) -> Result<String, LoadError> {
    let url = format!("{}/metrics", indexer_url.trim_end_matches('/'));
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?)
}

pub(super) fn metric_delta(before: &str, after: &str, name: &str) -> Result<f64, LoadError> {
    Ok(metric_value(after, name)? - metric_value(before, name)?)
}

fn metric_value(metrics: &str, name: &str) -> Result<f64, LoadError> {
    metrics
        .lines()
        .find_map(|line| {
            let (metric, value) = line.split_once(' ')?;
            (metric == name).then_some(value)
        })
        .ok_or_else(|| LoadError::Invalid(format!("metrics response has no `{name}` sample")))?
        .parse::<f64>()
        .map_err(|error| LoadError::Invalid(format!("invalid `{name}` metric value: {error}")))
}

#[derive(Clone, Copy)]
pub(super) struct ProcessMemory {
    pub(super) rss_bytes: u64,
    pub(super) peak_rss_bytes: u64,
}

pub(super) fn process_memory(pid: Option<u32>) -> Result<Option<ProcessMemory>, LoadError> {
    let Some(pid) = pid else {
        return Ok(None);
    };
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
    Ok(Some(ProcessMemory {
        rss_bytes: status_bytes(&status, "VmRSS:")
            .ok_or_else(|| LoadError::Invalid("process status has no VmRSS".into()))?,
        peak_rss_bytes: status_bytes(&status, "VmHWM:")
            .ok_or_else(|| LoadError::Invalid("process status has no VmHWM".into()))?,
    }))
}

fn status_bytes(status: &str, key: &str) -> Option<u64> {
    let line = status.lines().find(|line| line.starts_with(key))?;
    let kibibytes = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(kibibytes.saturating_mul(1024))
}

pub(super) fn memory_delta(
    before: Option<ProcessMemory>,
    after: Option<ProcessMemory>,
    select: impl Fn(ProcessMemory) -> u64,
) -> Option<i64> {
    let before = i64::try_from(select(before?)).ok()?;
    let after = i64::try_from(select(after?)).ok()?;
    Some(after.saturating_sub(before))
}
