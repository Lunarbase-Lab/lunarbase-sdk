use super::LoadError;
use bytes::Bytes;
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

#[derive(Default)]
struct WorkerResult {
    latencies: Vec<f64>,
    successful: usize,
    first_error: Option<String>,
}

pub(super) struct PhaseResult {
    pub(super) latencies: Vec<f64>,
    pub(super) successful: usize,
    pub(super) first_error: Option<String>,
    pub(super) elapsed: Duration,
}

impl PhaseResult {
    pub(super) fn ensure_success(&self, phase: &str) -> Result<(), LoadError> {
        if self.successful == self.latencies.len() {
            return Ok(());
        }
        Err(LoadError::Invalid(format!(
            "{phase} failed for {} of {} HTTP requests; first error: {}",
            self.latencies.len().saturating_sub(self.successful),
            self.latencies.len(),
            self.first_error.as_deref().unwrap_or("unknown")
        )))
    }
}

pub(super) async fn run_phase(
    client: reqwest::Client,
    endpoint: Arc<String>,
    bodies: Arc<Vec<Bytes>>,
    batch_size: usize,
    request_count: usize,
    concurrency: usize,
) -> Result<PhaseResult, LoadError> {
    let next = Arc::new(AtomicUsize::new(0));
    let ready = Arc::new(Barrier::new(concurrency + 1));
    let start = Arc::new(Barrier::new(concurrency + 1));
    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = client.clone();
        let endpoint = endpoint.clone();
        let bodies = bodies.clone();
        let next = next.clone();
        let ready = ready.clone();
        let start = start.clone();
        workers.push(tokio::spawn(async move {
            ready.wait().await;
            start.wait().await;
            let mut result = WorkerResult::default();
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= request_count {
                    break;
                }
                let started = Instant::now();
                let outcome = send_quote(
                    &client,
                    &endpoint,
                    bodies[index % bodies.len()].clone(),
                    batch_size,
                )
                .await;
                result
                    .latencies
                    .push(started.elapsed().as_secs_f64() * 1_000.0);
                match outcome {
                    Ok(()) => result.successful += 1,
                    Err(error) if result.first_error.is_none() => result.first_error = Some(error),
                    Err(_) => {}
                }
            }
            result
        }));
    }
    ready.wait().await;
    let started = Instant::now();
    start.wait().await;
    let mut phase = PhaseResult {
        latencies: Vec::with_capacity(request_count),
        successful: 0,
        first_error: None,
        elapsed: Duration::ZERO,
    };
    for worker in workers {
        let mut result = worker
            .await
            .map_err(|error| LoadError::Invalid(format!("load worker failed: {error}")))?;
        phase.successful += result.successful;
        phase.latencies.append(&mut result.latencies);
        if phase.first_error.is_none() {
            phase.first_error = result.first_error;
        }
    }
    phase.elapsed = started.elapsed();
    phase.latencies.sort_by(f64::total_cmp);
    Ok(phase)
}

async fn send_quote(
    client: &reqwest::Client,
    endpoint: &str,
    body: Bytes,
    batch_size: usize,
) -> Result<(), String> {
    let response = client
        .post(endpoint)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status = response.status();
    let body = response.bytes().await.map_err(|error| error.to_string())?;
    if !status.is_success() {
        return Err(format!(
            "HTTP {status}: {}",
            String::from_utf8_lossy(&body[..body.len().min(256)])
        ));
    }
    let value: Value = serde_json::from_slice(&body).map_err(|error| error.to_string())?;
    validate_available_response(&value, batch_size)
}

fn validate_available_response(value: &Value, batch_size: usize) -> Result<(), String> {
    if batch_size == 1 {
        return (value.pointer("/result/status").and_then(Value::as_str) == Some("available"))
            .then_some(())
            .ok_or_else(|| "single response is not available".into());
    }
    let results = value
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| "batch response has no results array".to_owned())?;
    if results.len() != batch_size {
        return Err(format!(
            "batch response contains {} results instead of {batch_size}",
            results.len()
        ));
    }
    if results
        .iter()
        .any(|result| result.get("status").and_then(Value::as_str) != Some("available"))
    {
        return Err("batch response contains an unavailable quote".into());
    }
    Ok(())
}

pub(super) fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    let Some(last) = sorted.len().checked_sub(1) else {
        return 0.0;
    };
    sorted[((last as f64) * quantile).round() as usize]
}

#[cfg(test)]
mod tests {
    use super::validate_available_response;
    use serde_json::json;

    #[test]
    fn response_validation_checks_every_batch_outcome() {
        assert!(
            validate_available_response(&json!({"result": {"status": "available"}}), 1).is_ok()
        );
        assert!(
            validate_available_response(
                &json!({"results": [
                    {"status": "available"}, {"status": "unavailable"}
                ]}),
                2
            )
            .is_err()
        );
    }
}
