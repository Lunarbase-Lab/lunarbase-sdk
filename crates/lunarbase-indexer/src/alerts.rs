//! Structured operational alerts and optional webhook delivery.

use crate::config::{ValidatedAlertsConfig, ValidatedConfig};
use crate::metrics::ServiceMetrics;
use crate::runtime::{RuntimeHandle, RuntimeRole, ServiceRuntimeEvent};
use lunarbase_client_core::{ClientRuntimeEvent, Commitment, Network};
use serde::Serialize;
use std::collections::HashMap;
use std::panic;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{interval, timeout, MissedTickBehavior};
use tracing::{error, info, warn};

/// Severity attached to log and webhook alert records.
#[derive(Clone, Copy, Debug)]
pub enum AlertSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

impl AlertSeverity {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
            Self::Critical => "critical",
        }
    }
}

#[derive(Clone, Debug)]
struct AlertIdentity {
    network: &'static str,
    chain_id: u64,
    core: String,
}

/// One panic captured by the process-wide panic hook.
#[derive(Clone, Debug)]
pub struct ProcessPanic {
    message: String,
    location: Option<String>,
}

/// Shared alert dispatcher with bounded log and webhook repetition frequency.
#[derive(Clone)]
pub struct AlertSink {
    config: ValidatedAlertsConfig,
    identity: AlertIdentity,
    http: reqwest::Client,
    last_alert: Arc<Mutex<HashMap<String, Instant>>>,
    metrics: ServiceMetrics,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WebhookPayload<'a> {
    text: String,
    service: &'static str,
    severity: &'static str,
    code: &'a str,
    message: &'a str,
    network: &'static str,
    chain_id: String,
    core: &'a str,
    unix_timestamp_seconds: String,
}

impl AlertSink {
    /// Builds an alert dispatcher for one configured deployment.
    pub fn new(config: &ValidatedConfig, metrics: ServiceMetrics) -> Self {
        Self {
            config: config.alerts.clone(),
            identity: AlertIdentity {
                network: network_name(config.deployment.network),
                chain_id: config.deployment.chain_id,
                core: config.deployment.core.to_hex(),
            },
            http: reqwest::Client::new(),
            last_alert: Arc::new(Mutex::new(HashMap::new())),
            metrics,
        }
    }

    /// Writes a structured log and, when configured, posts a deduplicated
    /// generic JSON webhook.
    pub async fn emit(&self, severity: AlertSeverity, code: &str, message: &str) {
        if !self.reserve_alert(code) {
            return;
        }
        match severity {
            AlertSeverity::Info => info!(
                alert = true,
                severity = severity.as_str(),
                code,
                network = self.identity.network,
                chain_id = self.identity.chain_id,
                core = self.identity.core,
                message,
                "LunarBase operational alert"
            ),
            AlertSeverity::Warning => warn!(
                alert = true,
                severity = severity.as_str(),
                code,
                network = self.identity.network,
                chain_id = self.identity.chain_id,
                core = self.identity.core,
                message,
                "LunarBase operational alert"
            ),
            AlertSeverity::Error | AlertSeverity::Critical => error!(
                alert = true,
                severity = severity.as_str(),
                code,
                network = self.identity.network,
                chain_id = self.identity.chain_id,
                core = self.identity.core,
                message,
                "LunarBase operational alert"
            ),
        }

        let Some(webhook_url) = self
            .config
            .enabled
            .then_some(self.config.webhook_url.as_deref())
            .flatten()
        else {
            return;
        };

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .to_string();
        let payload = WebhookPayload {
            text: format!(
                "[{}] lunarbase-indexer {}: {}",
                severity.as_str(),
                code,
                message
            ),
            service: "lunarbase-indexer",
            severity: severity.as_str(),
            code,
            message,
            network: self.identity.network,
            chain_id: self.identity.chain_id.to_string(),
            core: &self.identity.core,
            unix_timestamp_seconds: timestamp,
        };
        let delivery = timeout(
            self.config.request_timeout,
            self.http.post(webhook_url).json(&payload).send(),
        )
        .await;
        let failed = match delivery {
            Ok(Ok(response)) if response.status().is_success() => false,
            Ok(Ok(response)) => {
                error!(
                    alert = true,
                    code = "alert_delivery_failed",
                    status = %response.status(),
                    original_code = code,
                    "alert webhook returned a non-success status"
                );
                true
            }
            Ok(Err(delivery_error)) => {
                error!(
                    alert = true,
                    code = "alert_delivery_failed",
                    error = %delivery_error,
                    original_code = code,
                    "alert webhook request failed"
                );
                true
            }
            Err(_) => {
                error!(
                    alert = true,
                    code = "alert_delivery_timed_out",
                    original_code = code,
                    "alert webhook request exceeded its deadline"
                );
                true
            }
        };
        if failed {
            self.metrics.record_alert_failure();
            self.release_alert(code);
        }
    }

    fn reserve_alert(&self, code: &str) -> bool {
        let Ok(mut sent) = self.last_alert.lock() else {
            error!(
                alert = true,
                code = "alert_dedup_lock_poisoned",
                "alert webhook deduplication lock was poisoned"
            );
            return true;
        };
        let now = Instant::now();
        if sent
            .get(code)
            .is_some_and(|last| now.duration_since(*last) < self.config.repeat_interval)
        {
            return false;
        }
        sent.insert(code.to_owned(), now);
        true
    }

    fn release_alert(&self, code: &str) {
        if let Ok(mut sent) = self.last_alert.lock() {
            sent.remove(code);
        }
    }
}

/// Installs a panic hook that preserves the previous hook while forwarding
/// panic metadata to the asynchronous alert supervisor.
pub fn install_panic_hook(sender: mpsc::UnboundedSender<ProcessPanic>) {
    let previous = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        let message = panic_info
            .payload()
            .downcast_ref::<&str>()
            .map(|message| (*message).to_owned())
            .or_else(|| panic_info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "non-string panic payload".into());
        let location = panic_info.location().map(|location| {
            format!(
                "{}:{}:{}",
                location.file(),
                location.line(),
                location.column()
            )
        });
        error!(
            alert = true,
            severity = "critical",
            code = "process_panic",
            panic_message = message,
            panic_location = location.as_deref().unwrap_or("unknown"),
            "panic captured by lunarbase-indexer"
        );
        let _ = sender.send(ProcessPanic { message, location });
        previous(panic_info);
    }));
}

/// Starts readiness monitoring and consumes exact runtime/panic failure events.
pub fn spawn_supervisor(
    sink: AlertSink,
    runtime: RuntimeHandle,
    mut runtime_events: broadcast::Receiver<ServiceRuntimeEvent>,
    mut panics: mpsc::UnboundedReceiver<ProcessPanic>,
    mut stop: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(sink.config.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut not_ready_since = None;
        let mut outage_alerted = false;
        let mut events_open = true;
        let mut panics_open = true;

        loop {
            tokio::select! {
                biased;
                () = stop_requested(&mut stop) => break,
                event = runtime_events.recv(), if events_open => {
                    match event {
                        Ok(event) => {
                            sink.emit(event_severity(&event), event.code(), &event.detail()).await;
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            sink.emit(
                                AlertSeverity::Critical,
                                "runtime_alerts_lagged",
                                &format!("runtime alert consumer dropped {skipped} events"),
                            ).await;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            events_open = false;
                            sink.emit(
                                AlertSeverity::Critical,
                                "runtime_alert_channel_closed",
                                "runtime alert channel closed while the service was running",
                            ).await;
                        }
                    }
                }
                panic = panics.recv(), if panics_open => {
                    match panic {
                        Some(panic) => {
                            let detail = panic.location.map_or(panic.message.clone(), |location| {
                                format!("{} at {location}", panic.message)
                            });
                            sink.emit(AlertSeverity::Critical, "process_panic", &detail).await;
                        }
                        None => panics_open = false,
                    }
                }
                _ = ticker.tick(), if sink.config.enabled => {
                    let status = runtime.status().await;
                    if status.role != RuntimeRole::Active {
                        not_ready_since = None;
                        outage_alerted = false;
                        continue;
                    }
                    let Some(client) = runtime.client().await else {
                        continue;
                    };
                    if !client.is_ready() {
                        let since = not_ready_since.get_or_insert_with(Instant::now);
                        if since.elapsed() >= sink.config.not_ready_after {
                            sink.emit(
                                AlertSeverity::Error,
                                "indexer_not_ready",
                                &format!(
                                    "indexer has been not ready for {} seconds",
                                    since.elapsed().as_secs()
                                ),
                            ).await;
                            outage_alerted = true;
                        }
                        continue;
                    }
                    let health = client.health().await;
                    if health.ready {
                        not_ready_since = None;
                        if outage_alerted {
                            sink.emit(
                                AlertSeverity::Info,
                                "indexer_recovered",
                                &format!(
                                    "indexer recovered at commitment {} and block {}",
                                    commitment_name(health.commitment),
                                    health
                                        .cursor
                                        .as_ref()
                                        .map_or_else(|| "unknown".into(), |cursor| cursor.block_number.to_string()),
                                ),
                            ).await;
                            outage_alerted = false;
                        }
                    } else {
                        let since = not_ready_since.get_or_insert_with(Instant::now);
                        if since.elapsed() >= sink.config.not_ready_after {
                            sink.emit(
                                AlertSeverity::Error,
                                "indexer_not_ready",
                                &format!(
                                    "indexer has been not ready for {} seconds",
                                    since.elapsed().as_secs()
                                ),
                            ).await;
                            outage_alerted = true;
                        }
                    }
                }
            }
        }
    })
}

fn event_severity(event: &ServiceRuntimeEvent) -> AlertSeverity {
    match event {
        ServiceRuntimeEvent::Client(event) => client_event_severity(event),
        ServiceRuntimeEvent::LeaseAcquired => AlertSeverity::Info,
        ServiceRuntimeEvent::LeaseAcquireFailed { .. }
        | ServiceRuntimeEvent::RuntimeConnectFailed { .. } => AlertSeverity::Warning,
        ServiceRuntimeEvent::LeaseRenewFailed { .. }
        | ServiceRuntimeEvent::LeaseLost
        | ServiceRuntimeEvent::LeaseReleaseFailed { .. }
        | ServiceRuntimeEvent::RuntimeEventsLagged { .. } => AlertSeverity::Critical,
    }
}

fn client_event_severity(event: &ClientRuntimeEvent) -> AlertSeverity {
    match event {
        ClientRuntimeEvent::SourceSubscribeFailed { .. }
        | ClientRuntimeEvent::SourceStreamFailed { .. }
        | ClientRuntimeEvent::SourceStreamClosed => AlertSeverity::Warning,
        ClientRuntimeEvent::StateTransitionFailed { .. }
        | ClientRuntimeEvent::RecoveryFailed { .. }
        | ClientRuntimeEvent::CheckpointFailed { .. } => AlertSeverity::Error,
        ClientRuntimeEvent::BackgroundTaskStopped { .. }
        | ClientRuntimeEvent::ShutdownTimedOut
        | ClientRuntimeEvent::BackgroundTaskPanicked { .. } => AlertSeverity::Critical,
    }
}

const fn network_name(network: Network) -> &'static str {
    match network {
        Network::Base => "base",
        Network::Monad => "monad",
        Network::Arbitrum => "arbitrum",
    }
}

const fn commitment_name(commitment: Commitment) -> &'static str {
    match commitment {
        Commitment::Realtime => "realtime",
        Commitment::Canonical => "canonical",
        Commitment::Finalized => "finalized",
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::{Json, Router};
    use serde_json::Value;
    use std::time::Duration;

    async fn receive_webhook(
        State(sender): State<mpsc::UnboundedSender<Value>>,
        Json(payload): Json<Value>,
    ) -> StatusCode {
        let _ = sender.send(payload);
        StatusCode::NO_CONTENT
    }

    #[tokio::test]
    async fn webhook_alerts_are_deduplicated_by_code() {
        let (payload_sender, mut payloads) = mpsc::unbounded_channel();
        let router = Router::new()
            .route("/", post(receive_webhook))
            .with_state(payload_sender);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let sink = AlertSink {
            config: ValidatedAlertsConfig {
                enabled: true,
                webhook_url: Some(format!("http://{address}/")),
                poll_interval: Duration::from_secs(1),
                not_ready_after: Duration::from_secs(1),
                repeat_interval: Duration::from_secs(60),
                request_timeout: Duration::from_secs(1),
            },
            identity: AlertIdentity {
                network: "base",
                chain_id: 8453,
                core: "0x0000000000000000000000000000000000000001".into(),
            },
            http: reqwest::Client::new(),
            last_alert: Arc::new(Mutex::new(HashMap::new())),
            metrics: ServiceMetrics::default(),
        };

        sink.emit(AlertSeverity::Error, "same_code", "first").await;
        sink.emit(AlertSeverity::Error, "same_code", "second").await;

        let payload = timeout(Duration::from_secs(1), payloads.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(payload["code"], "same_code");
        assert!(timeout(Duration::from_millis(50), payloads.recv())
            .await
            .is_err());
        server.abort();
        let _ = (&mut server).await;
    }
}
