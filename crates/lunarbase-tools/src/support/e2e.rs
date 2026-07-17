//! Self-contained process-level E2E harness for the real indexer binary.

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use lunarbase_client_core::{RedisNamespace, TOPIC_LANE_ADDED, TOPIC_LANE_REMOVED};
use lunarbase_math::{encode_lane_slot0, Address, LaneSlot0, U256, WAD};
use serde_json::{json, Value};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const CORE: &str = "0x0000000000000000000000000000000000000010";
const CASH: &str = "0x0000000000000000000000000000000000000001";
const ASSET: &str = "0x0000000000000000000000000000000000000002";
const ROUTER: &str = "0x0000000000000000000000000000000000000003";
const EMPTY_CODE_HASH: &str = "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470";

/// CLI settings for the process-level validation.
#[derive(Clone, Debug, Parser)]
#[command(name = "lunarbase-e2e")]
pub struct E2eArguments {
    /// Previously built lunarbase-indexer executable.
    #[arg(long, default_value = "target/debug/lunarbase-indexer")]
    pub indexer_bin: PathBuf,
    /// Existing Redis URL. When omitted, a temporary redis-server is started.
    #[arg(long)]
    pub redis_url: Option<String>,
    /// Complete scenario deadline.
    #[arg(long, default_value_t = 60)]
    pub timeout_seconds: u64,
}

#[derive(Debug, Error)]
pub enum E2eError {
    #[error("I/O failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP failure: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Redis failure: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("scenario failed: {0}")]
    Scenario(String),
}

#[derive(Clone, Copy, Debug)]
enum MockEvent {
    Header(u64),
    Gap(u64),
}

struct MockState {
    block: AtomicU64,
    recovery_delay_milliseconds: AtomicU64,
    webhook_deliveries: AtomicUsize,
    websocket_connections: AtomicUsize,
    events: broadcast::Sender<MockEvent>,
    slot0: U256,
}

struct MockChain {
    state: Arc<MockState>,
    rpc_address: SocketAddr,
    websocket_address: SocketAddr,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl MockChain {
    async fn start() -> Result<Self, E2eError> {
        let (events, _) = broadcast::channel(32);
        let state = Arc::new(MockState {
            block: AtomicU64::new(100),
            recovery_delay_milliseconds: AtomicU64::new(0),
            webhook_deliveries: AtomicUsize::new(0),
            websocket_connections: AtomicUsize::new(0),
            events,
            slot0: encode_lane_slot0(&LaneSlot0 {
                price: WAD * U256::from(2),
                ask_fee_bps: U256::from(10_000),
                ..Default::default()
            })
            .map_err(|error| E2eError::Scenario(error.to_string()))?,
        });
        let rpc_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let rpc_address = rpc_listener.local_addr()?;
        let websocket_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let websocket_address = websocket_listener.local_addr()?;
        let (stop, _) = watch::channel(false);

        let rpc_state = state.clone();
        let mut rpc_stop = stop.subscribe();
        let rpc_task = tokio::spawn(async move {
            let app = Router::new()
                .route("/", post(rpc))
                .route("/webhook", post(webhook))
                .route("/health", get(|| async { "ok" }))
                .with_state(rpc_state);
            let _ = axum::serve(rpc_listener, app)
                .with_graceful_shutdown(async move {
                    stop_requested(&mut rpc_stop).await;
                })
                .await;
        });

        let websocket_state = state.clone();
        let mut websocket_stop = stop.subscribe();
        let websocket_task = tokio::spawn(async move {
            serve_websockets(websocket_listener, websocket_state, &mut websocket_stop).await;
        });

        Ok(Self {
            state,
            rpc_address,
            websocket_address,
            stop,
            tasks: vec![rpc_task, websocket_task],
        })
    }

    fn rpc_url(&self) -> String {
        format!("http://{}", self.rpc_address)
    }

    fn websocket_url(&self) -> String {
        format!("ws://{}", self.websocket_address)
    }

    fn webhook_url(&self) -> String {
        format!("http://{}/webhook", self.rpc_address)
    }

    fn publish(&self, event: MockEvent) {
        match event {
            MockEvent::Header(block) | MockEvent::Gap(block) => {
                self.state.block.store(block, Ordering::Relaxed);
            }
        }
        let _ = self.state.events.send(event);
    }

    async fn stop(mut self) {
        let _ = self.stop.send(true);
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
    }
}

impl Drop for MockChain {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        for task in &self.tasks {
            task.abort();
        }
    }
}

struct RedisProcess {
    url: String,
    child: Option<Child>,
}

impl RedisProcess {
    async fn start(configured: Option<String>) -> Result<Self, E2eError> {
        if let Some(url) = configured {
            wait_for_redis(&url).await?;
            return Ok(Self { url, child: None });
        }
        let port = free_port()?;
        let mut command = Command::new("redis-server");
        command
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            E2eError::Scenario(format!(
                "cannot start redis-server ({error}); pass --redis-url"
            ))
        })?;
        let url = format!("redis://127.0.0.1:{port}/");
        if let Err(error) = wait_for_redis(&url).await {
            let _ = child.kill().await;
            return Err(error);
        }
        Ok(Self {
            url,
            child: Some(child),
        })
    }

    async fn stop(mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }
}

struct Workspace {
    path: PathBuf,
}

impl Workspace {
    fn create() -> Result<Self, E2eError> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("lunarbase-e2e-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    fn config(&self, name: &str) -> PathBuf {
        self.path.join(format!("{name}.toml"))
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Runs bootstrap, realtime, recovery, lease takeover, webhook, checkpoint,
/// and SIGTERM scenarios against actual indexer OS processes.
pub async fn run(arguments: E2eArguments) -> Result<(), E2eError> {
    if !arguments.indexer_bin.is_file() {
        return Err(E2eError::Scenario(format!(
            "indexer binary `{}` does not exist; run `cargo build -p lunarbase-indexer`",
            arguments.indexer_bin.display()
        )));
    }
    timeout(
        Duration::from_secs(arguments.timeout_seconds),
        run_scenarios(arguments),
    )
    .await
    .map_err(|_| E2eError::Scenario("complete E2E deadline exceeded".into()))?
}

async fn run_scenarios(arguments: E2eArguments) -> Result<(), E2eError> {
    let mock = MockChain::start().await?;
    let redis = RedisProcess::start(arguments.redis_url).await?;
    let workspace = Workspace::create()?;
    let primary_port = free_port()?;
    let standby_port = free_port()?;
    let startup_port = free_port()?;
    let primary_config = workspace.config("primary");
    let standby_config = workspace.config("standby");
    let startup_config = workspace.config("startup");
    write_config(
        &primary_config,
        &mock,
        &redis.url,
        primary_port,
        "e2e-primary",
        true,
    )?;
    write_config(
        &standby_config,
        &mock,
        &redis.url,
        standby_port,
        "e2e-standby",
        true,
    )?;
    write_config(
        &startup_config,
        &mock,
        &redis.url,
        startup_port,
        "e2e-startup",
        false,
    )?;

    let mut primary = spawn_indexer(&arguments.indexer_bin, &primary_config)?;
    let primary_url = format!("http://127.0.0.1:{primary_port}");
    wait_for_role(&primary_url, "active", true).await?;
    assert_quote(&primary_url).await?;
    mock.publish(MockEvent::Header(101));
    wait_for_block(&primary_url, 101).await?;

    let mut standby = spawn_indexer(&arguments.indexer_bin, &standby_config)?;
    let standby_url = format!("http://127.0.0.1:{standby_port}");
    wait_for_role(&standby_url, "standby", false).await?;

    mock.state
        .recovery_delay_milliseconds
        .store(750, Ordering::Relaxed);
    mock.publish(MockEvent::Gap(103));
    wait_for_not_ready(&primary_url).await?;
    mock.state
        .recovery_delay_milliseconds
        .store(0, Ordering::Relaxed);
    mock.publish(MockEvent::Header(104));
    wait_for_role(&primary_url, "active", true).await?;
    assert_quote(&primary_url).await?;
    wait_until(Duration::from_secs(5), || async {
        mock.state.webhook_deliveries.load(Ordering::Relaxed) > 0
    })
    .await
    .map_err(|_| E2eError::Scenario("gap did not produce a webhook alert".into()))?;

    terminate(&mut primary)
        .await
        .map_err(|error| E2eError::Scenario(format!("primary shutdown failed: {error}")))?;
    wait_for_role(&standby_url, "active", true).await?;
    assert_quote(&standby_url).await?;
    terminate(&mut standby).await.map_err(|error| {
        E2eError::Scenario(format!("standby takeover shutdown failed: {error}"))
    })?;
    assert_checkpoint(&redis.url).await?;

    mock.state
        .recovery_delay_milliseconds
        .store(5_000, Ordering::Relaxed);
    let mut startup = spawn_indexer(&arguments.indexer_bin, &startup_config)?;
    sleep(Duration::from_millis(150)).await;
    terminate(&mut startup)
        .await
        .map_err(|error| E2eError::Scenario(format!("startup shutdown failed: {error}")))?;
    mock.state
        .recovery_delay_milliseconds
        .store(0, Ordering::Relaxed);

    wait_until(Duration::from_secs(3), || async {
        mock.state.websocket_connections.load(Ordering::Relaxed) == 0
    })
    .await
    .map_err(|_| E2eError::Scenario("indexer left a detached WebSocket task".into()))?;

    println!(
        "{}",
        json!({
            "status": "ok",
            "bootstrapQuote": true,
            "realtimeBlock": 101,
            "gapReturned503": true,
            "recoveredBlock": 104,
            "standbyTakeover": true,
            "sigtermDuringSnapshot": true,
            "finalCheckpoint": true,
            "webhookDeliveries": mock.state.webhook_deliveries.load(Ordering::Relaxed),
            "detachedWebsocketTasks": 0,
        })
    );
    mock.stop().await;
    redis.stop().await;
    Ok(())
}

async fn rpc(State(state): State<Arc<MockState>>, Json(request): Json<Value>) -> Json<Value> {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let delay = state.recovery_delay_milliseconds.load(Ordering::Relaxed);
    if delay > 0 && matches!(method, "eth_getBlockByNumber" | "eth_getLogs") {
        sleep(Duration::from_millis(delay)).await;
    }
    let block = state.block.load(Ordering::Relaxed);
    let result = match method {
        "eth_getBlockByNumber" => json!({
            "number": format!("0x{block:x}"),
            "hash": block_hash(block),
        }),
        "eth_getCode" => json!("0x"),
        "eth_getLogs" => discovery_logs(&request, block),
        "eth_call" => {
            let data = request
                .pointer("/params/0/data")
                .and_then(Value::as_str)
                .unwrap_or_default();
            json!(eth_call_result(data, state.slot0))
        }
        "eth_chainId" => json!("0x2105"),
        _ => Value::Null,
    };
    Json(json!({"jsonrpc": "2.0", "id": id, "result": result}))
}

async fn webhook(State(state): State<Arc<MockState>>, Json(_payload): Json<Value>) {
    state.webhook_deliveries.fetch_add(1, Ordering::Relaxed);
}

fn discovery_logs(request: &Value, block: u64) -> Value {
    let requested_topic = request
        .pointer("/params/0/topics/0")
        .and_then(Value::as_str);
    let added = word_hex(TOPIC_LANE_ADDED);
    let removed = word_hex(TOPIC_LANE_REMOVED);
    if requested_topic == Some(removed.as_str()) || requested_topic.is_none() {
        return json!([]);
    }
    if requested_topic != Some(added.as_str()) {
        return json!([]);
    }
    json!([{
        "address": CORE,
        "topics": [added, address_word(ASSET)],
        "data": "0x",
        "removed": false,
        "blockNumber": "0x1",
        "blockHash": block_hash(1),
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "transactionHash": block_hash(block),
    }])
}

fn eth_call_result(data: &str, slot0: U256) -> String {
    match data.get(..10).unwrap_or_default() {
        "0x961be391" => address_word(CASH),
        "0x93b6ab27" => words(&[U256::ONE]),
        "0xd1bacd10" => words(&[slot0, U256::ONE, U256::ZERO, U256::ZERO, U256::ZERO]),
        "0xd66bd524" => words(&[
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
            U256::from(1_000_000),
        ]),
        "0x9b19251a" => words(&[U256::ONE]),
        "0xaa5f434c" => words(&[U256::ZERO, U256::ZERO]),
        _ => words(&[U256::ZERO]),
    }
}

async fn serve_websockets(
    listener: TcpListener,
    state: Arc<MockState>,
    stop: &mut watch::Receiver<bool>,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = stop_requested(stop) => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { break };
                let connection_state = state.clone();
                connections.spawn(async move {
                    websocket_connection(stream, connection_state).await;
                });
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
}

async fn websocket_connection(stream: TcpStream, state: Arc<MockState>) {
    state.websocket_connections.fetch_add(1, Ordering::Relaxed);
    let _ = websocket_connection_inner(stream, &state).await;
    state.websocket_connections.fetch_sub(1, Ordering::Relaxed);
}

async fn websocket_connection_inner(
    stream: TcpStream,
    state: &Arc<MockState>,
) -> Result<(), E2eError> {
    let socket = accept_async(stream)
        .await
        .map_err(|error| E2eError::Scenario(error.to_string()))?;
    let (mut writer, mut reader) = socket.split();
    for _ in 0..2 {
        let Some(Ok(message)) = reader.next().await else {
            return Ok(());
        };
        let text = message
            .to_text()
            .map_err(|error| E2eError::Scenario(error.to_string()))?;
        let request: Value =
            serde_json::from_str(text).map_err(|error| E2eError::Scenario(error.to_string()))?;
        let id = request.get("id").and_then(Value::as_u64).unwrap_or(0);
        let subscription = if id == 1 { "pending" } else { "flashblocks" };
        writer
            .send(Message::Text(
                json!({"jsonrpc":"2.0","id":id,"result":subscription}).to_string(),
            ))
            .await
            .map_err(|error| E2eError::Scenario(error.to_string()))?;
    }
    let mut events = state.events.subscribe();
    loop {
        tokio::select! {
            incoming = reader.next() => {
                match incoming {
                    Some(Ok(Message::Ping(bytes))) => {
                        if writer.send(Message::Pong(bytes)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_) | Message::Text(_) | Message::Binary(_))) => {}
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                }
            }
            event = events.recv() => {
                let Ok(event) = event else { break };
                let block = match event {
                    MockEvent::Header(block) | MockEvent::Gap(block) => block,
                };
                let notification = json!({
                    "jsonrpc": "2.0",
                    "method": "eth_subscription",
                    "params": {
                        "subscription": "flashblocks",
                        "result": {
                            "payload_id": format!("0x{block:02x}"),
                            "index": 0,
                            "base": {"block_number": format!("0x{block:x}")},
                            "diff": {"block_hash": block_hash(block)}
                        }
                    }
                });
                if writer
                    .send(Message::Text(notification.to_string()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }
    Ok(())
}

fn write_config(
    path: &Path,
    mock: &MockChain,
    redis_url: &str,
    port: u16,
    owner: &str,
    lease_enabled: bool,
) -> Result<(), E2eError> {
    let redis_enabled = lease_enabled;
    let contents = format!(
        r#"network = "base"
core = "{CORE}"
deployment_block = 0
expected_runtime_code_hash = "{EMPTY_CODE_HASH}"
http_rpc_url = "{}"
realtime_url = "{}"
snapshot_tag = "finalized"
bind = "127.0.0.1:{port}"
explicit_lane_assets = ["{ASSET}"]
eager_routers = ["{ROUTER}"]

[runtime]
buffer_capacity = 128
reconnect_delay_milliseconds = 100

[redis]
enabled = {redis_enabled}
url = "{redis_url}"
io_timeout_milliseconds = 200
stream_max_len = 1000
dedup_ttl_seconds = 60
checkpoint_interval_updates = 1

[writer_lease]
enabled = {lease_enabled}
owner = "{owner}"
ttl_milliseconds = 2000
renew_interval_milliseconds = 500
retry_interval_milliseconds = 100

[transport]
max_frame_bytes = 262144
reorder_capacity = 128
require_evm_parent_context = true

[shutdown]
timeout_seconds = 4

[alerts]
enabled = true
webhook_url = "{}"
poll_interval_seconds = 1
not_ready_after_seconds = 1
repeat_interval_seconds = 1
request_timeout_seconds = 1
"#,
        mock.rpc_url(),
        mock.websocket_url(),
        mock.webhook_url(),
    );
    std::fs::write(path, contents)?;
    Ok(())
}

fn spawn_indexer(binary: &Path, config: &Path) -> Result<Child, E2eError> {
    let mut command = Command::new(binary);
    command
        .arg("--config")
        .arg(config)
        .env("RUST_LOG", "lunarbase_indexer=warn")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    Ok(command.spawn()?)
}

async fn terminate(child: &mut Child) -> Result<(), E2eError> {
    let Some(pid) = child.id() else {
        return Ok(());
    };
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .await?;
    if !status.success() {
        return Err(E2eError::Scenario(format!(
            "failed to send SIGTERM to process {pid}"
        )));
    }
    let exit = timeout(Duration::from_secs(8), child.wait())
        .await
        .map_err(|_| E2eError::Scenario(format!("process {pid} ignored SIGTERM")))??;
    if !exit.success() {
        return Err(E2eError::Scenario(format!(
            "process {pid} exited unsuccessfully: {exit}"
        )));
    }
    Ok(())
}

async fn wait_for_role(url: &str, role: &str, ready: bool) -> Result<(), E2eError> {
    let client = reqwest::Client::new();
    let endpoint = format!("{url}/health/ready");
    let started = Instant::now();
    let mut last_observation = "no HTTP response".to_owned();
    while started.elapsed() < Duration::from_secs(12) {
        match client.get(&endpoint).send().await {
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                last_observation = format!("status={status}, body={body}");
                let actual_role = serde_json::from_str::<Value>(&body)
                    .ok()
                    .and_then(|body| body.get("role").and_then(Value::as_str).map(str::to_owned));
                if status.is_success() == ready && actual_role.as_deref() == Some(role) {
                    return Ok(());
                }
            }
            Err(error) => last_observation = error.to_string(),
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(E2eError::Scenario(format!(
        "did not observe role `{role}` at {url}; last observation: {last_observation}"
    )))
}

async fn wait_for_not_ready(url: &str) -> Result<(), E2eError> {
    let client = reqwest::Client::new();
    wait_until(Duration::from_secs(3), || {
        let client = client.clone();
        let endpoint = format!("{url}/health/ready");
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

async fn wait_for_block(url: &str, expected: u64) -> Result<(), E2eError> {
    let client = reqwest::Client::new();
    wait_until(Duration::from_secs(5), || {
        let client = client.clone();
        let endpoint = format!("{url}/health/ready");
        async move {
            let Ok(response) = client.get(endpoint).send().await else {
                return false;
            };
            response
                .json::<Value>()
                .await
                .ok()
                .and_then(|body| {
                    body.pointer("/cursor/blockNumber")?
                        .as_str()?
                        .parse::<u64>()
                        .ok()
                })
                .is_some_and(|block| block >= expected)
        }
    })
    .await
    .map_err(|_| E2eError::Scenario(format!("indexer did not reach block {expected}")))
}

async fn assert_quote(url: &str) -> Result<(), E2eError> {
    let response = reqwest::Client::new()
        .post(format!("{url}/v1/quote"))
        .json(&json!({
            "router": ROUTER,
            "assetIn": CASH,
            "assetOut": ASSET,
            "amount": "100",
            "mode": "exactIn",
            "executionBlockNumber": "104",
            "minimumCommitment": "realtime",
            "maxAgeBlocks": 10,
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
    if body.pointer("/outcome/amountOut").is_none()
        || body.pointer("/outcome/status").and_then(Value::as_str) != Some("available")
    {
        return Err(E2eError::Scenario(format!(
            "quote did not contain an available amount: {body}"
        )));
    }
    Ok(())
}

async fn assert_checkpoint(redis_url: &str) -> Result<(), E2eError> {
    let url = redis_url.to_owned();
    let key = RedisNamespace::new(
        8453,
        Address::from_hex(CORE).map_err(|error| E2eError::Scenario(error.to_string()))?,
    )
    .checkpoint;
    let exists = tokio::task::spawn_blocking(move || -> Result<bool, redis::RedisError> {
        use redis::Commands;
        let client = redis::Client::open(url)?;
        let mut connection = client.get_connection()?;
        connection.exists(key)
    })
    .await
    .map_err(|error| E2eError::Scenario(error.to_string()))??;
    if !exists {
        return Err(E2eError::Scenario(
            "final Redis checkpoint key is absent".into(),
        ));
    }
    Ok(())
}

async fn wait_for_redis(url: &str) -> Result<(), E2eError> {
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

async fn wait_until<F, Fut>(deadline: Duration, mut predicate: F) -> Result<(), ()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    while started.elapsed() < deadline {
        if predicate().await {
            return Ok(());
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(())
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

fn free_port() -> Result<u16, E2eError> {
    let listener =
        std::net::TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))?;
    Ok(listener.local_addr()?.port())
}

fn address_word(address: &str) -> String {
    format!("0x{}{}", "0".repeat(24), address.trim_start_matches("0x"))
}

fn words(values: &[U256]) -> String {
    let mut output = String::from("0x");
    for value in values {
        output.push_str(&format!("{value:064x}"));
    }
    output
}

fn word_hex(value: U256) -> String {
    format!("0x{value:064x}")
}

fn block_hash(block: u64) -> String {
    format!("0x{block:064x}")
}
