//! Managed external processes and local network fixtures for indexer E2E tests.

use crate::support::e2e::assertions::wait_for_redis;
use crate::support::e2e::helpers::{free_port, stop_requested};
use crate::support::e2e::rpc_mock::rpc;
use crate::support::e2e::websocket_mock::serve_websockets;
use axum::Router;
use axum::routing::post;
use clap::Parser;
use lunarbase_math::U256;
use lunarbase_math::arithmetic::WAD;
use lunarbase_math::slot0::{LaneSlot0, encode_lane_slot0};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

/// CLI settings for the process-level validation.
#[derive(Clone, Debug, Parser)]
#[command(name = "lunarbase-e2e")]
pub struct E2eArguments {
    /// Path to the built `lunarbase-indexer` executable.
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
/// Infrastructure or assertion failure in a process-level scenario.
pub enum E2eError {
    /// Local file, socket, or child-process operation failed.
    #[error("I/O failure: {0}")]
    Io(#[from] std::io::Error),
    /// HTTP request to a mock or indexer endpoint failed.
    #[error("HTTP failure: {0}")]
    Http(#[from] reqwest::Error),
    /// Temporary or externally configured Redis operation failed.
    #[error("Redis failure: {0}")]
    Redis(#[from] redis::RedisError),
    /// Scenario invariant was not satisfied before its deadline.
    #[error("scenario failed: {0}")]
    Scenario(String),
}

#[derive(Clone, Copy, Debug)]
pub(super) enum MockEvent {
    Header(u64),
    Gap(u64),
}

pub(super) struct MockState {
    pub(super) block: AtomicU64,
    pub(super) recovery_delay_milliseconds: AtomicU64,
    pub(super) websocket_connections: AtomicUsize,
    pub(super) events: broadcast::Sender<MockEvent>,
    pub(super) slot0: U256,
}

pub(super) struct MockChain {
    pub(super) state: Arc<MockState>,
    rpc_address: SocketAddr,
    websocket_address: SocketAddr,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl MockChain {
    pub(super) async fn start() -> Result<Self, E2eError> {
        let (events, _) = broadcast::channel(32);
        let state = Arc::new(MockState {
            block: AtomicU64::new(100),
            recovery_delay_milliseconds: AtomicU64::new(0),
            websocket_connections: AtomicUsize::new(0),
            events,
            slot0: encode_lane_slot0(&LaneSlot0 {
                price: u128::try_from(WAD * U256::from(2)).expect("test price fits uint128"),
                ask_fee_bps: 10_000,
                latest_update_block: 100,
                exists: true,
                block_delay: u8::MAX,
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
            let app = Router::new().route("/", post(rpc)).with_state(rpc_state);
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

    pub(super) fn rpc_url(&self) -> String {
        format!("http://{}", self.rpc_address)
    }

    pub(super) fn websocket_url(&self) -> String {
        format!("ws://{}", self.websocket_address)
    }

    pub(super) fn publish(&self, event: MockEvent) {
        match event {
            MockEvent::Header(block) | MockEvent::Gap(block) => {
                self.state.block.store(block, Ordering::Relaxed);
            }
        }
        let _ = self.state.events.send(event);
    }

    pub(super) async fn stop(mut self) {
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

pub(super) struct RedisProcess {
    pub(super) url: String,
    child: Option<Child>,
}

impl RedisProcess {
    pub(super) async fn start(configured: Option<String>) -> Result<Self, E2eError> {
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

    pub(super) async fn stop(mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }
}

pub(super) struct Workspace {
    path: PathBuf,
}

impl Workspace {
    pub(super) fn create() -> Result<Self, E2eError> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("lunarbase-e2e-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    pub(super) fn config(&self, name: &str) -> PathBuf {
        self.path.join(format!("{name}.toml"))
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
