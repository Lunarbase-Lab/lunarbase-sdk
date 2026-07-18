//! Self-contained process-level E2E harness for the real indexer binary.

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use lunarbase_client_core::{TOPIC_LANE_ADDED, TOPIC_LANE_REMOVED};
use lunarbase_math::{encode_lane_slot0, Address, LaneSlot0, B256, U256, WAD};
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

mod assertions;
mod environment;
mod helpers;
mod process;
mod rpc_mock;
mod scenarios;
mod websocket_mock;

pub use environment::{E2eArguments, E2eError};
pub use scenarios::run;
