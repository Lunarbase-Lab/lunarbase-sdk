//! Composition of the common runtime with one compiled network client.

use crate::config::ValidatedConfig;
use lunarbase_client_core::{
    CheckpointStore, ClientConnectConfig, ConnectedQuoteClient, ContractFilter, IndexerError,
    RedisCheckpointStore, RpcHttpClient, RpcSnapshotProvider, SharedCheckpointStore,
    MATH_COMPATIBILITY_VERSION,
};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{broadcast, watch, RwLock};
use tokio::time::{interval, sleep, MissedTickBehavior};

const SERVICE_EVENT_CAPACITY: usize = 512;

include!("runtime/errors.rs");
include!("runtime/handle.rs");
include!("runtime/factory.rs");
include!("runtime/supervisor.rs");
include!("runtime/lease.rs");
