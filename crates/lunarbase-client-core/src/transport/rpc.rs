use crate::protocol::abi::{lane_discovery_topics, TOPIC_LANE_ADDED, TOPIC_LANE_REMOVED};
use crate::source::{NormalizedBackend, SourceStream};
use crate::{
    BackfillRequest, BootstrapSnapshot, ChainCursor, Commitment, ContractLog, DeploymentConfig,
    Network, SnapshotProvider, SourceError,
};
use async_trait::async_trait;
use lunarbase_math::{Address, LaneState, QuoteState, U256};
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use thiserror::Error;
use tiny_keccak::{Hasher, Keccak};

include!("rpc/client.rs");
include!("rpc/backend.rs");
include!("rpc/snapshot.rs");
include!("rpc/codec.rs");
include!("rpc/tests.rs");
