//! High-level quote indexer lifecycle and asynchronous core runtime facade.
//!
//! This context coordinates source subscription, snapshot handoff, reducer
//! recovery, freshness policy, and optional checkpoint persistence. The math
//! engine itself remains in `lunarbase-math` and is called only with immutable
//! state snapshots.

use crate::{
    decode_core_event, BackfillRequest, BootstrapSnapshot, ChainCursor, ChainEventSource,
    ChainUpdate, Checkpoint, Commitment, ContractFilter, ContractLog, DeploymentConfig,
    LogDecodeError, QuoteEvent, QuoteReducer, ReducerError, SharedCheckpointStore,
    SnapshotProvider, SourceError, MATH_COMPATIBILITY_VERSION, SCHEMA_VERSION,
};
use futures_util::StreamExt;
use lunarbase_math::{
    Address, QuoteContext, QuoteError, QuoteOutcome, QuoteRequest, QuoteState, U256,
};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, watch, Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

const RUNTIME_EVENT_CAPACITY: usize = 256;
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

include!("indexer/errors.rs");
include!("indexer/quote_types.rs");
include!("indexer/engine.rs");
include!("indexer/client_types.rs");
include!("indexer/client.rs");
include!("indexer/tasks.rs");
include!("indexer/checkpoint.rs");
