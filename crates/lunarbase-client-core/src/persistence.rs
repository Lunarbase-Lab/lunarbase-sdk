//! Core durable checkpoint and bounded update-stream storage.
//!
//! Redis is an optional process-boundary persistence layer; the in-memory
//! implementation provides deterministic tests and embedded deployments. Both
//! implementations expose the same atomic checkpoint contract.

use crate::protocol::codec::{
    bytes32_hex, decode_checkpoint, decode_fixed_hex32, decode_update, encode_checkpoint,
    encode_update,
};
use crate::{ChainUpdate, Checkpoint, RedisMeta, RedisNamespace};
use redis::Commands;
use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

include!("persistence/contract.rs");
include!("persistence/redis_store.rs");
include!("persistence/memory_store.rs");
include!("persistence/helpers.rs");
include!("persistence/lease_tests.rs");
