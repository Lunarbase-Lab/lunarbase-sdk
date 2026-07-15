use crate::codec::{
    bytes32_hex, decode_checkpoint, decode_fixed_hex32, decode_update, encode_checkpoint,
    encode_update,
};
use crate::{ChainCursor, ChainUpdate, Checkpoint, Commitment, RedisMeta, RedisNamespace};
use lunarbase_math::QuoteOutcome;
use redis::Commands;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};
/// The in-memory implementation is used by deterministic tests and by callers
/// that provide a Redis implementation at the process boundary. It commits a
/// complete checkpoint and ordered update payload atomically.
pub trait CheckpointStore: Send + Sync {
    fn load(&self) -> Option<Checkpoint>;
    fn commit(&mut self, checkpoint: Checkpoint, updates: Vec<ChainUpdate>) -> Result<(), String>;
    fn updates(&self) -> Vec<ChainUpdate>;
}

/// A concrete synchronous Redis checkpoint implementation. Redis is used for
/// durable state and catch-up, never as the hot quote path.
pub struct RedisCheckpointStore {
    connection: Mutex<redis::Connection>,
    namespace: RedisNamespace,
    max_updates: usize,
}

type RedisStreamEntries = Vec<(String, Vec<(String, Vec<u8>)>)>;

impl RedisCheckpointStore {
    pub fn connect(
        url: &str,
        namespace: RedisNamespace,
        max_updates: usize,
    ) -> redis::RedisResult<Self> {
        if max_updates == 0 {
            return Err(redis::RedisError::from((
                redis::ErrorKind::InvalidClientConfig,
                "max_updates must be non-zero",
            )));
        }
        let client = redis::Client::open(url)?;
        Ok(Self {
            connection: Mutex::new(client.get_connection()?),
            namespace,
            max_updates,
        })
    }

    pub fn load_checked(&self) -> Result<Option<Checkpoint>, String> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| "redis connection lock poisoned")?;
        let bytes: Option<Vec<u8>> = connection
            .get(&self.namespace.checkpoint)
            .map_err(|error| error.to_string())?;
        bytes.map(|bytes| decode_checkpoint(&bytes)).transpose()
    }

    pub fn load_meta(&self) -> Result<Option<RedisMeta>, String> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| "redis connection lock poisoned")?;
        let values: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(&self.namespace.meta)
            .arg("schema_version")
            .arg("math_compatibility_version")
            .arg("expected_runtime_code_hash")
            .query(&mut *connection)
            .map_err(|error: redis::RedisError| error.to_string())?;
        if values.iter().all(Option::is_none) {
            return Ok(None);
        }
        let schema_version = values
            .first()
            .and_then(Option::as_deref)
            .ok_or("Redis meta missing schema_version")?
            .parse::<u16>()
            .map_err(|_| "Redis meta has invalid schema_version")?;
        let math_compatibility_version = values
            .get(1)
            .and_then(Clone::clone)
            .ok_or("Redis meta missing math_compatibility_version")?;
        let hash = values
            .get(2)
            .and_then(Option::as_deref)
            .ok_or("Redis meta missing expected_runtime_code_hash")?;
        let expected_runtime_code_hash = decode_fixed_hex32(hash)?;
        Ok(Some(RedisMeta {
            schema_version,
            math_compatibility_version,
            expected_runtime_code_hash,
        }))
    }

    pub fn acquire_writer_lease(&self, owner: &str, ttl_seconds: u64) -> redis::RedisResult<bool> {
        let mut connection = self.connection.lock().map_err(|_| {
            redis::RedisError::from((redis::ErrorKind::IoError, "redis connection lock poisoned"))
        })?;
        let result: Option<String> = redis::cmd("SET")
            .arg(&self.namespace.writer_lease)
            .arg(owner)
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query(&mut *connection)?;
        Ok(result.is_some())
    }

    pub fn release_writer_lease(&self, owner: &str) -> redis::RedisResult<()> {
        let mut connection = self.connection.lock().map_err(|_| {
            redis::RedisError::from((redis::ErrorKind::IoError, "redis connection lock poisoned"))
        })?;
        let script = redis::Script::new(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
        );
        let _: i32 = script
            .key(&self.namespace.writer_lease)
            .arg(owner)
            .invoke(&mut *connection)?;
        Ok(())
    }

    pub fn updates_checked(&self) -> Result<Vec<ChainUpdate>, String> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| "redis connection lock poisoned")?;
        let entries: RedisStreamEntries = redis::cmd("XRANGE")
            .arg(&self.namespace.updates)
            .arg("-")
            .arg("+")
            .query(&mut *connection)
            .map_err(|error: redis::RedisError| error.to_string())?;
        entries
            .into_iter()
            .flat_map(|(_, fields)| {
                fields
                    .into_iter()
                    .filter(|(key, _)| key == "payload")
                    .map(|(_, value)| decode_update(&value))
            })
            .collect()
    }
}

impl CheckpointStore for RedisCheckpointStore {
    fn load(&self) -> Option<Checkpoint> {
        self.load_checked().ok().flatten()
    }
    fn commit(&mut self, checkpoint: Checkpoint, updates: Vec<ChainUpdate>) -> Result<(), String> {
        let checkpoint_bytes = encode_checkpoint(&checkpoint)?;
        let meta_hash = bytes32_hex(checkpoint.expected_runtime_code_hash);
        let mut pipeline = redis::pipe();
        pipeline
            .atomic()
            .cmd("SET")
            .arg(&self.namespace.checkpoint)
            .arg(&checkpoint_bytes)
            .ignore()
            .cmd("SET")
            .arg(&self.namespace.state)
            .arg(&checkpoint_bytes)
            .ignore()
            .cmd("HSET")
            .arg(&self.namespace.meta)
            .arg("schema_version")
            .arg(checkpoint.schema_version)
            .arg("math_compatibility_version")
            .arg(&checkpoint.math_compatibility_version)
            .arg("expected_runtime_code_hash")
            .arg(meta_hash)
            .ignore();
        for update in &updates {
            pipeline
                .cmd("XADD")
                .arg(&self.namespace.updates)
                .arg("*")
                .arg("payload")
                .arg(encode_update(update))
                .ignore();
        }
        pipeline
            .cmd("XTRIM")
            .arg(&self.namespace.updates)
            .arg("MAXLEN")
            .arg("~")
            .arg(self.max_updates)
            .ignore();
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| "redis connection lock poisoned")?;
        pipeline
            .query::<()>(&mut *connection)
            .map_err(|error| error.to_string())
    }
    fn updates(&self) -> Vec<ChainUpdate> {
        self.updates_checked().unwrap_or_default()
    }
}

#[derive(Default)]
pub struct InMemoryRedisStore {
    checkpoint: Option<Checkpoint>,
    updates: VecDeque<ChainUpdate>,
    max_updates: usize,
}

impl InMemoryRedisStore {
    pub fn new(max_updates: usize) -> Self {
        Self {
            max_updates,
            ..Default::default()
        }
    }
}

impl CheckpointStore for InMemoryRedisStore {
    fn load(&self) -> Option<Checkpoint> {
        self.checkpoint.clone()
    }
    fn commit(&mut self, checkpoint: Checkpoint, updates: Vec<ChainUpdate>) -> Result<(), String> {
        if self.max_updates == 0 {
            return Err("update stream capacity must be non-zero".into());
        }
        self.checkpoint = Some(checkpoint);
        for update in updates {
            self.updates.push_back(update);
            while self.updates.len() > self.max_updates {
                self.updates.pop_front();
            }
        }
        Ok(())
    }
    fn updates(&self) -> Vec<ChainUpdate> {
        self.updates.iter().cloned().collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientQuote {
    pub outcome: QuoteOutcome,
    pub cursor: ChainCursor,
    pub commitment: Commitment,
    pub observed_at: SystemTime,
    pub age: Duration,
    pub stale: bool,
    pub contract_code_hash: [u8; 32],
    pub math_compatibility_version: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexerHealth {
    pub ready: bool,
    pub commitment: Commitment,
    pub cursor: Option<ChainCursor>,
    pub code_hash: [u8; 32],
    pub math_compatibility_version: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FreshnessPolicy {
    pub minimum_commitment: Commitment,
    pub max_age_blocks: Option<u64>,
}

impl Default for FreshnessPolicy {
    fn default() -> Self {
        Self {
            minimum_commitment: Commitment::Realtime,
            max_age_blocks: None,
        }
    }
}
