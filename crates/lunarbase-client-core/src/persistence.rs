//! Core durable checkpoint and bounded update-stream storage.
//!
//! Redis is an optional process-boundary persistence layer; the in-memory
//! implementation provides deterministic tests and embedded deployments. Both
//! implementations expose the same atomic checkpoint contract.

use crate::protocol::codec::{
    bytes32_hex, decode_checkpoint, decode_fixed_hex32, decode_update, encode_checkpoint,
    encode_update,
};
use crate::{ChainCursor, ChainUpdate, Checkpoint, Commitment, RedisMeta, RedisNamespace};
use lunarbase_math::QuoteOutcome;
use redis::Commands;
use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

const DEFAULT_REDIS_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Shared store handle used by the high-level client. The reducer remains the
/// single writer; this lock only serializes checkpoint publication.
pub type SharedCheckpointStore = std::sync::Arc<tokio::sync::Mutex<Box<dyn CheckpointStore>>>;
/// The in-memory implementation is used by deterministic tests and by callers
/// that provide a Redis implementation at the process boundary. It commits a
/// complete checkpoint and ordered update payload atomically.
pub trait CheckpointStore: Send + Sync {
    /// Loads the most recent compatibility-checked checkpoint, if present.
    fn load(&self) -> Option<Checkpoint>;
    /// Atomically publishes a checkpoint and ordered update batch.
    fn commit(&mut self, checkpoint: Checkpoint, updates: Vec<ChainUpdate>) -> Result<(), String>;
    /// Returns the bounded ordered stream retained for worker catch-up.
    fn updates(&self) -> Vec<ChainUpdate>;
    /// Attempts to acquire the single-writer lease for `owner`.
    ///
    /// Embedded stores have no cross-process coordination and therefore
    /// acquire by default. Durable multi-replica stores must override this
    /// method with an atomic compare-and-set operation.
    fn acquire_writer_lease(&mut self, _owner: &str, _ttl: Duration) -> Result<bool, String> {
        Ok(true)
    }
    /// Renews the lease only while it is still owned by `owner`.
    fn renew_writer_lease(&mut self, _owner: &str, _ttl: Duration) -> Result<bool, String> {
        Ok(true)
    }
    /// Releases the lease only while it is still owned by `owner`.
    fn release_writer_lease(&mut self, _owner: &str) -> Result<(), String> {
        Ok(())
    }
    /// Requires future commits to prove ownership of the supplied lease key.
    ///
    /// Durable stores must enforce this inside the same atomic operation as
    /// checkpoint publication, not with a racy preflight read.
    fn configure_writer_lease(&mut self, _owner: Option<&str>) {}
}

/// A concrete synchronous Redis checkpoint implementation. Redis is used for
/// durable state and catch-up, never as the hot quote path.
pub struct RedisCheckpointStore {
    client: redis::Client,
    connection: Mutex<redis::Connection>,
    namespace: RedisNamespace,
    max_updates: usize,
    dedup_ttl_seconds: u64,
    io_timeout: Duration,
    writer_lease_owner: Option<String>,
}

type RedisStreamEntries = Vec<(String, Vec<(String, Vec<u8>)>)>;

fn open_connection(
    client: &redis::Client,
    io_timeout: Duration,
) -> redis::RedisResult<redis::Connection> {
    let connection = client.get_connection_with_timeout(io_timeout)?;
    connection.set_read_timeout(Some(io_timeout))?;
    connection.set_write_timeout(Some(io_timeout))?;
    Ok(connection)
}

impl RedisCheckpointStore {
    /// Opens a Redis-backed store with the default one-day deduplication TTL.
    pub fn connect(
        url: &str,
        namespace: RedisNamespace,
        max_updates: usize,
    ) -> redis::RedisResult<Self> {
        Self::connect_with_config(url, namespace, max_updates, 86_400)
    }

    /// Opens a Redis store with explicit stream and deduplication bounds.
    ///
    /// The connection is managed behind a mutex and transparently retried once
    /// after a transport failure. The atomic Lua commit is idempotent, so an
    /// ambiguous disconnect cannot duplicate a stream update.
    pub fn connect_with_config(
        url: &str,
        namespace: RedisNamespace,
        max_updates: usize,
        dedup_ttl_seconds: u64,
    ) -> redis::RedisResult<Self> {
        Self::connect_with_io_timeout(
            url,
            namespace,
            max_updates,
            dedup_ttl_seconds,
            DEFAULT_REDIS_IO_TIMEOUT,
        )
    }

    /// Opens a Redis store with explicit stream, deduplication, connect, read,
    /// and write bounds.
    pub fn connect_with_io_timeout(
        url: &str,
        namespace: RedisNamespace,
        max_updates: usize,
        dedup_ttl_seconds: u64,
        io_timeout: Duration,
    ) -> redis::RedisResult<Self> {
        if max_updates == 0 {
            return Err(redis::RedisError::from((
                redis::ErrorKind::InvalidClientConfig,
                "max_updates must be non-zero",
            )));
        }
        if dedup_ttl_seconds == 0 {
            return Err(redis::RedisError::from((
                redis::ErrorKind::InvalidClientConfig,
                "dedup_ttl_seconds must be non-zero",
            )));
        }
        if io_timeout.is_zero() {
            return Err(redis::RedisError::from((
                redis::ErrorKind::InvalidClientConfig,
                "io_timeout must be non-zero",
            )));
        }
        let client = redis::Client::open(url)?;
        let connection = open_connection(&client, io_timeout)?;
        Ok(Self {
            client,
            connection: Mutex::new(connection),
            namespace,
            max_updates,
            dedup_ttl_seconds,
            io_timeout,
            writer_lease_owner: None,
        })
    }

    /// Execute a Redis operation on the managed connection. A transport error
    /// causes one bounded reconnect-and-retry; the commit Lua script is
    /// idempotent, so retrying after an ambiguous socket close cannot append a
    /// duplicate update.
    fn with_connection<T>(
        &self,
        mut operation: impl FnMut(&mut redis::Connection) -> redis::RedisResult<T>,
    ) -> Result<T, String> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| "redis connection lock poisoned".to_string())?;
        match operation(&mut connection) {
            Ok(value) => Ok(value),
            Err(first) => {
                let replacement =
                    open_connection(&self.client, self.io_timeout).map_err(|retry| {
                        format!("Redis operation failed: {first}; reconnect failed: {retry}")
                    })?;
                *connection = replacement;
                operation(&mut connection)
                    .map_err(|retry| format!("Redis operation failed after reconnect: {retry}"))
            }
        }
    }

    /// Loads and decodes the durable checkpoint without hiding codec errors.
    pub fn load_checked(&self) -> Result<Option<Checkpoint>, String> {
        let bytes: Option<Vec<u8>> =
            self.with_connection(|connection| connection.get(&self.namespace.checkpoint))?;
        bytes.map(|bytes| decode_checkpoint(&bytes)).transpose()
    }

    /// Reads schema, math-version, and runtime-code-hash metadata.
    pub fn load_meta(&self) -> Result<Option<RedisMeta>, String> {
        let values: Vec<Option<String>> = self.with_connection(|connection| {
            redis::cmd("HMGET")
                .arg(&self.namespace.meta)
                .arg("schema_version")
                .arg("math_compatibility_version")
                .arg("expected_runtime_code_hash")
                .query(connection)
        })?;
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

    /// Checks that Redis metadata matches the running quote compatibility.
    pub fn validate_meta(
        &self,
        expected_runtime_code_hash: [u8; 32],
        math_compatibility_version: &str,
    ) -> Result<bool, String> {
        let Some(meta) = self.load_meta()? else {
            return Ok(false);
        };
        Ok(meta.schema_version == crate::SCHEMA_VERSION
            && meta.math_compatibility_version == math_compatibility_version
            && meta.expected_runtime_code_hash == expected_runtime_code_hash)
    }

    /// Performs a connectivity health check with a Redis `PING`.
    pub fn health(&self) -> Result<(), String> {
        let response: String =
            self.with_connection(|connection| redis::cmd("PING").query(connection))?;
        if response == "PONG" {
            Ok(())
        } else {
            Err(format!("unexpected Redis PING response: {response}"))
        }
    }

    /// Attempts to acquire the single-writer lease using `SET NX PX`.
    pub fn acquire_writer_lease(&self, owner: &str, ttl: Duration) -> redis::RedisResult<bool> {
        let ttl_milliseconds = duration_milliseconds(ttl)?;
        self.with_connection(|connection| {
            let result: Option<String> = redis::cmd("SET")
                .arg(&self.namespace.writer_lease)
                .arg(owner)
                .arg("NX")
                .arg("PX")
                .arg(ttl_milliseconds)
                .query(connection)?;
            Ok(result.is_some())
        })
        .map_err(managed_redis_error)
    }

    /// Extends the lease TTL only if `owner` still owns the key.
    pub fn renew_writer_lease(&self, owner: &str, ttl: Duration) -> redis::RedisResult<bool> {
        let ttl_milliseconds = duration_milliseconds(ttl)?;
        let script = redis::Script::new(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end",
        );
        self.with_connection(|connection| {
            let renewed: i32 = script
                .key(&self.namespace.writer_lease)
                .arg(owner)
                .arg(ttl_milliseconds)
                .invoke(connection)?;
            Ok(renewed == 1)
        })
        .map_err(managed_redis_error)
    }

    /// Releases the lease only when it is still owned by `owner`.
    pub fn release_writer_lease(&self, owner: &str) -> redis::RedisResult<()> {
        let script = redis::Script::new(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
        );
        self.with_connection(|connection| {
            let _: i32 = script
                .key(&self.namespace.writer_lease)
                .arg(owner)
                .invoke(connection)?;
            Ok(())
        })
        .map_err(managed_redis_error)
    }

    /// Reads all retained stream payloads and decodes them in stream order.
    pub fn updates_checked(&self) -> Result<Vec<ChainUpdate>, String> {
        let entries: RedisStreamEntries = self.with_connection(|connection| {
            redis::cmd("XRANGE")
                .arg(&self.namespace.updates)
                .arg("-")
                .arg("+")
                .query(connection)
        })?;
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
        let script = redis::Script::new(
            r#"
local owner = ARGV[6]
if owner ~= '' and redis.call('GET', KEYS[5]) ~= owner then
  return redis.error_reply('WRITER_LEASE_LOST')
end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], ARGV[1])
redis.call('HSET', KEYS[3], 'schema_version', ARGV[2], 'math_compatibility_version', ARGV[3], 'expected_runtime_code_hash', ARGV[4])
local count = tonumber(ARGV[5])
for i = 1, count do
  local payload_index = 7 + ((i - 1) * 2)
  local payload = ARGV[payload_index]
  local ttl = ARGV[payload_index + 1]
  local dedup = KEYS[5 + i]
  if redis.call('SET', dedup, '1', 'NX', 'EX', ttl) then
    redis.call('XADD', KEYS[4], '*', 'payload', payload)
  end
end
redis.call('XTRIM', KEYS[4], 'MAXLEN', '~', ARGV[7 + (count * 2)])
return 1
"#,
        );
        self.with_connection(|connection| {
            let mut invocation = script.prepare_invoke();
            invocation
                .key(&self.namespace.checkpoint)
                .key(&self.namespace.state)
                .key(&self.namespace.meta)
                .key(&self.namespace.updates)
                .key(&self.namespace.writer_lease)
                .arg(&checkpoint_bytes)
                .arg(checkpoint.schema_version)
                .arg(&checkpoint.math_compatibility_version)
                .arg(&meta_hash)
                .arg(updates.len())
                .arg(self.writer_lease_owner.as_deref().unwrap_or(""));
            for update in &updates {
                invocation
                    .key(update_dedup_key(&self.namespace, update))
                    .arg(encode_update(update))
                    .arg(self.dedup_ttl_seconds);
            }
            invocation.arg(self.max_updates).invoke::<i32>(connection)
        })
        .map(|_| ())
    }
    fn updates(&self) -> Vec<ChainUpdate> {
        self.updates_checked().unwrap_or_default()
    }
    fn acquire_writer_lease(&mut self, owner: &str, ttl: Duration) -> Result<bool, String> {
        RedisCheckpointStore::acquire_writer_lease(self, owner, ttl)
            .map_err(|error| error.to_string())
    }
    fn renew_writer_lease(&mut self, owner: &str, ttl: Duration) -> Result<bool, String> {
        RedisCheckpointStore::renew_writer_lease(self, owner, ttl)
            .map_err(|error| error.to_string())
    }
    fn release_writer_lease(&mut self, owner: &str) -> Result<(), String> {
        RedisCheckpointStore::release_writer_lease(self, owner).map_err(|error| error.to_string())
    }
    fn configure_writer_lease(&mut self, owner: Option<&str>) {
        self.writer_lease_owner = owner.map(str::to_owned);
    }
}

#[derive(Default)]
pub struct InMemoryRedisStore {
    checkpoint: Option<Checkpoint>,
    updates: VecDeque<ChainUpdate>,
    max_updates: usize,
    dedup: HashSet<String>,
    writer_lease: Option<(String, Instant)>,
    required_lease_owner: Option<String>,
}

impl InMemoryRedisStore {
    /// Creates a bounded deterministic store for tests or embedded callers.
    pub fn new(max_updates: usize) -> Self {
        Self {
            max_updates,
            dedup: HashSet::new(),
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
        if let Some(required_owner) = &self.required_lease_owner {
            let now = Instant::now();
            if !self
                .writer_lease
                .as_ref()
                .is_some_and(|(current_owner, expires_at)| {
                    current_owner == required_owner && *expires_at > now
                })
            {
                return Err("writer lease lost before checkpoint commit".into());
            }
        }
        self.checkpoint = Some(checkpoint);
        for update in updates {
            let identity = update_identity(&update);
            if self.dedup.insert(identity) {
                self.updates.push_back(update);
                while self.updates.len() > self.max_updates {
                    self.updates.pop_front();
                }
            }
        }
        Ok(())
    }
    fn updates(&self) -> Vec<ChainUpdate> {
        self.updates.iter().cloned().collect()
    }
    fn acquire_writer_lease(&mut self, owner: &str, ttl: Duration) -> Result<bool, String> {
        if ttl.is_zero() {
            return Err("writer lease TTL must be non-zero".into());
        }
        let now = Instant::now();
        if self
            .writer_lease
            .as_ref()
            .is_some_and(|(_, expires_at)| *expires_at > now)
        {
            return Ok(false);
        }
        self.writer_lease = Some((owner.to_owned(), now + ttl));
        Ok(true)
    }
    fn renew_writer_lease(&mut self, owner: &str, ttl: Duration) -> Result<bool, String> {
        if ttl.is_zero() {
            return Err("writer lease TTL must be non-zero".into());
        }
        let now = Instant::now();
        let Some((current_owner, expires_at)) = &mut self.writer_lease else {
            return Ok(false);
        };
        if *expires_at <= now {
            self.writer_lease = None;
            return Ok(false);
        }
        if current_owner != owner {
            return Ok(false);
        }
        *expires_at = now + ttl;
        Ok(true)
    }
    fn release_writer_lease(&mut self, owner: &str) -> Result<(), String> {
        if self
            .writer_lease
            .as_ref()
            .is_some_and(|(current_owner, _)| current_owner == owner)
        {
            self.writer_lease = None;
        }
        Ok(())
    }
    fn configure_writer_lease(&mut self, owner: Option<&str>) {
        self.required_lease_owner = owner.map(str::to_owned);
    }
}

fn duration_milliseconds(duration: Duration) -> redis::RedisResult<u64> {
    if duration.is_zero() {
        return Err(redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "writer lease TTL must be non-zero",
        )));
    }
    u64::try_from(duration.as_millis()).map_err(|_| {
        redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "writer lease TTL exceeds Redis PX range",
        ))
    })
}

fn managed_redis_error(error: String) -> redis::RedisError {
    redis::RedisError::from((
        redis::ErrorKind::IoError,
        "managed Redis operation failed",
        error,
    ))
}

pub(crate) fn update_dedup_key(namespace: &RedisNamespace, update: &ChainUpdate) -> String {
    format!("lb:{{{}}}:dedup:{}", namespace.tag, update_identity(update))
}

fn update_identity(update: &ChainUpdate) -> String {
    let (kind, cursor) = match update {
        ChainUpdate::Head(cursor) => ("head", Some(cursor)),
        ChainUpdate::Log(log) => ("log", Some(&log.cursor)),
        ChainUpdate::Reorg { new_head, .. } => ("reorg", Some(new_head)),
        ChainUpdate::Gap { cursor, .. } => ("gap", cursor.as_ref()),
        ChainUpdate::SourceHealth { healthy, .. } => {
            return format!("health:{}", u8::from(*healthy));
        }
    };
    cursor.map_or_else(
        || kind.to_owned(),
        |cursor| {
            format!(
                "{kind}:{}:{}:{}:{}",
                cursor.block_number,
                cursor.transaction_index.unwrap_or_default(),
                cursor.log_index.unwrap_or_default(),
                cursor.source_sequence.unwrap_or_default()
            )
        },
    )
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

#[cfg(test)]
mod lease_tests {
    use super::{CheckpointStore, InMemoryRedisStore};
    use crate::{ChainCursor, Checkpoint, Commitment, MATH_COMPATIBILITY_VERSION, SCHEMA_VERSION};
    use lunarbase_math::QuoteState;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn writer_lease_is_owner_checked_and_expires() {
        let mut store = InMemoryRedisStore::new(8);
        assert!(store
            .acquire_writer_lease("writer-a", Duration::from_millis(20))
            .unwrap());
        assert!(!store
            .acquire_writer_lease("writer-b", Duration::from_millis(20))
            .unwrap());
        assert!(!store
            .renew_writer_lease("writer-b", Duration::from_millis(20))
            .unwrap());
        assert!(!store
            .acquire_writer_lease("writer-b", Duration::from_millis(20))
            .unwrap());
        store.release_writer_lease("writer-a").unwrap();
        assert!(store
            .acquire_writer_lease("writer-b", Duration::from_millis(20))
            .unwrap());
        store.release_writer_lease("writer-a").unwrap();
        assert!(!store
            .acquire_writer_lease("writer-c", Duration::from_millis(20))
            .unwrap());
        sleep(Duration::from_millis(25));
        assert!(store
            .acquire_writer_lease("writer-c", Duration::from_millis(20))
            .unwrap());
    }

    #[test]
    fn writer_can_renew_and_release_its_own_lease() {
        let mut store = InMemoryRedisStore::new(8);
        assert!(store
            .acquire_writer_lease("writer", Duration::from_secs(1))
            .unwrap());
        assert!(store
            .renew_writer_lease("writer", Duration::from_secs(1))
            .unwrap());
        store.release_writer_lease("writer").unwrap();
        assert!(store
            .acquire_writer_lease("standby", Duration::from_secs(1))
            .unwrap());
    }

    #[test]
    fn checkpoint_commit_is_fenced_by_current_lease_owner() {
        let mut store = InMemoryRedisStore::new(8);
        assert!(store
            .acquire_writer_lease("writer-a", Duration::from_secs(1))
            .unwrap());
        store.configure_writer_lease(Some("writer-a"));
        assert!(store.commit(checkpoint(), Vec::new()).is_ok());
        store.release_writer_lease("writer-a").unwrap();
        assert!(store.commit(checkpoint(), Vec::new()).is_err());
        assert!(store
            .acquire_writer_lease("writer-b", Duration::from_secs(1))
            .unwrap());
        assert!(store.commit(checkpoint(), Vec::new()).is_err());
        store.configure_writer_lease(Some("writer-b"));
        assert!(store.commit(checkpoint(), Vec::new()).is_ok());
    }

    fn checkpoint() -> Checkpoint {
        Checkpoint {
            schema_version: SCHEMA_VERSION,
            math_compatibility_version: MATH_COMPATIBILITY_VERSION.into(),
            expected_runtime_code_hash: [0; 32],
            cursor: ChainCursor::block(1, 1, Some([1; 32]), Commitment::Canonical),
            state: QuoteState::default(),
        }
    }
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
