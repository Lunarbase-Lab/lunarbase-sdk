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
