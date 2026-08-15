//! Persistent Redis Stream writer isolated from the asynchronous source runtime.

use crate::{
    event::{DurableEvent, EventError, STREAM_SCHEMA_VERSION, decode_cursor},
    metrics::Metrics,
};
use alloy_primitives::Address;
use lunarbase_client::model::ChainCursor;
use redis::Connection;
use std::{
    collections::HashMap,
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

const APPEND_EVENT_LUA: &str = r#"
local existing = redis.call('HGET', KEYS[4], ARGV[1])
if existing then
  return {existing, 0}
end
local stream_id = redis.call('XADD', KEYS[1], '*', unpack(ARGV, 4))
redis.call('HSET', KEYS[4], ARGV[1], stream_id)
local current_order = redis.call('GET', KEYS[3])
if (not current_order) or ARGV[3] >= current_order then
  redis.call('SET', KEYS[2], ARGV[2])
  redis.call('SET', KEYS[3], ARGV[3])
end
return {stream_id, 1}
"#;

#[derive(Clone, Debug)]
pub(crate) struct RedisKeys {
    pub stream: String,
    pub cursor: String,
    cursor_order: String,
    event_ids: String,
    metadata: String,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RedisQueueLimits {
    pub capacity: usize,
    pub byte_capacity: usize,
}

impl RedisKeys {
    pub(crate) fn new(namespace: &str, chain_id: u64, core: Address) -> Self {
        let deployment = format!("{{{chain_id}:{core:#x}}}");
        let prefix = format!("{namespace}:event:v{STREAM_SCHEMA_VERSION}:{deployment}");
        Self {
            stream: format!("{prefix}:stream"),
            cursor: format!("{prefix}:cursor"),
            cursor_order: format!("{prefix}:cursor-order"),
            event_ids: format!("{prefix}:event-ids"),
            metadata: format!("{prefix}:metadata"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AppendOutcome {
    pub stream_id: String,
    pub appended: bool,
}

#[derive(Debug, Error)]
pub(crate) enum StoreError {
    #[error("Redis: {0}")]
    Redis(String),
    #[error("Redis durability configuration: {0}")]
    Durability(String),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error("Redis writer stopped")]
    ChannelClosed,
    #[error("Redis writer thread panicked")]
    WorkerPanicked,
    #[error("Redis command exceeds the configured queue byte budget")]
    QueueByteLimit,
}

impl StoreError {
    pub(crate) fn retryable(&self) -> bool {
        matches!(self, Self::Redis(_))
    }
}

#[derive(Clone)]
pub(crate) struct RedisEventStore {
    sender: mpsc::Sender<Command>,
    metrics: Arc<Metrics>,
    keys: Arc<RedisKeys>,
    byte_budget: Arc<Semaphore>,
    byte_capacity: usize,
}

pub(crate) struct RedisWriter {
    handle: JoinHandle<()>,
}

enum Operation {
    Initialize,
    LoadCursor,
    Append(Arc<DurableEvent>),
}

enum Response {
    Initialized,
    Cursor(Option<Vec<u8>>),
    Appended(AppendOutcome),
}

struct Command {
    operation: Operation,
    response: oneshot::Sender<Result<Response, StoreError>>,
    _depth: RedisDepthGuard,
    _byte_permit: OwnedSemaphorePermit,
}

struct RedisDepthGuard(Arc<Metrics>, usize);

impl Drop for RedisDepthGuard {
    fn drop(&mut self) {
        self.0.redis_finished(self.1);
    }
}

struct BlockingStore {
    client: redis::Client,
    connection: Option<Connection>,
    initialized: bool,
    keys: Arc<RedisKeys>,
    group: String,
    timeout: Duration,
}

impl RedisEventStore {
    pub(crate) fn start(
        url: String,
        namespace: &str,
        group: String,
        chain_id: u64,
        core: Address,
        timeout: Duration,
        queue_limits: RedisQueueLimits,
        metrics: Arc<Metrics>,
    ) -> Result<(Self, RedisWriter), StoreError> {
        let client = redis::Client::open(url).map_err(redis_error)?;
        let keys = Arc::new(RedisKeys::new(namespace, chain_id, core));
        let (sender, receiver) = mpsc::channel(queue_limits.capacity);
        let byte_budget = Arc::new(Semaphore::new(queue_limits.byte_capacity));
        let worker_keys = keys.clone();
        let handle = thread::Builder::new()
            .name("lunarbase-redis-events".into())
            .spawn(move || {
                BlockingStore {
                    client,
                    connection: None,
                    initialized: false,
                    keys: worker_keys,
                    group,
                    timeout,
                }
                .run(receiver);
            })
            .map_err(|error| StoreError::Redis(error.to_string()))?;
        Ok((
            Self {
                sender,
                metrics,
                keys,
                byte_budget,
                byte_capacity: queue_limits.byte_capacity,
            },
            RedisWriter { handle },
        ))
    }

    pub(crate) fn keys(&self) -> &RedisKeys {
        &self.keys
    }

    pub(crate) async fn initialize(&self) -> Result<(), StoreError> {
        match self.request(Operation::Initialize).await? {
            Response::Initialized => Ok(()),
            _ => Err(StoreError::ChannelClosed),
        }
    }

    pub(crate) async fn load_cursor(
        &self,
        chain_id: u64,
        core: Address,
    ) -> Result<Option<ChainCursor>, StoreError> {
        match self.request(Operation::LoadCursor).await? {
            Response::Cursor(payload) => payload
                .map(|payload| decode_cursor(&payload, chain_id, core))
                .transpose()
                .map_err(StoreError::from),
            _ => Err(StoreError::ChannelClosed),
        }
    }

    pub(crate) async fn append(
        &self,
        event: Arc<DurableEvent>,
    ) -> Result<AppendOutcome, StoreError> {
        match self.request(Operation::Append(event)).await? {
            Response::Appended(outcome) => Ok(outcome),
            _ => Err(StoreError::ChannelClosed),
        }
    }

    async fn request(&self, operation: Operation) -> Result<Response, StoreError> {
        let bytes = operation.retained_bytes();
        if bytes > self.byte_capacity {
            return Err(StoreError::QueueByteLimit);
        }
        let byte_permit = match self
            .byte_budget
            .clone()
            .try_acquire_many_owned(bytes.max(1) as u32)
        {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                self.metrics.queue_saturated();
                self.byte_budget
                    .clone()
                    .acquire_many_owned(bytes.max(1) as u32)
                    .await
                    .map_err(|_| StoreError::ChannelClosed)?
            }
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(StoreError::ChannelClosed);
            }
        };
        let permit = match self.sender.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.queue_saturated();
                self.sender
                    .reserve()
                    .await
                    .map_err(|_| StoreError::ChannelClosed)?
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(StoreError::ChannelClosed),
        };
        let (response, receiver) = oneshot::channel();
        self.metrics.redis_started(bytes);
        permit.send(Command {
            operation,
            response,
            _depth: RedisDepthGuard(self.metrics.clone(), bytes),
            _byte_permit: byte_permit,
        });
        receiver.await.map_err(|_| StoreError::ChannelClosed)?
    }
}

impl RedisWriter {
    pub(crate) fn join(self) -> Result<(), StoreError> {
        self.handle.join().map_err(|_| StoreError::WorkerPanicked)
    }
}

impl BlockingStore {
    fn run(mut self, mut receiver: mpsc::Receiver<Command>) {
        while let Some(command) = receiver.blocking_recv() {
            let Command {
                operation,
                response,
                _depth,
                _byte_permit,
            } = command;
            let result = self.execute(operation);
            if matches!(&result, Err(StoreError::Redis(_))) {
                self.connection = None;
                self.initialized = false;
            }
            drop(_depth);
            drop(_byte_permit);
            let _ = response.send(result);
        }
    }

    fn execute(&mut self, operation: Operation) -> Result<Response, StoreError> {
        self.connect()?;
        let connection = self.connection.as_mut().ok_or(StoreError::ChannelClosed)?;
        if !self.initialized {
            initialize(connection, &self.keys, &self.group)?;
            self.initialized = true;
        }
        match operation {
            Operation::Initialize => Ok(Response::Initialized),
            Operation::LoadCursor => redis::cmd("GET")
                .arg(&self.keys.cursor)
                .query::<Option<Vec<u8>>>(connection)
                .map(Response::Cursor)
                .map_err(redis_error),
            Operation::Append(event) => {
                append(connection, &self.keys, &event).map(Response::Appended)
            }
        }
    }

    fn connect(&mut self) -> Result<(), StoreError> {
        if self.connection.is_some() {
            return Ok(());
        }
        let connection = self
            .client
            .get_connection_with_timeout(self.timeout)
            .map_err(redis_error)?;
        connection
            .set_read_timeout(Some(self.timeout))
            .map_err(redis_error)?;
        connection
            .set_write_timeout(Some(self.timeout))
            .map_err(redis_error)?;
        self.connection = Some(connection);
        self.initialized = false;
        Ok(())
    }
}

impl Operation {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(match self {
            Self::Append(event) => event.retained_bytes(),
            Self::Initialize | Self::LoadCursor => 0,
        })
    }
}

fn initialize(
    connection: &mut Connection,
    keys: &RedisKeys,
    group: &str,
) -> Result<(), StoreError> {
    redis::cmd("PING")
        .query::<String>(connection)
        .map_err(redis_error)?;
    verify_durability(connection)?;
    let schema = redis::cmd("HGET")
        .arg(&keys.metadata)
        .arg("schemaVersion")
        .query::<Option<String>>(connection)
        .map_err(redis_error)?;
    match schema {
        Some(version) if version != STREAM_SCHEMA_VERSION.to_string() => {
            return Err(StoreError::Durability(format!(
                "stream schema v{version} cannot be opened as v{STREAM_SCHEMA_VERSION}"
            )));
        }
        None => {
            redis::cmd("HSET")
                .arg(&keys.metadata)
                .arg("schemaVersion")
                .arg(STREAM_SCHEMA_VERSION)
                .query::<usize>(connection)
                .map_err(redis_error)?;
        }
        Some(_) => {}
    }
    match redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&keys.stream)
        .arg(group)
        .arg("0-0")
        .arg("MKSTREAM")
        .query::<String>(connection)
    {
        Ok(_) => Ok(()),
        Err(error) if error.to_string().contains("BUSYGROUP") => Ok(()),
        Err(error) => Err(redis_error(error)),
    }
}

fn verify_durability(connection: &mut Connection) -> Result<(), StoreError> {
    let values = redis::cmd("CONFIG")
        .arg("GET")
        .arg("append*")
        .query::<Vec<String>>(connection)
        .map_err(redis_error)?;
    let config = values
        .chunks_exact(2)
        .map(|entry| (entry[0].to_ascii_lowercase(), entry[1].to_ascii_lowercase()))
        .collect::<HashMap<_, _>>();
    if config.get("appendonly").map(String::as_str) != Some("yes")
        || config.get("appendfsync").map(String::as_str) != Some("always")
    {
        return Err(StoreError::Durability(
            "require appendonly=yes and appendfsync=always".into(),
        ));
    }
    Ok(())
}

fn append(
    connection: &mut Connection,
    keys: &RedisKeys,
    event: &DurableEvent,
) -> Result<AppendOutcome, StoreError> {
    let mut command = redis::cmd("EVAL");
    command
        .arg(APPEND_EVENT_LUA)
        .arg(4)
        .arg(&keys.stream)
        .arg(&keys.cursor)
        .arg(&keys.cursor_order)
        .arg(&keys.event_ids)
        .arg(&event.event_id)
        .arg(&event.cursor_json)
        .arg(&event.cursor_order);
    for (name, value) in &event.fields {
        command.arg(name).arg(value);
    }
    let (stream_id, appended) = command
        .query::<(String, i64)>(connection)
        .map_err(redis_error)?;
    Ok(AppendOutcome {
        stream_id,
        appended: appended == 1,
    })
}

fn redis_error(error: redis::RedisError) -> StoreError {
    StoreError::Redis(error.to_string())
}

#[cfg(test)]
#[path = "redis_store_tests.rs"]
mod tests;
