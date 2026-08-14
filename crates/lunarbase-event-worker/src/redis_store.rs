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
use tokio::sync::{mpsc, oneshot};

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
}

struct RedisDepthGuard(Arc<Metrics>);

impl Drop for RedisDepthGuard {
    fn drop(&mut self) {
        self.0.redis_finished();
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
        queue_bound: usize,
        metrics: Arc<Metrics>,
    ) -> Result<(Self, RedisWriter), StoreError> {
        let client = redis::Client::open(url).map_err(redis_error)?;
        let keys = Arc::new(RedisKeys::new(namespace, chain_id, core));
        let (sender, receiver) = mpsc::channel(queue_bound);
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
        self.metrics.redis_started();
        permit.send(Command {
            operation,
            response,
            _depth: RedisDepthGuard(self.metrics.clone()),
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
            } = command;
            let result = self.execute(operation);
            if matches!(&result, Err(StoreError::Redis(_))) {
                self.connection = None;
                self.initialized = false;
            }
            drop(_depth);
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
mod tests {
    use super::{RedisEventStore, RedisKeys, StoreError};
    use crate::{event::DurableEvent, metrics::Metrics};
    use alloy_primitives::{Address, B256, Bytes};
    use lunarbase_client::model::{ChainCursor, Commitment, ContractLog};
    use std::{sync::Arc, time::Duration};

    #[test]
    fn deployment_keys_share_one_cluster_hash_slot() {
        let keys = RedisKeys::new("lunarbase", 8453, Address::new([3; 20]));
        let tag = "{8453:0x0303030303030303030303030303030303030303}";
        assert!(keys.stream.contains(tag));
        assert!(keys.cursor.contains(tag));
        assert!(keys.cursor_order.contains(tag));
        assert!(keys.event_ids.contains(tag));
        assert!(keys.metadata.contains(tag));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires LUNARBASE_TEST_REDIS_URL with AOF fsync-always"]
    async fn durable_redis_append_is_atomic_and_idempotent() {
        let url = std::env::var("LUNARBASE_TEST_REDIS_URL").expect("durable Redis URL");
        let core = Address::new([3; 20]);
        let metrics = Arc::new(Metrics::new(8, 8));
        let namespace = format!("lunarbase-test-{}", std::process::id());
        let (store, writer) = RedisEventStore::start(
            url.clone(),
            &namespace,
            "integration-consumers".into(),
            8453,
            core,
            Duration::from_secs(2),
            8,
            metrics,
        )
        .unwrap();
        store.initialize().await.unwrap();

        let applied_log = log(core, false);
        let applied = Arc::new(DurableEvent::from_log(&applied_log).unwrap());
        let first = store.append(applied.clone()).await.unwrap();
        let duplicate = store.append(applied).await.unwrap();
        assert!(first.appended);
        assert!(!duplicate.appended);
        assert_eq!(first.stream_id, duplicate.stream_id);

        let removed = Arc::new(DurableEvent::from_log(&log(core, true)).unwrap());
        assert!(store.append(removed).await.unwrap().appended);
        assert_eq!(
            store.load_cursor(8453, core).await.unwrap(),
            Some(applied_log.cursor.clone())
        );

        let client = redis::Client::open(url.clone()).unwrap();
        let mut connection = client.get_connection().unwrap();
        let stream_length = redis::cmd("XLEN")
            .arg(&store.keys().stream)
            .query::<usize>(&mut connection)
            .unwrap();
        assert_eq!(stream_length, 2);

        drop(store);
        writer.join().unwrap();

        let restarted_metrics = Arc::new(Metrics::new(8, 8));
        let (restarted, restarted_writer) = RedisEventStore::start(
            url,
            &namespace,
            "integration-consumers".into(),
            8453,
            core,
            Duration::from_secs(2),
            8,
            restarted_metrics,
        )
        .unwrap();
        restarted.initialize().await.unwrap();
        assert_eq!(
            restarted.load_cursor(8453, core).await.unwrap(),
            Some(applied_log.cursor.clone())
        );
        let replay = Arc::new(DurableEvent::from_log(&applied_log).unwrap());
        let replayed = restarted.append(replay).await.unwrap();
        assert!(!replayed.appended);
        assert_eq!(replayed.stream_id, first.stream_id);

        drop(restarted);
        restarted_writer.join().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires LUNARBASE_TEST_UNSAFE_REDIS_URL without AOF fsync-always"]
    async fn redis_without_fsync_always_is_rejected() {
        let url = std::env::var("LUNARBASE_TEST_UNSAFE_REDIS_URL").expect("unsafe Redis URL");
        let metrics = Arc::new(Metrics::new(8, 8));
        let (store, writer) = RedisEventStore::start(
            url,
            "lunarbase-unsafe-test",
            "integration-consumers".into(),
            8453,
            Address::new([4; 20]),
            Duration::from_secs(2),
            8,
            metrics,
        )
        .unwrap();
        assert!(matches!(
            store.initialize().await.unwrap_err(),
            StoreError::Durability(_)
        ));
        drop(store);
        writer.join().unwrap();
    }

    fn log(core: Address, removed: bool) -> ContractLog {
        ContractLog {
            address: core,
            transaction_hash: Some(B256::new([7; 32])),
            topics: vec![B256::new([8; 32])],
            data: Bytes::from_static(&[9; 64]),
            removed,
            cursor: ChainCursor {
                chain_id: 8453,
                block_number: 41,
                execution_block_number: 41,
                block_hash: Some(B256::new([6; 32])),
                transaction_index: Some(2),
                log_index: Some(3),
                source_sequence: None,
                source_sub_index: None,
                commitment: Commitment::Canonical,
            },
        }
    }
}
