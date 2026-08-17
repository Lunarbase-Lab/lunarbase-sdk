//! Persistent Redis Stream writer isolated from the asynchronous source runtime.

#[path = "redis_store/commands.rs"]
mod commands;
#[path = "redis_store/correction.rs"]
mod correction;
#[path = "redis_store/window.rs"]
mod window;

pub(crate) use correction::CorrectionLimits;

use crate::{
    event::{
        DurableEvent, DurableHead, EventError, ReorgCorrection, STREAM_SCHEMA_VERSION,
        commitment_name, decode_cursor,
    },
    metrics::Metrics,
};
use alloy_primitives::Address;
use lunarbase_client::model::{ChainCursor, Commitment};
use redis::Connection;
use std::{
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

#[derive(Clone, Debug)]
pub(crate) struct RedisKeys {
    pub stream: String,
    pub cursor: String,
    cursor_order: String,
    resume: String,
    record_ids: String,
    log_state: String,
    headers: String,
    canonical_height: String,
    canonical_head: String,
    finalized_head: String,
    reorg_manifest: String,
    journal_usage: String,
    metadata: String,
    block_prefix: String,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RedisDeployment {
    pub chain_id: u64,
    pub core: Address,
    pub delivery_mode: Commitment,
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
            resume: format!("{prefix}:resume"),
            record_ids: format!("{prefix}:record-ids"),
            log_state: format!("{prefix}:log-state"),
            headers: format!("{prefix}:headers"),
            canonical_height: format!("{prefix}:canonical-height"),
            canonical_head: format!("{prefix}:canonical-head"),
            finalized_head: format!("{prefix}:finalized-head"),
            reorg_manifest: format!("{prefix}:reorg-manifest"),
            journal_usage: format!("{prefix}:journal-usage"),
            metadata: format!("{prefix}:metadata"),
            block_prefix: format!("{prefix}:block:"),
        }
    }

    pub(crate) fn block_logs(&self, block_hash: &str) -> String {
        format!("{}{block_hash}:logs", self.block_prefix)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AppendOutcome {
    pub stream_id: String,
    pub appended: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct JournalWindow {
    pub blocks: Vec<lunarbase_client::model::BlockRef>,
    pub finalized: Option<lunarbase_client::model::BlockRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CorrectionOutcome {
    pub stream_id: String,
    pub appended: bool,
    pub reverted: usize,
    pub applied: usize,
}

#[derive(Debug, Error)]
pub(crate) enum StoreError {
    #[error("Redis: {0}")]
    Redis(String),
    #[error("Redis durability configuration: {0}")]
    Durability(String),
    #[error("Redis journal invariant: {0}")]
    Journal(String),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error("fork correction exceeds a configured resource budget: {0}")]
    CorrectionBudget(String),
    #[error("fork correction JSON: {0}")]
    Json(#[from] serde_json::Error),
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
    LoadWindow {
        chain_id: u64,
        max_blocks: usize,
        max_bytes: usize,
    },
    AppendEvent(Arc<DurableEvent>),
    AppendHead(Arc<DurableHead>),
    Correct(Arc<ReorgCorrection>, CorrectionLimits),
}

enum Response {
    Initialized,
    Cursor(Option<Vec<u8>>),
    Window(JournalWindow),
    Appended(AppendOutcome),
    Corrected(CorrectionOutcome),
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
    metadata: commands::DeploymentMetadata,
    script: redis::Script,
    correction_script: redis::Script,
    group: String,
    timeout: Duration,
}

impl RedisEventStore {
    pub(crate) fn start(
        url: String,
        namespace: &str,
        group: String,
        deployment: RedisDeployment,
        timeout: Duration,
        queue_limits: RedisQueueLimits,
        metrics: Arc<Metrics>,
    ) -> Result<(Self, RedisWriter), StoreError> {
        let client = redis::Client::open(url).map_err(redis_error)?;
        let keys = Arc::new(RedisKeys::new(
            namespace,
            deployment.chain_id,
            deployment.core,
        ));
        let metadata = commands::DeploymentMetadata::new(
            deployment.chain_id,
            deployment.core,
            commitment_name(deployment.delivery_mode),
        );
        let script = commands::script();
        let correction_script = correction::script();
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
                    metadata,
                    script,
                    correction_script,
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

    pub(crate) async fn append_event(
        &self,
        event: Arc<DurableEvent>,
    ) -> Result<AppendOutcome, StoreError> {
        match self.request(Operation::AppendEvent(event)).await? {
            Response::Appended(outcome) => Ok(outcome),
            _ => Err(StoreError::ChannelClosed),
        }
    }

    pub(crate) async fn load_window(
        &self,
        chain_id: u64,
        max_blocks: usize,
        max_bytes: usize,
    ) -> Result<JournalWindow, StoreError> {
        let operation = Operation::LoadWindow {
            chain_id,
            max_blocks,
            max_bytes,
        };
        match self.request(operation).await? {
            Response::Window(window) => Ok(window),
            _ => Err(StoreError::ChannelClosed),
        }
    }

    pub(crate) async fn append_head(
        &self,
        head: Arc<DurableHead>,
    ) -> Result<AppendOutcome, StoreError> {
        match self.request(Operation::AppendHead(head)).await? {
            Response::Appended(outcome) => Ok(outcome),
            _ => Err(StoreError::ChannelClosed),
        }
    }

    pub(crate) async fn correct(
        &self,
        correction: Arc<ReorgCorrection>,
        limits: CorrectionLimits,
    ) -> Result<CorrectionOutcome, StoreError> {
        match self.request(Operation::Correct(correction, limits)).await? {
            Response::Corrected(outcome) => Ok(outcome),
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
            commands::initialize(
                connection,
                &self.keys,
                &self.group,
                &self.metadata,
                &self.script,
                &self.correction_script,
            )?;
            self.initialized = true;
        }
        match operation {
            Operation::Initialize => Ok(Response::Initialized),
            Operation::LoadCursor => redis::cmd("GET")
                .arg(&self.keys.cursor)
                .query::<Option<Vec<u8>>>(connection)
                .map(Response::Cursor)
                .map_err(redis_error),
            Operation::LoadWindow {
                chain_id,
                max_blocks,
                max_bytes,
            } => window::load(connection, &self.keys, chain_id, max_blocks, max_bytes)
                .map(Response::Window),
            Operation::AppendEvent(event) => {
                commands::append_event(connection, &self.keys, &self.metadata, &self.script, &event)
                    .map(Response::Appended)
            }
            Operation::AppendHead(head) => {
                commands::append_head(connection, &self.keys, &self.metadata, &self.script, &head)
                    .map(Response::Appended)
            }
            Operation::Correct(correction, limits) => correction::correct(
                connection,
                &self.keys,
                &self.metadata,
                &self.correction_script,
                &correction,
                limits,
            )
            .map(Response::Corrected),
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
            Self::AppendEvent(event) => event.retained_bytes(),
            Self::AppendHead(head) => head.retained_bytes(),
            Self::Correct(correction, _) => correction.retained_bytes(),
            Self::LoadWindow { .. } => 0,
            Self::Initialize | Self::LoadCursor => 0,
        })
    }
}

fn redis_error(error: redis::RedisError) -> StoreError {
    let detail = error.to_string();
    if detail.contains("LUNARBASE_") {
        StoreError::Journal(detail)
    } else {
        StoreError::Redis(detail)
    }
}

#[cfg(test)]
#[path = "redis_store_reorg_tests.rs"]
mod reorg_tests;

#[cfg(test)]
#[path = "redis_store_tests.rs"]
mod tests;
