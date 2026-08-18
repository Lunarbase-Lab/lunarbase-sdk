//! In-memory resume cursor and compact proposal state retained across reconnects.

use super::{MonadParserConfig, wire::StreamBounds};
use crate::{
    execution::ExecutionEvent,
    lifecycle::{LifecycleLimits, ProposalLifecycle, RawExecRecord},
};
use lunarbase_client::model::{ContractFilter, SourceError};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Default)]
pub(crate) struct ParserV2Session {
    connected: AtomicBool,
    state: Mutex<SessionState>,
}

#[derive(Debug, Default)]
struct SessionState {
    stream_id: Option<String>,
    acknowledged: u64,
    processed: u64,
    rebase: bool,
    lifecycle: Option<ProposalLifecycle>,
}

pub(super) struct ConnectionLease(Arc<ParserV2Session>);

impl Drop for ConnectionLease {
    fn drop(&mut self) {
        self.0.connected.store(false, Ordering::Release);
    }
}

impl ParserV2Session {
    pub(super) fn acquire(self: &Arc<Self>) -> Result<ConnectionLease, SourceError> {
        self.connected
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                SourceError::Unavailable(
                    "Monad parser session already has an active subscriber".into(),
                )
            })?;
        Ok(ConnectionLease(self.clone()))
    }

    pub(super) fn expected_stream_id(&self) -> Result<Option<String>, SourceError> {
        Ok(self.lock()?.stream_id.clone())
    }

    pub(super) fn prepare(
        &self,
        stream_id: &str,
        bounds: StreamBounds,
        config: &MonadParserConfig,
        filter: ContractFilter,
    ) -> Result<u64, SourceError> {
        let mut state = self.lock()?;
        if state
            .stream_id
            .as_deref()
            .is_some_and(|known| known != stream_id)
        {
            state.stream_id = None;
            state.rebase = true;
            state.lifecycle = None;
            return Err(SourceError::Gap(
                "Monad parser stream identity changed".into(),
            ));
        }
        if state.stream_id.is_none() || state.rebase {
            let after = bounds.latest_sequence.unwrap_or(0);
            state.stream_id = Some(stream_id.to_owned());
            state.acknowledged = after;
            state.processed = after;
            state.rebase = false;
            state.lifecycle = Some(ProposalLifecycle::new(
                config.chain_id,
                config.delivery_mode,
                config.emit_removed_logs,
                filter,
                LifecycleLimits {
                    max_proposals: config.max_pending_proposals,
                    max_logs: config.max_pending_logs,
                    max_bytes: config.max_pending_bytes,
                },
            ));
            return Ok(after);
        }
        Ok(state.acknowledged)
    }

    pub(super) fn process(
        &self,
        record: RawExecRecord,
    ) -> Result<(Vec<ExecutionEvent>, u64), SourceError> {
        let mut state = self.lock()?;
        if record.sequence <= state.processed {
            return Ok((Vec::new(), state.acknowledged));
        }
        if record.sequence != state.processed.saturating_add(1) {
            return Err(SourceError::Gap(
                "Monad durable stream sequence is non-contiguous".into(),
            ));
        }
        let output = state
            .lifecycle
            .as_mut()
            .ok_or_else(|| SourceError::Gap("Monad lifecycle is not initialized".into()))?
            .process(record)?;
        state.processed = state.processed.saturating_add(1);
        Ok((output, state.acknowledged))
    }

    pub(super) fn confirm_ack(&self, sequence: u64) -> Result<(), SourceError> {
        let mut state = self.lock()?;
        if sequence > state.processed || sequence < state.acknowledged {
            return Err(SourceError::Gap(
                "Monad parser acknowledged an invalid local sequence".into(),
            ));
        }
        state.acknowledged = sequence;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn acknowledged(&self) -> Result<u64, SourceError> {
        Ok(self.lock()?.acknowledged)
    }

    pub(super) fn mark_rebase(&self) -> Result<(), SourceError> {
        let mut state = self.lock()?;
        state.rebase = true;
        state.lifecycle = None;
        Ok(())
    }

    pub(super) fn clear_identity_for_rebase(&self) -> Result<(), SourceError> {
        let mut state = self.lock()?;
        state.stream_id = None;
        state.rebase = true;
        state.lifecycle = None;
        Ok(())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, SessionState>, SourceError> {
        self.state
            .lock()
            .map_err(|_| SourceError::Unavailable("Monad parser session lock is poisoned".into()))
    }
}
