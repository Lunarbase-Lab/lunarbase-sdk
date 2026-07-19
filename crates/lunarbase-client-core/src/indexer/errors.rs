//! Failures and bounded operational events emitted by the client runtime.

use crate::model::{LogDecodeError, SourceError};
use crate::state::reducer::ReducerError;
use lunarbase_math::state::QuoteError;
use thiserror::Error;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Bootstrap, recovery, or quote failure.
pub enum IndexerError {
    /// Quote state has not reached a verified, serviceable cursor.
    #[error("not ready")]
    NotReady,
    /// A discontinuity invalidated the current state and requires recovery.
    #[error("source gap: {0}")]
    Gap(String),
    /// The ordered reducer rejected a state transition.
    #[error(transparent)]
    Reducer(#[from] ReducerError),
    /// Pure quote evaluation failed for the requested route or amount.
    #[error(transparent)]
    Quote(#[from] QuoteError),
    /// A quote-critical contract log did not match the expected ABI shape.
    #[error(transparent)]
    Decode(#[from] LogDecodeError),
    /// The configured chain data source failed.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// Runtime bytecode at the configured Core address differs from the pinned deployment.
    #[error("runtime code hash mismatch")]
    CodeHashMismatch,
    /// No verified chain position is available for a quote or checkpoint.
    #[error("no canonical cursor")]
    NoCursor,
    /// A thread panicked while holding the synchronous quote-state lock.
    #[error("runtime state lock was poisoned")]
    LockPoisoned,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Bounded operational event used by tracing and metrics.
pub enum ClientRuntimeEvent {
    /// The initial realtime subscription could not be established.
    SourceSubscribeFailed {
        /// Provider or transport error suitable for structured logs.
        detail: String,
    },
    /// An established realtime stream terminated with an error.
    SourceStreamFailed {
        /// Provider or transport error suitable for structured logs.
        detail: String,
    },
    /// The realtime stream ended without an explicit transport error.
    SourceStreamClosed,
    /// A normalized update could not be safely applied to quote state.
    StateTransitionFailed {
        /// Reducer or decoding error that triggered fail-closed recovery.
        detail: String,
    },
    /// Canonical snapshot and backfill recovery has started.
    RecoveryStarted,
    /// Canonical recovery completed and quote readiness was restored.
    RecoveryCompleted,
    /// A canonical recovery attempt failed.
    RecoveryFailed {
        /// Snapshot, backfill, or validation error reported by the source.
        detail: String,
    },
    /// A required background task exited before shutdown was requested.
    BackgroundTaskStopped {
        /// Stable task name used by logs and metrics.
        task: &'static str,
    },
    /// Graceful shutdown exceeded its configured deadline.
    ShutdownTimedOut,
    /// A required background task panicked.
    BackgroundTaskPanicked {
        /// Stable task name used by logs and metrics.
        task: &'static str,
        /// Panic payload converted to a diagnostic string.
        detail: String,
    },
}

impl ClientRuntimeEvent {
    /// Stable code suitable for metric labels and structured logs.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::SourceSubscribeFailed { .. } => "source_subscribe_failed",
            Self::SourceStreamFailed { .. } => "source_stream_failed",
            Self::SourceStreamClosed => "source_stream_closed",
            Self::StateTransitionFailed { .. } => "state_transition_failed",
            Self::RecoveryStarted => "recovery_started",
            Self::RecoveryCompleted => "recovery_completed",
            Self::RecoveryFailed { .. } => "recovery_failed",
            Self::BackgroundTaskStopped { .. } => "background_task_stopped",
            Self::ShutdownTimedOut => "shutdown_timed_out",
            Self::BackgroundTaskPanicked { .. } => "background_task_panicked",
        }
    }

    /// Human-readable context for structured tracing.
    pub fn detail(&self) -> String {
        match self {
            Self::SourceSubscribeFailed { detail }
            | Self::SourceStreamFailed { detail }
            | Self::StateTransitionFailed { detail }
            | Self::RecoveryFailed { detail } => detail.clone(),
            Self::SourceStreamClosed => "source stream closed; canonical recovery required".into(),
            Self::RecoveryStarted => "canonical recovery started".into(),
            Self::RecoveryCompleted => "canonical recovery completed".into(),
            Self::BackgroundTaskStopped { task } => {
                format!("background task `{task}` stopped before shutdown")
            }
            Self::ShutdownTimedOut => "graceful shutdown exceeded its deadline".into(),
            Self::BackgroundTaskPanicked { task, detail } => {
                format!("background task `{task}` panicked: {detail}")
            }
        }
    }
}
