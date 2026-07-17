use crate::{LogDecodeError, ReducerError, SourceError};
use lunarbase_math::QuoteError;
use thiserror::Error;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Bootstrap, recovery, or quote failure.
pub enum IndexerError {
    #[error("not ready")]
    NotReady,
    #[error("source gap: {0}")]
    Gap(String),
    #[error(transparent)]
    Reducer(#[from] ReducerError),
    #[error(transparent)]
    Quote(#[from] QuoteError),
    #[error(transparent)]
    Decode(#[from] LogDecodeError),
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("runtime code hash mismatch")]
    CodeHashMismatch,
    #[error("no canonical cursor")]
    NoCursor,
    #[error("runtime state lock was poisoned")]
    LockPoisoned,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Bounded operational event used by tracing and metrics.
pub enum ClientRuntimeEvent {
    SourceSubscribeFailed { detail: String },
    SourceStreamFailed { detail: String },
    SourceStreamClosed,
    StateTransitionFailed { detail: String },
    RecoveryStarted,
    RecoveryCompleted,
    RecoveryFailed { detail: String },
    BackgroundTaskStopped { task: &'static str },
    ShutdownTimedOut,
    BackgroundTaskPanicked { task: &'static str, detail: String },
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
