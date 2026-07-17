#[derive(Clone, Debug, Error, Eq, PartialEq)]
/// Errors returned while bootstrapping, recovering, or quoting from the
/// stateful client.
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
    #[error("requested freshness cannot be proven")]
    FreshnessUnavailable,
    #[error("no canonical cursor")]
    NoCursor,
}

/// Operational event emitted by the asynchronous client lifecycle.
///
/// Events are intentionally bounded and lossy: correctness continues to be
/// enforced by reducer readiness, while observability consumers can subscribe
/// without applying backpressure to the hot indexing path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientRuntimeEvent {
    SourceSubscribeFailed { detail: String },
    SourceStreamFailed { detail: String },
    SourceStreamClosed,
    StateTransitionFailed { detail: String },
    RecoveryFailed { detail: String },
    CheckpointFailed { detail: String },
    BackgroundTaskStopped { task: &'static str },
    ShutdownTimedOut,
    BackgroundTaskPanicked { task: &'static str, detail: String },
}

impl ClientRuntimeEvent {
    /// Stable machine-readable code suitable for metrics and alert routing.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::SourceSubscribeFailed { .. } => "source_subscribe_failed",
            Self::SourceStreamFailed { .. } => "source_stream_failed",
            Self::SourceStreamClosed => "source_stream_closed",
            Self::StateTransitionFailed { .. } => "state_transition_failed",
            Self::RecoveryFailed { .. } => "recovery_failed",
            Self::CheckpointFailed { .. } => "checkpoint_failed",
            Self::BackgroundTaskStopped { .. } => "background_task_stopped",
            Self::ShutdownTimedOut => "shutdown_timed_out",
            Self::BackgroundTaskPanicked { .. } => "background_task_panicked",
        }
    }

    /// Human-readable detail for structured logs and webhook alerts.
    pub fn detail(&self) -> String {
        match self {
            Self::SourceSubscribeFailed { detail }
            | Self::SourceStreamFailed { detail }
            | Self::StateTransitionFailed { detail }
            | Self::RecoveryFailed { detail }
            | Self::CheckpointFailed { detail } => detail.clone(),
            Self::SourceStreamClosed => {
                "source stream closed before shutdown; canonical recovery is required".into()
            }
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
