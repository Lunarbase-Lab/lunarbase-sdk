/// Failure while composing the selected network source and common runtime.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Indexer(#[from] IndexerError),
    #[error("Redis startup failed: {0}")]
    Redis(String),
    #[cfg(not(all(feature = "base", feature = "monad", feature = "arbitrum")))]
    #[error("network `{0}` was not compiled into this binary")]
    FeatureDisabled(&'static str),
}

