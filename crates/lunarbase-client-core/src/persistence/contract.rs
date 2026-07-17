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

