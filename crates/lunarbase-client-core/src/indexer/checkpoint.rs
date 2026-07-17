async fn persist_checkpoint(
    indexer: &Arc<Mutex<QuoteIndexer>>,
    store: Option<&SharedCheckpointStore>,
    updates: Vec<ChainUpdate>,
    stats: &ClientRuntimeStats,
) -> Result<(), String> {
    let Some(store) = store else {
        return Ok(());
    };
    let checkpoint = indexer
        .lock()
        .await
        .checkpoint()
        .ok_or("cannot persist checkpoint without a cursor")?;
    commit_checkpoint(store, checkpoint, updates, stats).await
}

async fn commit_checkpoint(
    store: &SharedCheckpointStore,
    checkpoint: Checkpoint,
    updates: Vec<ChainUpdate>,
    stats: &ClientRuntimeStats,
) -> Result<(), String> {
    let started_at = Instant::now();
    let store = Arc::clone(store);
    let result = tokio::task::spawn_blocking(move || {
        store.blocking_lock_owned().commit(checkpoint, updates)
    })
    .await
    .map_err(|error| format!("checkpoint worker failed: {error}"))?;
    stats.checkpoint_latency_nanoseconds.fetch_add(
        u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    match result {
        Ok(()) => {
            stats.checkpoint_commits.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(error) => {
            stats.checkpoint_failures.fetch_add(1, Ordering::Relaxed);
            Err(error)
        }
    }
}

fn update_order(update: &ChainUpdate) -> (u64, u32, u32, u8) {
    match update {
        ChainUpdate::Log(log) => {
            let (block, tx, log_index) = log.cursor.event_order();
            (block, tx, log_index, 1)
        }
        ChainUpdate::Head(cursor) => {
            let (block, tx, log_index) = cursor.event_order();
            (block, tx, log_index, 0)
        }
        ChainUpdate::Reorg { new_head, .. } => {
            let (block, tx, log_index) = new_head.event_order();
            (block, tx, log_index, 2)
        }
        ChainUpdate::Gap { cursor, .. } => cursor.as_ref().map_or((u64::MAX, 0, 0, 3), |cursor| {
            let (block, tx, log_index) = cursor.event_order();
            (block, tx, log_index, 3)
        }),
        ChainUpdate::SourceHealth { .. } => (0, 0, 0, 0),
    }
}
