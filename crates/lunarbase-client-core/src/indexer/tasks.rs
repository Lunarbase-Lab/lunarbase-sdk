async fn source_pump(
    source: Arc<dyn ChainEventSource>,
    filter: ContractFilter,
    sender: mpsc::Sender<ChainUpdate>,
    reconnect_delay: Duration,
    mut cancel: watch::Receiver<bool>,
    runtime_events: broadcast::Sender<ClientRuntimeEvent>,
    stats: Arc<ClientRuntimeStats>,
) {
    loop {
        let stream = match tokio::select! {
            biased;
            () = cancellation_requested(&mut cancel) => return,
            result = source.subscribe(filter.clone()) => result,
        } {
            Ok(stream) => stream,
            Err(error) => {
                stats.source_reconnects.fetch_add(1, Ordering::Relaxed);
                let detail = error.to_string();
                publish_runtime_event(
                    &runtime_events,
                    ClientRuntimeEvent::SourceSubscribeFailed {
                        detail: detail.clone(),
                    },
                );
                if !send_update(
                    &sender,
                    &mut cancel,
                    ChainUpdate::Gap {
                        cursor: None,
                        reason: format!("source subscribe failed: {detail}"),
                    },
                    &stats,
                )
                .await
                {
                    break;
                }
                if sleep_or_cancel(reconnect_delay, &mut cancel).await {
                    break;
                }
                continue;
            }
        };
        futures_util::pin_mut!(stream);
        let mut ended_with_gap = false;
        loop {
            let item = tokio::select! {
                biased;
                () = cancellation_requested(&mut cancel) => return,
                item = stream.next() => item,
            };
            let Some(item) = item else {
                break;
            };
            let update = match item {
                Ok(update) => update,
                Err(error) => {
                    let detail = error.to_string();
                    publish_runtime_event(
                        &runtime_events,
                        ClientRuntimeEvent::SourceStreamFailed {
                            detail: detail.clone(),
                        },
                    );
                    ChainUpdate::Gap {
                        cursor: None,
                        reason: format!("source stream failed: {detail}"),
                    }
                }
            };
            let terminal = matches!(&update, ChainUpdate::Gap { .. });
            if !send_update(&sender, &mut cancel, update, &stats).await {
                break;
            }
            if terminal {
                ended_with_gap = true;
                break;
            }
        }
        if cancellation_is_requested(&cancel) {
            return;
        }
        if ended_with_gap {
            stats.source_reconnects.fetch_add(1, Ordering::Relaxed);
        }
        if !ended_with_gap {
            stats.source_reconnects.fetch_add(1, Ordering::Relaxed);
            publish_runtime_event(&runtime_events, ClientRuntimeEvent::SourceStreamClosed);
            if !send_update(
                &sender,
                &mut cancel,
                ChainUpdate::Gap {
                    cursor: None,
                    reason: "source stream closed; canonical recovery required".into(),
                },
                &stats,
            )
            .await
            {
                break;
            }
        }
        if sleep_or_cancel(reconnect_delay, &mut cancel).await {
            return;
        }
    }
    if !cancellation_is_requested(&cancel) {
        publish_runtime_event(
            &runtime_events,
            ClientRuntimeEvent::BackgroundTaskStopped {
                task: "source-pump",
            },
        );
    }
}

struct ReducerLoopContext {
    indexer: Arc<Mutex<QuoteIndexer>>,
    source: Arc<dyn ChainEventSource>,
    filter: ContractFilter,
    ready: Arc<Notify>,
    available: Arc<AtomicBool>,
    handoff_block: u64,
    checkpoint_store: Option<SharedCheckpointStore>,
    runtime_events: broadcast::Sender<ClientRuntimeEvent>,
    stats: Arc<ClientRuntimeStats>,
}

async fn source_reducer_loop(
    context: ReducerLoopContext,
    updates: &mut mpsc::Receiver<ChainUpdate>,
    mut cancel: watch::Receiver<bool>,
) {
    loop {
        let update = tokio::select! {
            biased;
            () = cancellation_requested(&mut cancel) => break,
            update = updates.recv() => update,
        };
        let Some(update) = update else {
            if !cancellation_is_requested(&cancel) {
                publish_runtime_event(
                    &context.runtime_events,
                    ClientRuntimeEvent::BackgroundTaskStopped { task: "reducer" },
                );
            }
            break;
        };
        context.stats.queue_depth.fetch_sub(1, Ordering::Relaxed);
        let skip = match &update {
            ChainUpdate::Log(log) => log.cursor.block_number <= context.handoff_block,
            ChainUpdate::Head(cursor) => cursor.block_number < context.handoff_block,
            _ => false,
        };
        if skip {
            continue;
        }
        tokio::select! {
            biased;
            () = cancellation_requested(&mut cancel) => break,
            () = process_reducer_update(&context, update) => {}
        }
    }
    context.available.store(false, Ordering::Release);
    context.indexer.lock().await.reducer.mark_not_ready();
    context.ready.notify_waiters();
}

async fn process_reducer_update(context: &ReducerLoopContext, update: ChainUpdate) {
    let discontinuity = matches!(&update, ChainUpdate::Gap { .. } | ChainUpdate::Reorg { .. });
    if discontinuity {
        context.stats.gaps.fetch_add(1, Ordering::Relaxed);
        context.available.store(false, Ordering::Release);
    }
    let persisted_update = update.clone();
    let apply_result = {
        let mut indexer = context.indexer.lock().await;
        indexer.apply_core_update(update)
    };
    if apply_result.is_ok() {
        let persisted = persist_checkpoint(
            &context.indexer,
            context.checkpoint_store.as_ref(),
            vec![persisted_update],
            &context.stats,
        )
        .await;
        if persisted.is_ok() {
            let ready = context.indexer.lock().await.reducer.is_ready();
            context.available.store(ready, Ordering::Release);
            context.ready.notify_waiters();
        } else {
            publish_runtime_event(
                &context.runtime_events,
                ClientRuntimeEvent::CheckpointFailed {
                    detail: persisted.expect_err("persistence result was checked as an error"),
                },
            );
            context.available.store(false, Ordering::Release);
            context.indexer.lock().await.reducer.mark_not_ready();
        }
        return;
    }
    let apply_error = apply_result.expect_err("apply result was checked as an error");
    context.available.store(false, Ordering::Release);
    publish_runtime_event(
        &context.runtime_events,
        ClientRuntimeEvent::StateTransitionFailed {
            detail: apply_error.to_string(),
        },
    );

    let recovery = {
        let mut indexer = context.indexer.lock().await;
        indexer
            .recover_from_source(context.source.as_ref(), context.filter.clone())
            .await
    };
    match recovery {
        Ok(()) => {
            match persist_checkpoint(
                &context.indexer,
                context.checkpoint_store.as_ref(),
                Vec::new(),
                &context.stats,
            )
            .await
            {
                Ok(()) => {
                    context.stats.recoveries.fetch_add(1, Ordering::Relaxed);
                    context.available.store(true, Ordering::Release);
                    context.ready.notify_waiters();
                }
                Err(error) => {
                    publish_runtime_event(
                        &context.runtime_events,
                        ClientRuntimeEvent::CheckpointFailed { detail: error },
                    );
                    context.available.store(false, Ordering::Release);
                    context.indexer.lock().await.reducer.mark_not_ready();
                }
            }
        }
        Err(error) => {
            context
                .stats
                .recovery_failures
                .fetch_add(1, Ordering::Relaxed);
            publish_runtime_event(
                &context.runtime_events,
                ClientRuntimeEvent::RecoveryFailed {
                    detail: error.to_string(),
                },
            );
            context.indexer.lock().await.reducer.mark_not_ready();
        }
    }
}

struct AbortOnDrop {
    handle: Option<JoinHandle<()>>,
}

impl AbortOnDrop {
    fn new(handle: JoinHandle<()>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn disarm(mut self) -> JoinHandle<()> {
        self.handle
            .take()
            .expect("abort-on-drop task handle must exist")
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

fn publish_runtime_event(
    sender: &broadcast::Sender<ClientRuntimeEvent>,
    event: ClientRuntimeEvent,
) {
    let _ = sender.send(event);
}

fn cancellation_is_requested(cancel: &watch::Receiver<bool>) -> bool {
    *cancel.borrow()
}

async fn cancellation_requested(cancel: &mut watch::Receiver<bool>) {
    if cancellation_is_requested(cancel) {
        return;
    }
    loop {
        if cancel.changed().await.is_err() || cancellation_is_requested(cancel) {
            return;
        }
    }
}

async fn sleep_or_cancel(delay: Duration, cancel: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        biased;
        () = cancellation_requested(cancel) => true,
        () = sleep(delay) => false,
    }
}

async fn send_update(
    sender: &mpsc::Sender<ChainUpdate>,
    cancel: &mut watch::Receiver<bool>,
    update: ChainUpdate,
    stats: &ClientRuntimeStats,
) -> bool {
    let permit = tokio::select! {
        biased;
        () = cancellation_requested(cancel) => return false,
        result = sender.reserve() => result,
    };
    match permit {
        Ok(permit) => {
            stats.queue_depth.fetch_add(1, Ordering::Relaxed);
            permit.send(update);
            true
        }
        Err(_) => false,
    }
}

fn collect_join_failure(
    task: &'static str,
    result: Option<Result<(), tokio::task::JoinError>>,
    runtime_events: &broadcast::Sender<ClientRuntimeEvent>,
    failures: &mut Vec<String>,
) {
    let Some(Err(error)) = result else {
        return;
    };
    let detail = error.to_string();
    if error.is_panic() {
        publish_runtime_event(
            runtime_events,
            ClientRuntimeEvent::BackgroundTaskPanicked {
                task,
                detail: detail.clone(),
            },
        );
    } else {
        publish_runtime_event(
            runtime_events,
            ClientRuntimeEvent::BackgroundTaskStopped { task },
        );
    }
    failures.push(format!("background task `{task}` failed: {detail}"));
}

fn remaining_timeout(started_at: Instant, deadline: Duration) -> Duration {
    deadline.saturating_sub(started_at.elapsed())
}
