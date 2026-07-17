fn forward_client_event(
    handle: &RuntimeHandle,
    event: Result<lunarbase_client_core::ClientRuntimeEvent, broadcast::error::RecvError>,
) {
    match event {
        Ok(event) => handle.publish(ServiceRuntimeEvent::Client(event)),
        Err(broadcast::error::RecvError::Lagged(skipped)) => {
            handle.publish(ServiceRuntimeEvent::RuntimeEventsLagged { skipped });
        }
        Err(broadcast::error::RecvError::Closed) => {}
    }
}

async fn release_after_failed_start(
    config: &ValidatedConfig,
    store: &SharedCheckpointStore,
    handle: &RuntimeHandle,
) {
    if let Err(detail) = lease_release(store, &config.writer_lease.owner).await {
        handle.publish(ServiceRuntimeEvent::LeaseReleaseFailed { detail });
    }
    let _ = configure_lease_fencing(store, None).await;
}

async fn configure_lease_fencing(
    store: &SharedCheckpointStore,
    owner: Option<&str>,
) -> Result<(), RuntimeError> {
    let store = Arc::clone(store);
    let owner = owner.map(str::to_owned);
    tokio::task::spawn_blocking(move || {
        store
            .blocking_lock_owned()
            .configure_writer_lease(owner.as_deref());
    })
    .await
    .map_err(|error| RuntimeError::Redis(format!("lease fencing worker failed: {error}")))
}

async fn lease_acquire(
    store: &SharedCheckpointStore,
    owner: &str,
    ttl: Duration,
) -> Result<bool, String> {
    lease_operation(store, owner, Some(ttl), LeaseOperation::Acquire).await
}

async fn lease_renew(
    store: &SharedCheckpointStore,
    owner: &str,
    ttl: Duration,
) -> Result<bool, String> {
    lease_operation(store, owner, Some(ttl), LeaseOperation::Renew).await
}

async fn lease_release(store: &SharedCheckpointStore, owner: &str) -> Result<(), String> {
    lease_operation(store, owner, None, LeaseOperation::Release)
        .await
        .map(|_| ())
}

#[derive(Clone, Copy)]
enum LeaseOperation {
    Acquire,
    Renew,
    Release,
}

async fn lease_operation(
    store: &SharedCheckpointStore,
    owner: &str,
    ttl: Option<Duration>,
    operation: LeaseOperation,
) -> Result<bool, String> {
    let store = Arc::clone(store);
    let owner = owner.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut store = store.blocking_lock_owned();
        match operation {
            LeaseOperation::Acquire => store
                .acquire_writer_lease(&owner, ttl.expect("acquire lease operation requires a TTL")),
            LeaseOperation::Renew => {
                store.renew_writer_lease(&owner, ttl.expect("renew lease operation requires a TTL"))
            }
            LeaseOperation::Release => {
                store.release_writer_lease(&owner)?;
                Ok(true)
            }
        }
    })
    .await
    .map_err(|error| format!("writer lease worker failed: {error}"))?
}

async fn sleep_or_shutdown(delay: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        biased;
        () = shutdown_requested(shutdown) => true,
        () = sleep(delay) => false,
    }
}

fn shutdown_is_requested(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

async fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) {
    if shutdown_is_requested(shutdown) {
        return;
    }
    loop {
        if shutdown.changed().await.is_err() || shutdown_is_requested(shutdown) {
            return;
        }
    }
}

