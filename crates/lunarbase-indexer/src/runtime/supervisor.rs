/// Runs active/standby election until shutdown. Losing or failing to renew the
/// lease clears the active client before any slow cleanup begins.
pub async fn supervise(
    config: &ValidatedConfig,
    store: Option<SharedCheckpointStore>,
    handle: RuntimeHandle,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    if !config.writer_lease.enabled {
        let Some(client) = connect_or_shutdown(config, store, &mut shutdown).await? else {
            handle
                .transition(
                    RuntimeRole::Stopping,
                    "shutdown requested during writer initialization",
                    None,
                )
                .await;
            return Ok(());
        };
        let client = Arc::new(client);
        return supervise_unleased(config, handle, client, &mut shutdown).await;
    }
    let store = store.ok_or_else(|| {
        RuntimeError::Redis("writer lease enabled without a checkpoint store".into())
    })?;

    handle
        .transition(
            RuntimeRole::Standby,
            "waiting to acquire the Redis writer lease",
            None,
        )
        .await;
    loop {
        if shutdown_is_requested(&shutdown) {
            handle
                .transition(RuntimeRole::Stopping, "shutdown requested", None)
                .await;
            return Ok(());
        }
        match lease_acquire(&store, &config.writer_lease.owner, config.writer_lease.ttl).await {
            Ok(true) => {
                configure_lease_fencing(&store, Some(&config.writer_lease.owner)).await?;
                handle.publish(ServiceRuntimeEvent::LeaseAcquired);
                match connect_or_shutdown(config, Some(store.clone()), &mut shutdown).await {
                    Ok(Some(client)) => {
                        let client = Arc::new(client);
                        let keep_running =
                            supervise_leased(config, &store, &handle, client, &mut shutdown)
                                .await?;
                        if !keep_running {
                            return Ok(());
                        }
                    }
                    Ok(None) => {
                        handle
                            .transition(
                                RuntimeRole::Stopping,
                                "shutdown requested during writer initialization",
                                None,
                            )
                            .await;
                        release_after_failed_start(config, &store, &handle).await;
                        return Ok(());
                    }
                    Err(error) => {
                        let detail = error.to_string();
                        handle.publish(ServiceRuntimeEvent::RuntimeConnectFailed {
                            detail: detail.clone(),
                        });
                        handle
                            .transition(
                                RuntimeRole::Standby,
                                format!("writer initialization failed; waiting to retry: {detail}"),
                                None,
                            )
                            .await;
                        release_after_failed_start(config, &store, &handle).await;
                    }
                }
            }
            Ok(false) => {
                handle
                    .transition(
                        RuntimeRole::Standby,
                        "another replica owns the Redis writer lease",
                        None,
                    )
                    .await;
            }
            Err(detail) => {
                handle.publish(ServiceRuntimeEvent::LeaseAcquireFailed {
                    detail: detail.clone(),
                });
                handle
                    .transition(
                        RuntimeRole::Standby,
                        format!("writer lease acquisition failed: {detail}"),
                        None,
                    )
                    .await;
            }
        }
        if sleep_or_shutdown(config.writer_lease.retry_interval, &mut shutdown).await {
            handle
                .transition(RuntimeRole::Stopping, "shutdown requested", None)
                .await;
            return Ok(());
        }
    }
}

async fn connect_or_shutdown(
    config: &ValidatedConfig,
    store: Option<SharedCheckpointStore>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<ConnectedQuoteClient>, RuntimeError> {
    tokio::select! {
        biased;
        () = shutdown_requested(shutdown) => Ok(None),
        result = connect(config, store) => result.map(Some),
    }
}

async fn supervise_unleased(
    config: &ValidatedConfig,
    handle: RuntimeHandle,
    client: Arc<ConnectedQuoteClient>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    let mut client_events = client.subscribe_runtime_events();
    handle
        .transition(
            RuntimeRole::Active,
            "active writer; distributed lease is disabled",
            Some(client.clone()),
        )
        .await;
    loop {
        tokio::select! {
            biased;
            () = shutdown_requested(shutdown) => break,
            event = client_events.recv() => forward_client_event(&handle, event),
        }
    }
    handle
        .transition(RuntimeRole::Stopping, "shutdown requested", None)
        .await;
    client
        .shutdown_gracefully(config.shutdown_timeout)
        .await
        .map_err(RuntimeError::from)
}

/// Returns `false` for process shutdown and `true` when the replica should
/// return to standby after lease loss.
async fn supervise_leased(
    config: &ValidatedConfig,
    store: &SharedCheckpointStore,
    handle: &RuntimeHandle,
    client: Arc<ConnectedQuoteClient>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<bool, RuntimeError> {
    let mut renew = interval(config.writer_lease.renew_interval);
    renew.set_missed_tick_behavior(MissedTickBehavior::Delay);
    renew.tick().await;
    let mut client_events = client.subscribe_runtime_events();
    handle
        .transition(
            RuntimeRole::Active,
            "this replica owns the Redis writer lease",
            Some(client.clone()),
        )
        .await;

    loop {
        tokio::select! {
            biased;
            () = shutdown_requested(shutdown) => {
                handle.transition(RuntimeRole::Stopping, "shutdown requested", None).await;
                let shutdown_result = client.shutdown_gracefully(config.shutdown_timeout).await;
                let release_result = lease_release(store, &config.writer_lease.owner).await;
                if let Err(detail) = release_result {
                    handle.publish(ServiceRuntimeEvent::LeaseReleaseFailed {
                        detail: detail.clone(),
                    });
                    return Err(RuntimeError::Redis(detail));
                }
                configure_lease_fencing(store, None).await?;
                shutdown_result.map_err(RuntimeError::from)?;
                return Ok(false);
            }
            _ = renew.tick() => {
                match lease_renew(store, &config.writer_lease.owner, config.writer_lease.ttl).await {
                    Ok(true) => {}
                    Ok(false) => {
                        handle.transition(
                            RuntimeRole::LeaseLost,
                            "Redis reports that this replica no longer owns the writer lease",
                            None,
                        ).await;
                        handle.publish(ServiceRuntimeEvent::LeaseLost);
                        let _ = client
                            .shutdown_after_lease_loss(config.shutdown_timeout)
                            .await;
                        configure_lease_fencing(store, None).await?;
                        handle.transition(
                            RuntimeRole::Standby,
                            "writer stopped after lease loss; waiting to reacquire",
                            None,
                        ).await;
                        return Ok(true);
                    }
                    Err(detail) => {
                        handle.transition(
                            RuntimeRole::LeaseLost,
                            format!("writer lease renewal failed: {detail}"),
                            None,
                        ).await;
                        handle.publish(ServiceRuntimeEvent::LeaseRenewFailed {
                            detail,
                        });
                        let _ = client
                            .shutdown_after_lease_loss(config.shutdown_timeout)
                            .await;
                        configure_lease_fencing(store, None).await?;
                        handle.transition(
                            RuntimeRole::Standby,
                            "writer stopped after lease renewal failure; waiting to reacquire",
                            None,
                        ).await;
                        return Ok(true);
                    }
                }
            }
            event = client_events.recv() => forward_client_event(handle, event),
        }
    }
}

