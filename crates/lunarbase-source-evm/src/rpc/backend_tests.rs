use super::{RpcHttpBackend, VerificationHook};
use crate::rpc::client::RpcHttpClient;
use alloy_rpc_client::RpcClient;
use alloy_transport::mock::Asserter;
use lunarbase_client::model::Network;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

fn backend(asserter: &Asserter) -> RpcHttpBackend {
    RpcHttpBackend::new(
        RpcHttpClient::from_client(RpcClient::mocked(asserter.clone())),
        Network::Evm,
        97,
        "latest",
    )
}

async fn wait_for_pending(backend: &RpcHttpBackend, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while backend.chain_verification.pending.load(Ordering::Acquire) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pending verification count did not converge");
}

#[tokio::test]
async fn cancelled_waiting_verification_invalidates_the_cached_session() {
    let asserter = Asserter::new();
    let backend = backend(&asserter);
    backend
        .chain_verification
        .verified
        .store(true, Ordering::Release);
    let task_backend = backend.clone();
    let singleflight = backend.chain_verification.singleflight.lock().await;
    let task = tokio::spawn(async move { task_backend.verify_chain_id().await });

    wait_for_pending(&backend, 1).await;
    assert!(!backend.chain_verification.is_idle_and_verified());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(singleflight);

    assert_eq!(
        backend.chain_verification.pending.load(Ordering::Acquire),
        0
    );
    assert!(!backend.chain_verification.verified.load(Ordering::Acquire));
    assert!(asserter.read_q().is_empty());
}

#[tokio::test]
async fn ensure_does_not_publish_while_an_explicit_verification_is_pending() {
    let asserter = Asserter::new();
    asserter.push_success(&serde_json::json!("0x61"));
    asserter.push_success(&serde_json::json!("0x61"));
    let mut backend = backend(&asserter);
    let hook = Arc::new(VerificationHook::new());
    backend.verification_hook = Some(hook.clone());

    let ensure_backend = backend.clone();
    let ensure = tokio::spawn(async move { ensure_backend.ensure_chain_id().await });
    hook.started.notified().await;

    let verify_backend = backend.clone();
    let verify = tokio::spawn(async move { verify_backend.verify_chain_id().await });
    wait_for_pending(&backend, 1).await;
    hook.proceed.add_permits(1);
    ensure.await.unwrap().unwrap();

    assert!(!backend.chain_verification.verified.load(Ordering::Acquire));
    assert_eq!(
        backend.chain_verification.pending.load(Ordering::Acquire),
        1
    );
    hook.started.notified().await;
    hook.proceed.add_permits(1);
    verify.await.unwrap().unwrap();

    assert_eq!(
        backend.chain_verification.pending.load(Ordering::Acquire),
        0
    );
    assert!(backend.chain_verification.is_idle_and_verified());
    assert!(asserter.read_q().is_empty());
}
