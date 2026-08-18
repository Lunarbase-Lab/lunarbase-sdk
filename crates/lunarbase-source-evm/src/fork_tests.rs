use crate::fork::{CanonicalWindow, ForkError, ForkResolver, ForkWindowLimits};
use crate::rpc::backend::RpcHttpBackend;
use crate::rpc::client::RpcHttpClient;
use alloy_rpc_client::RpcClient;
use alloy_transport::mock::Asserter;
use lunarbase_client::model::{BlockRef, ChainCursor, Commitment, Network};
use lunarbase_math::B256;
use std::mem::size_of;

const CHAIN_ID: u64 = 97;

#[test]
fn common_append_is_bounded_and_does_not_prune_silently() {
    let charge = size_of::<BlockRef>();
    let mut count_window = CanonicalWindow::new(ForkWindowLimits {
        max_blocks: 2,
        max_bytes: charge * 4,
    })
    .unwrap();
    count_window.push_head(block(10, 10, 9)).unwrap();
    count_window.push_head(block(11, 11, 10)).unwrap();
    assert_eq!(
        count_window.push_head(block(12, 12, 11)),
        Err(ForkError::BlockBudget)
    );
    assert_eq!(count_window.len(), 2);
    assert_eq!(count_window.retained_bytes(), charge * 2);

    let mut byte_window = CanonicalWindow::new(ForkWindowLimits {
        max_blocks: 4,
        max_bytes: charge * 2,
    })
    .unwrap();
    byte_window.push_head(block(10, 10, 9)).unwrap();
    byte_window.push_head(block(11, 11, 10)).unwrap();
    assert_eq!(
        byte_window.push_head(block(12, 12, 11)),
        Err(ForkError::ByteBudget)
    );
    assert_eq!(byte_window.tip(), Some(&block(11, 11, 10)));
}

#[test]
fn prepared_heads_do_not_mutate_until_committed() {
    let mut window = window();
    let first = block(10, 10, 9);
    let second = block(11, 11, 10);
    window.push_head(first.clone()).unwrap();

    let append = window.prepare_head(second.clone()).unwrap();
    assert_eq!(window.tip(), Some(&first));
    assert!(window.commit_head(append));
    assert_eq!(window.tip(), Some(&second));

    let promoted = block_with_commitment(11, 11, 10, Commitment::Finalized);
    let replacement = window.prepare_progressive_tip(promoted.clone()).unwrap();
    assert_eq!(window.tip(), Some(&second));
    window.commit_progressive_tip(replacement);
    assert_eq!(window.tip(), Some(&promoted));
}

#[test]
fn finality_prunes_only_history_before_the_boundary() {
    let mut window = window();
    window.push_head(block(10, 10, 9)).unwrap();
    window.push_head(block(11, 11, 10)).unwrap();
    window.push_head(block(12, 12, 11)).unwrap();

    let finalized = block_with_commitment(11, 11, 10, Commitment::Finalized);
    window.advance_finalized(finalized.clone()).unwrap();

    assert_eq!(
        window.blocks().cloned().collect::<Vec<_>>(),
        [finalized, block(12, 12, 11)]
    );
    assert_eq!(
        window.finalized().map(|block| block.cursor.block_number),
        Some(11)
    );
}

#[tokio::test]
async fn direct_fork_resolves_without_an_http_lookup() {
    let asserter = Asserter::new();
    let resolver = resolver(asserter.clone(), 4);
    let mut window = window();
    window.push_head(block(10, 10, 9)).unwrap();
    window.push_head(block(11, 21, 10)).unwrap();

    let replacement = block(11, 31, 10);
    let resolution = resolver
        .resolve(&window, replacement.clone())
        .await
        .unwrap();

    assert_eq!(resolution.common_ancestor, block(10, 10, 9));
    assert_eq!(resolution.old_branch, [block(11, 21, 10)]);
    assert_eq!(resolution.new_branch, std::slice::from_ref(&replacement));
    assert!(asserter.read_q().is_empty());
    window.apply_resolution(&resolution).unwrap();
    assert_eq!(window.tip(), Some(&replacement));
}

#[tokio::test]
async fn deep_fork_walks_only_the_missing_replacement_branch() {
    let asserter = Asserter::new();
    let resolver = resolver(asserter.clone(), 4);
    asserter.push_success(&serde_json::json!("0x61"));
    asserter.push_success(&rpc_block(12, 42, 41));
    asserter.push_success(&rpc_block(11, 41, 10));
    let mut window = window();
    window.push_head(block(10, 10, 9)).unwrap();
    window.push_head(block(11, 21, 10)).unwrap();
    window.push_head(block(12, 22, 21)).unwrap();
    window.push_head(block(13, 23, 22)).unwrap();

    let resolution = resolver.resolve(&window, block(13, 43, 42)).await.unwrap();

    assert_eq!(resolution.common_ancestor, block(10, 10, 9));
    assert_eq!(
        resolution.old_branch,
        [block(11, 21, 10), block(12, 22, 21), block(13, 23, 22)]
    );
    assert_eq!(
        resolution.new_branch,
        [block(11, 41, 10), block(12, 42, 41), block(13, 43, 42)]
    );
    assert!(
        asserter.read_q().is_empty(),
        "resolver made an extra RPC request"
    );
}

#[tokio::test]
async fn fork_walk_rejects_a_foreign_http_chain_before_block_lookup() {
    let asserter = Asserter::new();
    let resolver = resolver(asserter.clone(), 4);
    asserter.push_success(&serde_json::json!("0x62"));
    asserter.push_failure_msg("block lookup must remain queued");
    let mut window = window();
    window.push_head(block(10, 10, 9)).unwrap();
    window.push_head(block(11, 21, 10)).unwrap();

    let error = resolver
        .resolve(&window, block(11, 31, 30))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("expected 97, got 98"));
    assert_eq!(asserter.read_q().len(), 1);
}

#[tokio::test]
async fn depth_and_finality_fail_closed_without_mutating_the_window() {
    let asserter = Asserter::new();
    let resolver = resolver(asserter.clone(), 2);
    asserter.push_success(&serde_json::json!("0x61"));
    asserter.push_success(&rpc_block(12, 42, 41));
    let mut window = window();
    window.push_head(block(10, 10, 9)).unwrap();
    window.push_head(block(11, 21, 10)).unwrap();
    window.push_head(block(12, 22, 21)).unwrap();
    window.push_head(block(13, 23, 22)).unwrap();
    let old_tip = window.tip().cloned();

    assert_eq!(
        resolver.resolve(&window, block(13, 43, 42)).await,
        Err(ForkError::DepthExceeded)
    );
    assert_eq!(window.tip().cloned(), old_tip);

    let finalized = block_with_commitment(10, 10, 9, Commitment::Finalized);
    window.advance_finalized(finalized).unwrap();
    assert_eq!(
        resolver.resolve(&window, block(11, 51, 50)).await,
        Err(ForkError::FinalizedConflict)
    );
}

#[tokio::test]
async fn stale_or_malformed_resolution_is_atomic() {
    let asserter = Asserter::new();
    let resolver = resolver(asserter, 4);
    let mut window = window();
    window.push_head(block(10, 10, 9)).unwrap();
    window.push_head(block(11, 21, 10)).unwrap();
    let mut resolution = resolver.resolve(&window, block(11, 31, 10)).await.unwrap();
    let old_blocks = window.blocks().cloned().collect::<Vec<_>>();
    resolution.new_branch[0].parent_hash = Some(hash(99));

    assert_eq!(
        window.apply_resolution(&resolution),
        Err(ForkError::Disconnected)
    );
    assert_eq!(window.blocks().cloned().collect::<Vec<_>>(), old_blocks);
}

fn resolver(asserter: Asserter, max_depth: usize) -> ForkResolver {
    let rpc = RpcHttpClient::from_client(RpcClient::mocked(asserter));
    let backend = RpcHttpBackend::new(rpc, Network::Evm, CHAIN_ID, "latest");
    ForkResolver::new(backend, max_depth).unwrap()
}

fn window() -> CanonicalWindow {
    CanonicalWindow::new(ForkWindowLimits::default()).unwrap()
}

fn block(number: u64, hash_byte: u8, parent_byte: u8) -> BlockRef {
    block_with_commitment(number, hash_byte, parent_byte, Commitment::Canonical)
}

fn block_with_commitment(
    number: u64,
    hash_byte: u8,
    parent_byte: u8,
    commitment: Commitment,
) -> BlockRef {
    BlockRef::new(
        ChainCursor::block(CHAIN_ID, number, Some(hash(hash_byte)), commitment),
        Some(hash(parent_byte)),
    )
}

fn hash(byte: u8) -> B256 {
    B256::new([byte; 32])
}

fn rpc_block(number: u64, hash_byte: u8, parent_byte: u8) -> serde_json::Value {
    serde_json::json!({
        "number": format!("0x{number:x}"),
        "hash": format!("{:#x}", hash(hash_byte)),
        "parentHash": format!("{:#x}", hash(parent_byte)),
    })
}
