//! Self-contained process-level E2E harness for the real indexer binary.

pub(super) const CORE: &str = "0x0000000000000000000000000000000000000010";
pub(super) const CASH: &str = "0x0000000000000000000000000000000000000001";
pub(super) const ASSET: &str = "0x0000000000000000000000000000000000000002";
pub(super) const ROUTER: &str = "0x0000000000000000000000000000000000000003";
pub(super) const EMPTY_CODE_HASH: &str =
    "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470";

mod assertions;
/// Process fixtures, mock chain, Redis process, and temporary workspace.
pub mod environment;
mod helpers;
mod process;
mod rpc_mock;
/// End-to-end recovery, shutdown, and multi-replica scenarios.
pub mod scenarios;
mod websocket_mock;
