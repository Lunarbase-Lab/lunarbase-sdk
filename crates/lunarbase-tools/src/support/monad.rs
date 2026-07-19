//! Live Monad parser/indexer/RPC soak validation.

mod comparison;
mod helpers;
mod parser_monitor;
/// Soak-test orchestration and report generation.
pub mod runner;
/// CLI configuration, validation vectors, errors, and report DTOs.
pub mod types;
