//! Benchmarks fresh release indexer processes across deterministic quote scenarios.

use clap::Parser;
use lunarbase_tools::support::e2e::benchmark::{IndexerBenchmarkArguments, run};

#[tokio::main]
async fn main() {
    if let Err(error) = run(IndexerBenchmarkArguments::parse()).await {
        eprintln!("lunarbase-indexer-bench failed: {error}");
        std::process::exit(1);
    }
}
