use clap::Parser;
use lunarbase_tools::support::load::{LoadArguments, run};

#[tokio::main]
async fn main() {
    if let Err(error) = run(LoadArguments::parse()).await {
        eprintln!("lunarbase-load failed: {error}");
        std::process::exit(1);
    }
}
