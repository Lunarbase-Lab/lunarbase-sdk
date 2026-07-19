use clap::Parser;
use lunarbase_tools::support::monad::runner::run;
use lunarbase_tools::support::monad::types::MonadArguments;

#[tokio::main]
async fn main() {
    if let Err(error) = run(MonadArguments::parse()).await {
        eprintln!("lunarbase-monad-validate failed: {error}");
        std::process::exit(1);
    }
}
