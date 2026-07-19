use clap::Parser;
use lunarbase_tools::support::e2e::environment::E2eArguments;
use lunarbase_tools::support::e2e::scenarios::run;

#[tokio::main]
async fn main() {
    if let Err(error) = run(E2eArguments::parse()).await {
        eprintln!("lunarbase-e2e failed: {error}");
        std::process::exit(1);
    }
}
