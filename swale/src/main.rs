//! The `swale` binary.

mod cli;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    cli::main().await
}
