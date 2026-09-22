//! `swale start`, `swale rerun` and `swale cancel`: the commands that write
//! a request.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use swale::records::RequestOutcome;
use swale::{OperatorSet, Partition, Request, RequestId, StatusReader};
use taquba::{Clock, QueueReader, ReaderMode, ReaderOptions, SystemClock};

use super::CommandResult;
use super::store::{StoreArg, open_store};

/// The arguments of a command that writes a request.
#[derive(clap::Args)]
pub(crate) struct RequestArgs {
    #[command(flatten)]
    pub(crate) store: StoreArg,
    /// Waits for the daemon to apply the request and prints the outcome.
    #[arg(long)]
    pub(crate) wait: bool,
}

/// Writes `request` to the store and, with `--wait`, prints its outcome
/// once the daemon applies it.
pub(crate) async fn send_request(request: Request, args: RequestArgs) -> CommandResult {
    let id = RequestId::generate(SystemClock.now_ms());
    let store = open_store(args.store)?;
    store.requests().submit(&id, &request).await?;
    if !args.wait {
        println!("request {id}: written, the daemon applies it at its next sync pass");
        return Ok(ExitCode::SUCCESS);
    }
    // The wait polls the record, so the reader refreshes its view every
    // second.
    let options = ReaderOptions::default()
        .mode(ReaderMode::FollowLatest)
        .manifest_poll_interval(Duration::from_secs(1));
    let reader = StatusReader::new(
        QueueReader::open_with_options(store.objects.clone(), &store.queue_path, options).await?,
        store.definitions(Arc::new(OperatorSet::builtin())),
    );
    let record = loop {
        if let Some(record) = reader.request(&id).await? {
            break record;
        }
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("interrupted, request {id} stays in the store");
                reader.close().await?;
                return Ok(ExitCode::from(130));
            }
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    };
    reader.close().await?;
    match record.outcome {
        RequestOutcome::Started { partitions } => {
            let keys: Vec<String> = partitions.iter().map(Partition::to_string).collect();
            if keys.is_empty() {
                println!("request {id}: every partition has a graph run");
            } else {
                println!("request {id}: started {}", keys.join(", "));
            }
        }
        RequestOutcome::Rerun { run_id } => println!("request {id}: submitted {run_id}"),
        RequestOutcome::Cancelled => println!("request {id}: cancelled"),
        RequestOutcome::Refused { reason } => {
            return Err(format!("request {id}: refused, {reason}").into());
        }
    }
    Ok(ExitCode::SUCCESS)
}
