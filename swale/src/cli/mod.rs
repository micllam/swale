//! The command line: the argument parser and the dispatch to each command.
//! [`store`] opens the store of `--store`, [`table`] prints a table, and
//! each of the other modules implements one command.

mod daemon;
mod publish;
mod queues;
mod request;
mod run;
mod status;
mod store;
mod table;
mod validate;

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use swale::partition::InvalidPartition;
use swale::{DefinitionError, Error, Partition, Request};

use self::request::RequestArgs;
use self::store::StoreArg;

/// The result of a command: its exit code, or the error printed on stderr.
pub(crate) type CommandResult = Result<ExitCode, Box<dyn std::error::Error>>;

/// A scheduled, dependency-ordered orchestrator of asset graphs.
#[derive(Parser)]
#[command(name = "swale", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Checks a definition file and reports every fault.
    Validate {
        /// The definition file.
        file: PathBuf,
    },
    /// Runs a graph for one partition on a store and waits for the run to
    /// settle. A second run for the same partition resumes the existing
    /// graph run.
    Run {
        /// The definition file.
        file: PathBuf,
        #[command(flatten)]
        store: StoreArg,
        /// The partition key. Required for a partitioned graph.
        #[arg(long, value_parser = parse_partition_arg)]
        partition: Option<Partition>,
        /// Steps run at a time in each pool.
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
    },
    /// Publishes a definition file as the current definition of its graph.
    /// The daemon on the store adopts it at its next sync pass.
    Publish {
        /// The definition file.
        file: PathBuf,
        #[command(flatten)]
        store: StoreArg,
    },
    /// Runs the published graphs on a store until interrupted: adopts the
    /// published definitions, fires each schedule and runs the task
    /// instances.
    Daemon {
        #[command(flatten)]
        store: StoreArg,
        /// Steps run at a time in the `default` pool.
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
        /// A further pool as `name=steps`. Repeat for each pool.
        #[arg(long = "pool", value_parser = daemon::parse_pool_arg)]
        pools: Vec<(String, usize)>,
        /// Seconds between sync passes over the published definitions.
        #[arg(long, default_value_t = 30)]
        sync_interval: u64,
    },
    /// Asks the daemon on a store to start the graph run of each partition.
    /// A partition with a graph run is unchanged.
    Start {
        /// The graph.
        graph: String,
        /// The partition keys.
        #[arg(required = true, value_parser = parse_partition_arg)]
        partitions: Vec<Partition>,
        #[command(flatten)]
        request: RequestArgs,
    },
    /// Asks the daemon on a store to run a node again. After a succeeded
    /// node, the nodes downstream of it through an all-succeeded edge run
    /// again with the new outputs.
    Rerun {
        /// The graph.
        graph: String,
        /// The partition key.
        #[arg(value_parser = parse_partition_arg)]
        partition: Partition,
        /// The node.
        node: String,
        #[command(flatten)]
        request: RequestArgs,
    },
    /// Asks the daemon on a store to cancel an active graph run.
    Cancel {
        /// The graph.
        graph: String,
        /// The partition key.
        #[arg(value_parser = parse_partition_arg)]
        partition: Partition,
        #[command(flatten)]
        request: RequestArgs,
    },
    /// Prints the graphs of a store, the graph runs of a graph, or the
    /// nodes of one graph run. The command only reads from the store, so
    /// it can run alongside a daemon.
    Status {
        /// The graph. Without it, the command lists every graph.
        graph: Option<String>,
        /// The partition key of one graph run. Without it, the command
        /// lists the graph runs of the graph.
        #[arg(value_parser = parse_partition_arg)]
        partition: Option<Partition>,
        #[command(flatten)]
        store: StoreArg,
        /// The graph runs listed, the latest partitions of the graph.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Prints the job counts of every queue of a store, or the dead jobs
    /// of one queue. The command only reads from the store.
    Queues {
        /// The queue. Without it, the command lists every queue.
        queue: Option<String>,
        #[command(flatten)]
        store: StoreArg,
        /// The dead jobs listed, in enqueue order.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
}

pub(crate) async fn main() -> ExitCode {
    let result = match Cli::parse().command {
        Command::Validate { file } => validate::validate(&file),
        Command::Run {
            file,
            store,
            partition,
            concurrency,
        } => run::run(&file, store, partition, concurrency).await,
        Command::Publish { file, store } => publish::publish(&file, store).await,
        Command::Daemon {
            store,
            concurrency,
            pools,
            sync_interval,
        } => {
            daemon::daemon(
                store,
                concurrency,
                pools,
                Duration::from_secs(sync_interval),
            )
            .await
        }
        Command::Start {
            graph,
            partitions,
            request: args,
        } => request::send_request(Request::Start { graph, partitions }, args).await,
        Command::Rerun {
            graph,
            partition,
            node,
            request: args,
        } => {
            request::send_request(
                Request::Rerun {
                    graph,
                    partition,
                    node,
                },
                args,
            )
            .await
        }
        Command::Cancel {
            graph,
            partition,
            request: args,
        } => request::send_request(Request::Cancel { graph, partition }, args).await,
        Command::Status {
            graph,
            partition,
            store,
            limit,
        } => status::status(store, graph, partition, limit).await,
        Command::Queues {
            queue,
            store,
            limit,
        } => queues::queues(store, queue, limit).await,
    };
    match result {
        Ok(code) => code,
        Err(e) => {
            print_error(e.as_ref());
            ExitCode::FAILURE
        }
    }
}

/// Prints `error` on stderr: one `error:` line per problem of an invalid
/// definition, and one line for any other error.
fn print_error(error: &(dyn std::error::Error + 'static)) {
    let problems = match error.downcast_ref::<Error>() {
        Some(Error::Invalid(problems)) => Some(problems),
        _ => match error.downcast_ref::<DefinitionError>() {
            Some(DefinitionError::Invalid(Error::Invalid(problems))) => Some(problems),
            _ => None,
        },
    };
    match problems {
        Some(problems) => {
            for problem in problems {
                eprintln!("error: {problem}");
            }
        }
        None => eprintln!("error: {error}"),
    }
}

/// Parses a partition key argument.
pub(crate) fn parse_partition_arg(raw: &str) -> Result<Partition, InvalidPartition> {
    Partition::new(raw)
}
