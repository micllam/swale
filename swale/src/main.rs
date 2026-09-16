use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use swale::records::{GraphRunRecord, GraphRunState, NodeRecord, graph_run_key, node_record_key};
use swale::{
    DefinitionStore, EVENTS_QUEUE, Error, OperatorSet, Partition, Partitioning, Pools, RecordHook,
    Scheduler, SchedulerOptions, definition,
};
use taquba::Queue;
use taquba::object_store::local::LocalFileSystem;
use taquba::object_store::path::Path as ObjectPath;
use taquba::object_store::{ObjectStore, parse_url};
use tokio_util::sync::CancellationToken;

/// The SlateDB path of the queue within the store.
const QUEUE_PATH: &str = "swale";

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
        /// The store: a directory, created when absent, or an object store
        /// URL (`s3://bucket/prefix`, `gs://bucket/prefix`,
        /// `az://container/prefix`, `file:///path`). A cloud scheme needs
        /// the matching cargo feature and reads the provider's environment
        /// variables for its credentials.
        #[arg(long, value_parser = parse_store_arg)]
        store: String,
        /// The partition key. Required for a partitioned graph.
        #[arg(long)]
        partition: Option<String>,
        /// Steps run at a time in each pool.
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Validate { file } => validate(&file),
        Command::Run {
            file,
            store,
            partition,
            concurrency,
        } => match run(&file, store, partition, concurrency).await {
            Ok(code) => code,
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            }
        },
    }
}

fn validate(path: &Path) -> ExitCode {
    match swale::load_path(path, &OperatorSet::builtin()) {
        Ok(graph) => {
            println!(
                "{}: {} nodes, {} edges",
                graph.name(),
                graph.nodes().len(),
                graph.edge_count()
            );
            ExitCode::SUCCESS
        }
        Err(Error::Invalid(problems)) => {
            for problem in problems {
                eprintln!("error: {problem}");
            }
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Checks the scheme of a `--store` URL at argument parsing. A bare path
/// passes through.
fn parse_store_arg(raw: &str) -> Result<String, String> {
    let is_url = raw.contains("://") && raw.chars().next().is_some_and(|c| c.is_ascii_alphabetic());
    if !is_url {
        return Ok(raw.to_string());
    }
    let url = url::Url::parse(raw).map_err(|e| format!("`{raw}` is not a URL: {e}"))?;
    let feature = match url.scheme() {
        "file" => None,
        "s3" => Some(("aws", cfg!(feature = "aws"))),
        "gs" => Some(("gcp", cfg!(feature = "gcp"))),
        "az" | "abfs" | "abfss" => Some(("azure", cfg!(feature = "azure"))),
        other => {
            return Err(format!(
                "scheme `{other}` is not one of s3, gs, az, abfs, abfss or file"
            ));
        }
    };
    if let Some((feature, enabled)) = feature
        && !enabled
    {
        return Err(format!(
            "scheme `{}` needs a build with the `{feature}` feature",
            url.scheme()
        ));
    }
    Ok(raw.to_string())
}

/// Opens the store named by `--store`: a directory, or an object store URL
/// with the path in the URL as the prefix.
fn open_store(raw: &str) -> Result<(Arc<dyn ObjectStore>, ObjectPath), Box<dyn std::error::Error>> {
    if raw.contains("://") {
        let url = url::Url::parse(raw)?;
        let (store, prefix) = parse_url(&url)?;
        return Ok((Arc::from(store), prefix));
    }
    std::fs::create_dir_all(raw)?;
    Ok((
        Arc::new(LocalFileSystem::new_with_prefix(raw)?),
        ObjectPath::default(),
    ))
}

async fn run(
    file: &Path,
    store_arg: String,
    partition: Option<String>,
    concurrency: usize,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(file)?;
    let operators = Arc::new(OperatorSet::builtin());
    let graph = swale::load_str(&text, &operators)?;
    let hash = definition::hash(&text);
    let partition = match partition {
        Some(key) => Partition::new(key)?,
        None if graph.partitioning() == Partitioning::Unpartitioned => Partition::none(),
        None => return Err("the graph is partitioned: pass --partition <key>".into()),
    };

    let (store, prefix) = open_store(&store_arg)?;
    let prefix = prefix.as_ref().to_string();
    let queue_path = if prefix.is_empty() {
        QUEUE_PATH.to_string()
    } else {
        format!("{prefix}/{QUEUE_PATH}")
    };
    let queue = Arc::new(Queue::open(store.clone(), &queue_path).await?);
    let pool_names: BTreeSet<&str> = graph.nodes().iter().map(|n| n.pool()).collect();
    let hook = RecordHook::new(queue.clock(), EVENTS_QUEUE);
    let mut pools = Pools::builder(queue.clone(), store, operators, hook)
        .poll_interval(Duration::from_millis(100))
        .store_prefix(prefix);
    for name in pool_names {
        pools = pools.pool(name, concurrency);
    }
    let pools = Arc::new(pools.build());
    let definitions = Arc::new(DefinitionStore::new());
    let graph = definitions.insert(hash.clone(), graph);
    let scheduler = Arc::new(Scheduler::new(queue.clone(), definitions, pools.clone()));

    let stop = CancellationToken::new();
    let pool_handles = pools.spawn(&stop);
    let scheduler_handle = scheduler.clone().spawn(
        SchedulerOptions {
            concurrency: 2,
            poll_interval: Duration::from_millis(100),
            reconcile_interval: Duration::from_secs(10),
        },
        stop.clone().cancelled_owned(),
    );

    let outcome = scheduler.start_run(&hash, &partition).await?;
    if outcome.started {
        println!(
            "{}/{partition}: started, {} root node(s) submitted",
            graph.name(),
            outcome.submitted.len()
        );
    } else {
        println!("{}/{partition}: run exists, resuming", graph.name());
    }

    let run_key = graph_run_key(graph.name(), &partition);
    let mut printed = BTreeSet::new();
    let state = loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("interrupted, stopping the workers");
                stop.cancel();
                return Ok(ExitCode::from(130));
            }
            () = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
        for node in graph.nodes() {
            if printed.contains(node.name()) {
                continue;
            }
            let key = node_record_key(graph.name(), &partition, node);
            if let Some(bytes) = queue.kv_get(&key).await? {
                let record = NodeRecord::from_bytes(&bytes)?;
                println!("  {}: {} ({})", node.name(), record.status, record.run_id);
                if let Some(error) = &record.error {
                    println!("    {error}");
                }
                printed.insert(node.name().to_string());
            }
        }
        if let Some(bytes) = queue.kv_get(&run_key).await? {
            let run = GraphRunRecord::from_bytes(&bytes)?;
            if run.state != GraphRunState::Active {
                break run.state;
            }
        }
    };
    println!("{}/{partition}: {state}", graph.name());

    stop.cancel();
    for handle in pool_handles {
        let _ = handle.wait().await;
    }
    let _ = scheduler_handle.wait().await;
    drop(scheduler);
    drop(pools);
    if let Ok(queue) = Arc::try_unwrap(queue) {
        queue.close().await?;
    }
    Ok(if state == GraphRunState::Complete {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}
