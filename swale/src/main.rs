use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use swale::records::{GraphRunRecord, GraphRunState, NodeRecord, graph_run_key, node_record_key};
use swale::{
    Daemon, DaemonOptions, DefinitionStore, EVENTS_QUEUE, Error, OperatorSet, Partition,
    Partitioning, Pools, RecordHook, Scheduler, SchedulerOptions,
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
        #[command(flatten)]
        store: StoreArg,
        /// The partition key. Required for a partitioned graph.
        #[arg(long)]
        partition: Option<String>,
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
        #[arg(long = "pool", value_parser = parse_pool_arg)]
        pools: Vec<(String, usize)>,
        /// Seconds between sync passes over the published definitions.
        #[arg(long, default_value_t = 30)]
        sync_interval: u64,
    },
}

#[derive(clap::Args)]
struct StoreArg {
    /// The store: a directory, created when absent, or an object store
    /// URL (`s3://bucket/prefix`, `gs://bucket/prefix`,
    /// `az://container/prefix`, `file:///path`). A cloud scheme needs
    /// the matching cargo feature and reads the provider's environment
    /// variables for its credentials. The default is `~/.swale/store`.
    #[arg(long, value_parser = parse_store_arg)]
    store: Option<String>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let result = match Cli::parse().command {
        Command::Validate { file } => return validate(&file),
        Command::Run {
            file,
            store,
            partition,
            concurrency,
        } => run(&file, store, partition, concurrency).await,
        Command::Publish { file, store } => publish(&file, store).await,
        Command::Daemon {
            store,
            concurrency,
            pools,
            sync_interval,
        } => {
            daemon(
                store,
                concurrency,
                pools,
                Duration::from_secs(sync_interval),
            )
            .await
        }
    };
    match result {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
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

/// Parses a `--pool` argument of the form `name=steps`.
fn parse_pool_arg(raw: &str) -> Result<(String, usize), String> {
    let invalid = || format!("`{raw}` is not `name=steps`, such as `warehouse=2`");
    let (name, steps) = raw.split_once('=').ok_or_else(invalid)?;
    let steps: usize = steps.parse().map_err(|_| invalid())?;
    if !swale::graph::is_name(name) || steps == 0 {
        return Err(invalid());
    }
    Ok((name.to_string(), steps))
}

/// The default store directory: `.swale/store` in `home`, the result of
/// [`std::env::home_dir`]. On Unix that result is empty for an empty `HOME`.
fn default_store(home: Option<PathBuf>) -> Option<PathBuf> {
    let home = home.filter(|dir| !dir.as_os_str().is_empty())?;
    Some(home.join(".swale").join("store"))
}

/// An open store: the object store, the store prefix and the path of the
/// queue within the store.
struct Store {
    objects: Arc<dyn ObjectStore>,
    prefix: String,
    queue_path: String,
}

/// Opens the store of `--store`, or the default store: a directory, or an
/// object store URL with the path in the URL as the store prefix.
fn open_store(arg: StoreArg) -> Result<Store, Box<dyn std::error::Error>> {
    let raw = match arg.store {
        Some(raw) => raw,
        None => default_store(std::env::home_dir())
            .ok_or("the default store needs a home directory: pass --store <dir or URL>")?
            .to_string_lossy()
            .into_owned(),
    };
    let (objects, prefix): (Arc<dyn ObjectStore>, ObjectPath) = if raw.contains("://") {
        let url = url::Url::parse(&raw)?;
        let (store, prefix) = parse_url(&url)?;
        (Arc::from(store), prefix)
    } else {
        std::fs::create_dir_all(&raw)?;
        (
            Arc::new(LocalFileSystem::new_with_prefix(&raw)?),
            ObjectPath::default(),
        )
    };
    let prefix = prefix.as_ref().to_string();
    let queue_path = if prefix.is_empty() {
        QUEUE_PATH.to_string()
    } else {
        format!("{prefix}/{QUEUE_PATH}")
    };
    Ok(Store {
        objects,
        prefix,
        queue_path,
    })
}

/// The queue, the pools and the scheduler of a process over `store`.
struct Runtime {
    queue: Arc<Queue>,
    pools: Arc<Pools>,
    scheduler: Arc<Scheduler>,
}

impl Runtime {
    async fn open(
        store: Store,
        operators: Arc<OperatorSet>,
        definitions: Arc<DefinitionStore>,
        pool_sizes: &BTreeMap<String, usize>,
    ) -> Result<Runtime, Box<dyn std::error::Error>> {
        let queue = Arc::new(Queue::open(store.objects.clone(), &store.queue_path).await?);
        let hook = RecordHook::new(queue.clock(), EVENTS_QUEUE);
        let mut pools = Pools::builder(queue.clone(), store.objects, operators, hook)
            .poll_interval(Duration::from_millis(100))
            .store_prefix(store.prefix);
        for (name, steps) in pool_sizes {
            pools = pools.pool(name, *steps);
        }
        let pools = Arc::new(pools.build());
        let scheduler = Arc::new(Scheduler::new(queue.clone(), definitions, pools.clone()));
        Ok(Runtime {
            queue,
            pools,
            scheduler,
        })
    }

    async fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        drop(self.scheduler);
        drop(self.pools);
        if let Ok(queue) = Arc::try_unwrap(self.queue) {
            queue.close().await?;
        }
        Ok(())
    }
}

async fn publish(file: &Path, store: StoreArg) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(file)?;
    let store = open_store(store)?;
    let definitions = DefinitionStore::new(
        store.objects,
        &store.prefix,
        Arc::new(OperatorSet::builtin()),
    );
    let published = definitions.publish(&text).await?;
    let state = if published.changed {
        "published"
    } else {
        "unchanged"
    };
    println!("{}: {state} {}", published.graph.name(), published.hash);
    Ok(ExitCode::SUCCESS)
}

async fn daemon(
    store: StoreArg,
    concurrency: usize,
    pools: Vec<(String, usize)>,
    sync_interval: Duration,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("SWALE_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,swale=info")),
        )
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .with_writer(std::io::stderr)
        .init();

    let store = open_store(store)?;
    let operators = Arc::new(OperatorSet::builtin());
    let definitions = Arc::new(DefinitionStore::new(
        store.objects.clone(),
        &store.prefix,
        operators.clone(),
    ));
    let mut pool_sizes = BTreeMap::from([("default".to_string(), concurrency)]);
    pool_sizes.extend(pools);
    let runtime = Runtime::open(store, operators, definitions, &pool_sizes).await?;
    let daemon = Daemon::new(
        runtime.queue.clone(),
        runtime.scheduler.clone(),
        runtime.pools.clone(),
    );
    let result = daemon
        .run(
            DaemonOptions {
                sync_interval,
                ..DaemonOptions::default()
            },
            async {
                let _ = tokio::signal::ctrl_c().await;
            },
        )
        .await;
    drop(daemon);
    runtime.close().await?;
    result?;
    Ok(ExitCode::SUCCESS)
}

async fn run(
    file: &Path,
    store: StoreArg,
    partition: Option<String>,
    concurrency: usize,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(file)?;
    let operators = Arc::new(OperatorSet::builtin());
    let graph = swale::load_str(&text, &operators)?;
    let partition = match partition {
        Some(key) => Partition::new(key)?,
        None if graph.partitioning() == Partitioning::Unpartitioned => Partition::none(),
        None => return Err("the graph is partitioned: pass --partition <key>".into()),
    };

    let store = open_store(store)?;
    let definitions = Arc::new(DefinitionStore::new(
        store.objects.clone(),
        &store.prefix,
        operators.clone(),
    ));
    let (hash, graph) = definitions.put(&text).await?;
    let pool_sizes: BTreeMap<String, usize> = graph
        .nodes()
        .iter()
        .map(|n| (n.pool().to_string(), concurrency))
        .collect();
    let runtime = Runtime::open(store, operators, definitions, &pool_sizes).await?;
    let (queue, pools, scheduler) = (&runtime.queue, &runtime.pools, &runtime.scheduler);

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
    runtime.close().await?;
    Ok(if state == GraphRunState::Complete {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_store_is_swale_store_in_the_home_directory() {
        assert_eq!(
            default_store(Some(PathBuf::from("/home/u"))),
            Some(Path::new("/home/u").join(".swale").join("store"))
        );
        assert_eq!(default_store(Some(PathBuf::new())), None);
        assert_eq!(default_store(None), None);
    }

    #[test]
    fn a_pool_argument_is_a_name_and_a_positive_step_count() {
        assert_eq!(parse_pool_arg("warehouse=2"), Ok(("warehouse".into(), 2)));
        for raw in ["warehouse", "warehouse=0", "warehouse=x", "Bad=1", "=1"] {
            assert!(parse_pool_arg(raw).is_err(), "{raw}");
        }
    }
}
