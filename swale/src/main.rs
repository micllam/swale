use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use swale::JsonBytes;
use swale::partition::InvalidPartition;
use swale::records::{
    GraphRunRecord, GraphRunState, NodeRecord, RequestOutcome, graph_run_key, node_record_key,
};
use swale::store::store_path;
use swale::{
    Daemon, DaemonOptions, DefinitionError, DefinitionStore, Error, OperatorSet, Partition,
    Partitioning, Pools, RecordHook, Request, RequestId, RequestStore, Scheduler, SchedulerOptions,
    StatusReader,
};
use taquba::object_store::local::LocalFileSystem;
use taquba::object_store::path::Path as ObjectPath;
use taquba::object_store::{ObjectStore, parse_url_opts};
use taquba::{Clock, Queue, QueueReader, ReaderMode, ReaderOptions, SystemClock};
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
        #[arg(long = "pool", value_parser = parse_pool_arg)]
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

/// The arguments of a command that writes a request.
#[derive(clap::Args)]
struct RequestArgs {
    #[command(flatten)]
    store: StoreArg,
    /// Waits for the daemon to apply the request and prints the outcome.
    #[arg(long)]
    wait: bool,
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
        Command::Validate { file } => validate(&file),
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
        Command::Start {
            graph,
            partitions,
            request: args,
        } => send_request(Request::Start { graph, partitions }, args).await,
        Command::Rerun {
            graph,
            partition,
            node,
            request: args,
        } => {
            send_request(
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
        } => send_request(Request::Cancel { graph, partition }, args).await,
        Command::Status {
            graph,
            partition,
            store,
            limit,
        } => status(store, graph, partition, limit).await,
        Command::Queues {
            queue,
            store,
            limit,
        } => queues(store, queue, limit).await,
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

fn validate(path: &Path) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let graph = swale::load_path(path, &OperatorSet::builtin())?;
    println!(
        "{}: {} nodes, {} edges",
        graph.name(),
        graph.nodes().len(),
        graph.edge_count()
    );
    Ok(ExitCode::SUCCESS)
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

/// Parses a partition key argument.
fn parse_partition_arg(raw: &str) -> Result<Partition, InvalidPartition> {
    Partition::new(raw)
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

/// The object store options among the environment variables `vars`: the
/// variables of the three providers, with the name in lower case as the
/// option key. The prefixes exclude a variable of another program whose lower
/// case name is an option key, such as `TOKEN` or `ENDPOINT`.
fn store_options(
    vars: impl Iterator<Item = (String, String)>,
) -> impl Iterator<Item = (String, String)> {
    vars.filter_map(|(name, value)| {
        let key = name.to_ascii_lowercase();
        ["aws_", "google_", "azure_"]
            .iter()
            .any(|prefix| key.starts_with(prefix))
            .then_some((key, value))
    })
}

/// An open store: the object store, the store prefix and the path of the
/// queue within the store.
struct Store {
    objects: Arc<dyn ObjectStore>,
    prefix: String,
    queue_path: String,
}

/// The value of `--store`, or the default store directory.
fn store_location(arg: StoreArg) -> Result<String, Box<dyn std::error::Error>> {
    match arg.store {
        Some(raw) => Ok(raw),
        None => Ok(default_store(std::env::home_dir())
            .ok_or("the default store needs a home directory: pass --store <dir or URL>")?
            .to_string_lossy()
            .into_owned()),
    }
}

/// Opens the store of a command that writes to it, and creates the
/// directory of a directory store first.
fn create_store(arg: StoreArg) -> Result<Store, Box<dyn std::error::Error>> {
    let location = store_location(arg)?;
    if !location.contains("://") {
        std::fs::create_dir_all(&location)?;
    }
    open_location(&location)
}

/// Opens the store of `--store`, or the default store.
fn open_store(arg: StoreArg) -> Result<Store, Box<dyn std::error::Error>> {
    open_location(&store_location(arg)?)
}

/// Opens the store at `location`: a directory, or an object store URL with
/// the path in the URL as the store prefix.
fn open_location(location: &str) -> Result<Store, Box<dyn std::error::Error>> {
    let (objects, prefix): (Arc<dyn ObjectStore>, ObjectPath) = if location.contains("://") {
        let url = url::Url::parse(location)?;
        let (store, prefix) = parse_url_opts(&url, store_options(std::env::vars()))?;
        (Arc::from(store), prefix)
    } else {
        (
            Arc::new(LocalFileSystem::new_with_prefix(location)?),
            ObjectPath::default(),
        )
    };
    let prefix = prefix.as_ref().to_string();
    let queue_path = store_path(&prefix, QUEUE_PATH);
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
        let hook = RecordHook::new(queue.clock());
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
    let store = create_store(store)?;
    let definitions = store.definitions(Arc::new(OperatorSet::builtin()));
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

    let store = create_store(store)?;
    let operators = Arc::new(OperatorSet::builtin());
    let definitions = store.definitions(operators.clone());
    let mut pool_sizes = BTreeMap::from([("default".to_string(), concurrency)]);
    pool_sizes.extend(pools);
    let requests = store.requests();
    let runtime = Runtime::open(store, operators, definitions, &pool_sizes).await?;
    let daemon = Daemon::new(
        runtime.queue.clone(),
        runtime.scheduler.clone(),
        runtime.pools.clone(),
        requests,
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

/// Writes `request` to the store and, with `--wait`, prints its outcome
/// once the daemon applies it.
async fn send_request(
    request: Request,
    args: RequestArgs,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
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

/// Opens the status reader of `--store` with the built-in operators.
async fn open_status(store: StoreArg) -> Result<StatusReader, Box<dyn std::error::Error>> {
    let store = open_store(store)?;
    Ok(StatusReader::open(
        store.objects.clone(),
        &store.queue_path,
        store.definitions(Arc::new(OperatorSet::builtin())),
    )
    .await?)
}

impl Store {
    /// The definition store, checking definitions against `operators`.
    fn definitions(&self, operators: Arc<OperatorSet>) -> Arc<DefinitionStore> {
        Arc::new(DefinitionStore::new(
            self.objects.clone(),
            &self.prefix,
            operators,
        ))
    }

    /// The request store.
    fn requests(&self) -> RequestStore {
        RequestStore::new(self.objects.clone(), &self.prefix)
    }
}

async fn status(
    store: StoreArg,
    graph: Option<String>,
    partition: Option<Partition>,
    limit: usize,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    with_reader(store, async |reader| {
        print_status(reader, graph.as_deref(), partition.as_ref(), limit).await
    })
    .await
}

/// Opens the status reader of `store`, runs `print` on it and closes it.
async fn with_reader(
    store: StoreArg,
    print: impl AsyncFnOnce(&StatusReader) -> Result<(), Box<dyn std::error::Error>>,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let reader = open_status(store).await?;
    let result = print(&reader).await;
    reader.close().await?;
    result?;
    Ok(ExitCode::SUCCESS)
}

async fn print_status(
    reader: &StatusReader,
    graph: Option<&str>,
    partition: Option<&Partition>,
    limit: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(graph) = graph else {
        let header = [
            "GRAPH",
            "DEFINITION",
            "ACTIVE",
            "COMPLETE",
            "FAILED",
            "CANCELLED",
            "LATEST",
        ];
        let mut rows = Vec::new();
        for graph in reader.graphs().await? {
            rows.push([
                graph.name,
                graph
                    .adopted
                    .map_or("-".to_string(), |record| short_hash(&record.definition)),
                graph.runs.active.to_string(),
                graph.runs.complete.to_string(),
                graph.runs.failed.to_string(),
                graph.runs.cancelled.to_string(),
                graph.latest.map_or("-".to_string(), |run| {
                    format!("{} {}", run.partition, run.record.state)
                }),
            ]);
        }
        print_table(header, &rows);
        return Ok(());
    };
    let Some(partition) = partition else {
        let runs = reader.runs(graph).await?;
        if runs.is_empty() {
            return Err(format!("graph `{graph}` does not have a graph run").into());
        }
        let header = ["PARTITION", "STATE", "REQUESTED", "DEFINITION"];
        let mut rows = Vec::new();
        for run in &runs[runs.len().saturating_sub(limit)..] {
            rows.push([
                run.partition.to_string(),
                run.record.state.to_string(),
                format_time(run.record.requested_at_ms),
                short_hash(&run.record.definition),
            ]);
        }
        print_table(header, &rows);
        return Ok(());
    };
    let Some(run) = reader.run(graph, partition).await? else {
        return Err(
            format!("graph `{graph}` does not have a run for partition `{partition}`").into(),
        );
    };
    println!(
        "{graph}/{partition}: {}, requested {}, definition {}",
        run.record.state,
        format_time(run.record.requested_at_ms),
        short_hash(&run.record.definition)
    );
    let header = ["NODE", "POOL", "STATE", "RUN", "TERMINATED"];
    let mut rows = Vec::new();
    for node in &run.nodes {
        let record = node.record.as_ref();
        rows.push([
            node.name.clone(),
            node.pool.clone(),
            node.state.to_string(),
            record.map_or("-".to_string(), |r| r.run_id.clone()),
            record.map_or("-".to_string(), |r| format_time(r.terminated_at_ms)),
        ]);
    }
    print_table(header, &rows);
    for node in &run.nodes {
        if let Some(error) = node.record.as_ref().and_then(|r| r.error.as_ref()) {
            println!("{}: {error}", node.name);
        }
    }
    Ok(())
}

async fn queues(
    store: StoreArg,
    queue: Option<String>,
    limit: usize,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    with_reader(store, async |reader| {
        print_queues(reader, queue.as_deref(), limit).await
    })
    .await
}

async fn print_queues(
    reader: &StatusReader,
    queue: Option<&str>,
    limit: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(queue) = queue else {
        let header = ["QUEUE", "PENDING", "SCHEDULED", "CLAIMED", "DONE", "DEAD"];
        let mut rows = Vec::new();
        for stats in reader.queues().await? {
            rows.push([
                stats.queue,
                stats.pending.to_string(),
                stats.scheduled.to_string(),
                stats.claimed.to_string(),
                stats.done.to_string(),
                stats.dead.to_string(),
            ]);
        }
        print_table(header, &rows);
        return Ok(());
    };
    let header = ["JOB", "RUN", "ATTEMPTS", "ENQUEUED"];
    let mut rows = Vec::new();
    let Some(jobs) = reader.dead_jobs(queue, limit).await? else {
        return Err(format!("queue `{queue}` does not exist").into());
    };
    for job in &jobs {
        rows.push([
            job.id.clone(),
            job.headers
                .get(taquba_workflow::HEADER_RUN_ID)
                .cloned()
                .unwrap_or_else(|| "-".to_string()),
            format!("{}/{}", job.attempts, job.max_attempts),
            format_time(job.enqueued_at),
        ]);
    }
    print_table(header, &rows);
    for job in &jobs {
        if let Some(error) = &job.last_error {
            println!("{}: {error}", job.id);
        }
    }
    Ok(())
}

/// The first twelve characters of a definition hash.
fn short_hash(hash: &str) -> String {
    hash.chars().take(12).collect()
}

/// A time in milliseconds from the Unix epoch as UTC, to the second.
fn format_time(ms: u64) -> String {
    i64::try_from(ms)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map_or_else(
            || ms.to_string(),
            |time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        )
}

/// The header and the rows as columns padded to the widest cell.
fn format_table<const N: usize>(header: [&str; N], rows: &[[String; N]]) -> String {
    let mut widths = header.map(str::len);
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.len());
        }
    }
    let mut text = String::new();
    let mut push = |cells: [&str; N]| {
        let line: Vec<String> = cells
            .iter()
            .zip(&widths)
            .map(|(cell, width)| format!("{cell:<width$}"))
            .collect();
        text.push_str(line.join("  ").trim_end());
        text.push('\n');
    };
    push(header);
    for row in rows {
        push(row.each_ref().map(String::as_str));
    }
    text
}

fn print_table<const N: usize>(header: [&str; N], rows: &[[String; N]]) {
    print!("{}", format_table(header, rows));
}

async fn run(
    file: &Path,
    store: StoreArg,
    partition: Option<Partition>,
    concurrency: usize,
) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(file)?;
    let operators = Arc::new(OperatorSet::builtin());
    let graph = swale::load_str(&text, &operators)?;
    let partition = match partition {
        Some(partition) => partition,
        None if graph.partitioning() == Partitioning::Unpartitioned => Partition::none(),
        None => return Err("the graph is partitioned: pass --partition <key>".into()),
    };

    let store = create_store(store)?;
    let definitions = store.definitions(operators.clone());
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
                for handle in pool_handles {
                    let _ = handle.wait().await;
                }
                let _ = scheduler_handle.wait().await;
                runtime.close().await?;
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
    fn the_store_options_are_the_provider_variables_in_lower_case() {
        let vars = [
            ("AWS_ENDPOINT", "http://127.0.0.1:9000"),
            ("ENDPOINT", "other"),
            ("GOOGLE_SERVICE_ACCOUNT", "sa.json"),
            ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
            ("HOME", "/home/u"),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()));
        let options: Vec<(String, String)> = store_options(vars.into_iter()).collect();
        assert_eq!(
            options,
            [
                ("aws_endpoint", "http://127.0.0.1:9000"),
                ("google_service_account", "sa.json"),
                ("azure_storage_account_name", "account"),
            ]
            .map(|(name, value)| (name.to_string(), value.to_string()))
        );
    }

    #[test]
    fn a_pool_argument_is_a_name_and_a_positive_step_count() {
        assert_eq!(parse_pool_arg("warehouse=2"), Ok(("warehouse".into(), 2)));
        for raw in ["warehouse", "warehouse=0", "warehouse=x", "Bad=1", "=1"] {
            assert!(parse_pool_arg(raw).is_err(), "{raw}");
        }
    }
}
