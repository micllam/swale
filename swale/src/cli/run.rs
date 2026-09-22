//! `swale run`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use swale::JsonBytes;
use swale::records::{GraphRunRecord, GraphRunState, NodeRecord, graph_run_key, node_record_key};
use swale::{OperatorSet, Partition, Partitioning, SchedulerOptions};
use tokio_util::sync::CancellationToken;

use super::CommandResult;
use super::store::{Runtime, StoreArg, create_store};

pub(crate) async fn run(
    file: &Path,
    store: StoreArg,
    partition: Option<Partition>,
    concurrency: usize,
) -> CommandResult {
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
