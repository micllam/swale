//! The status view on an in-memory store, read through a `QueueReader` while
//! the queue is open.

use std::sync::Arc;
use std::time::Duration;

use swale::records::{GraphRunState, graph_key};
use swale::{
    DefinitionStore, EVENTS_QUEUE, GraphRecord, NodeState, OperatorSet, Partition, Pools,
    RecordHook, RunCounts, Scheduler, SchedulerOptions, StatusReader,
};
use taquba::object_store::ObjectStore;
use taquba::object_store::memory::InMemory;
use taquba::{MockClock, OpenOptions, Queue, QueueConfig};
use tokio_util::sync::CancellationToken;

const PARTITION: &str = "20260915";
const START_MS: u64 = 1_700_000_000_000;

/// `transform` fails permanently, `load` consumes its asset in the
/// `warehouse` pool, and `notify` runs after `transform` with `all_done`.
const DEFINITION: &str = r#"
[graph]
name = "orders"
partition = "daily"

[[node]]
name = "extract"
produces = "orders_raw"
operator = "subprocess"
[node.params]
argv = ["sh", "-c", "cat >/dev/null; printf '{}'"]

[[node]]
name = "transform"
produces = "orders_clean"
consumes = ["orders_raw"]
operator = "subprocess"
[node.params]
argv = ["sh", "-c", "cat >/dev/null; exit 3"]

[[node]]
name = "load"
produces = "orders_warehouse"
consumes = ["orders_clean"]
operator = "subprocess"
pool = "warehouse"
[node.params]
argv = ["sh", "-c", "cat >/dev/null; printf '{}'"]

[[node]]
name = "notify"
after = ["transform"]
trigger_rule = "all_done"
operator = "subprocess"
[node.params]
argv = ["sh", "-c", "cat >/dev/null; printf '{}'"]
"#;

struct Harness {
    store: Arc<dyn ObjectStore>,
    queue: Arc<Queue>,
    definitions: Arc<DefinitionStore>,
    scheduler: Arc<Scheduler>,
    hash: String,
    stop: CancellationToken,
}

impl Harness {
    async fn start(spawn_workers: bool) -> Harness {
        let clock = MockClock::new(START_MS);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let opts = OpenOptions::default()
            .clock(Arc::new(clock))
            .default_queue_config(QueueConfig::default().retry_backoff_base(Duration::ZERO))
            .reaper_interval(Duration::from_millis(10))
            .scheduler_interval(Duration::from_millis(10));
        let queue = Arc::new(
            Queue::open_with_options(store.clone(), "test", opts)
                .await
                .unwrap(),
        );
        let operators = Arc::new(OperatorSet::builtin());
        let definitions = Arc::new(DefinitionStore::new(store.clone(), "", operators.clone()));
        let (hash, _) = definitions.put(DEFINITION).await.unwrap();
        let hook = RecordHook::new(queue.clock(), EVENTS_QUEUE);
        let pools = Arc::new(
            Pools::builder(queue.clone(), store.clone(), operators, hook)
                .poll_interval(Duration::from_millis(10))
                .pool("default", 4)
                .pool("warehouse", 1)
                .build(),
        );
        let scheduler = Arc::new(Scheduler::new(
            queue.clone(),
            definitions.clone(),
            pools.clone(),
        ));
        let stop = CancellationToken::new();
        if spawn_workers {
            let _ = pools.spawn(&stop);
            let _ = scheduler.clone().spawn(
                SchedulerOptions {
                    concurrency: 2,
                    poll_interval: Duration::from_millis(10),
                    reconcile_interval: Duration::from_secs(3600),
                },
                stop.clone().cancelled_owned(),
            );
        }
        Harness {
            store,
            queue,
            definitions,
            scheduler,
            hash,
            stop,
        }
    }

    fn partition() -> Partition {
        Partition::new(PARTITION).unwrap()
    }

    async fn reader(&self) -> StatusReader {
        StatusReader::open(self.store.clone(), "test", self.definitions.clone())
            .await
            .unwrap()
    }

    /// A reader whose view includes the graph run in `state`. A reader sees
    /// a write after the writer flushes it, so the open repeats until then.
    async fn reader_at(&self, state: GraphRunState) -> StatusReader {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let reader = self.reader().await;
            let runs = reader.runs("orders").await.unwrap();
            if runs.first().is_some_and(|run| run.record.state == state) {
                return reader;
            }
            reader.close().await.unwrap();
            assert!(
                tokio::time::Instant::now() < deadline,
                "no reader saw the graph run in {state:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

fn states(run: &swale::GraphRunStatus) -> Vec<(&str, NodeState)> {
    run.nodes
        .iter()
        .map(|node| (node.name.as_str(), node.state))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_run_reads_with_its_counts_its_blocked_node_and_its_dead_job() {
    let h = Harness::start(true).await;
    let adopted = GraphRecord {
        definition: h.hash.clone(),
        adopted_at_ms: START_MS,
    };
    h.queue
        .kv_put(&graph_key("orders"), &adopted.to_bytes())
        .await
        .unwrap();
    h.scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();

    let reader = h.reader_at(GraphRunState::Failed).await;
    let graphs = reader.graphs().await.unwrap();
    assert_eq!(graphs.len(), 1);
    assert_eq!(graphs[0].name, "orders");
    assert_eq!(graphs[0].adopted, Some(adopted));
    assert_eq!(
        graphs[0].runs,
        RunCounts {
            failed: 1,
            ..RunCounts::default()
        }
    );
    let latest = graphs[0].latest.as_ref().unwrap();
    assert_eq!(latest.partition, Harness::partition());

    let runs = reader.runs("orders").await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].record.state, GraphRunState::Failed);
    assert_eq!(runs[0].record.requested_at_ms, START_MS);
    assert!(reader.runs("order").await.unwrap().is_empty());

    let run = reader
        .run("orders", &Harness::partition())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.record.definition, h.hash);
    assert_eq!(
        states(&run),
        [
            ("extract", NodeState::Succeeded),
            ("transform", NodeState::Failed),
            ("load", NodeState::Blocked),
            ("notify", NodeState::Succeeded),
        ]
    );
    let transform = run.nodes[1].record.as_ref().unwrap();
    assert_eq!(transform.run_id, "orders-20260915-transform-r0");
    assert!(
        transform
            .error
            .as_deref()
            .unwrap()
            .contains("exited with 3")
    );
    assert_eq!(run.nodes[2].pool, "warehouse");
    assert_eq!(run.nodes[2].record, None);
    let absent = Partition::new("20260916").unwrap();
    assert_eq!(reader.run("orders", &absent).await.unwrap(), None);

    let queues = reader.queues().await.unwrap();
    let pool = queues
        .iter()
        .find(|stats| stats.queue == "swale-pool-default")
        .unwrap();
    assert_eq!(pool.dead, 1);
    let dead = reader.dead_jobs("swale-pool-default", 10).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(
        dead[0].headers[taquba_workflow::HEADER_RUN_ID],
        "orders-20260915-transform-r0"
    );
    reader.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_active_run_without_workers_has_a_ready_root_and_waiting_downstreams() {
    let h = Harness::start(false).await;
    h.scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();

    let reader = h.reader().await;
    let graphs = reader.graphs().await.unwrap();
    assert_eq!(graphs[0].adopted, None);
    assert_eq!(graphs[0].runs.active, 1);
    let run = reader
        .run("orders", &Harness::partition())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.record.state, GraphRunState::Active);
    assert_eq!(
        states(&run),
        [
            ("extract", NodeState::Ready),
            ("transform", NodeState::Waiting),
            ("load", NodeState::Waiting),
            ("notify", NodeState::Waiting),
        ]
    );
    reader.close().await.unwrap();
}
