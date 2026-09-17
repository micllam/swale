//! The daemon on an in-memory store with the mock clock: adoption, the
//! catch-up of a new graph, a cron firing and the backfill after downtime.

use std::sync::Arc;
use std::time::Duration;

use swale::records::{GraphRunRecord, GraphRunState, graph_key, graph_run_key};
use swale::scheduler::Error;
use swale::{
    Daemon, DaemonOptions, DefinitionStore, EVENTS_QUEUE, GraphRecord, OperatorSet, Partition,
    Pools, RecordHook, Scheduler, SchedulerOptions, TRIGGERS_QUEUE, Trigger,
};
use taquba::object_store::memory::InMemory;
use taquba::object_store::path::Path as ObjectPath;
use taquba::object_store::{ObjectStore, ObjectStoreExt};
use taquba::{MockClock, OpenOptions, Queue, QueueConfig};
use taquba_cron::{Backfill, BackfillStart, Schedule};
use tokio_util::sync::CancellationToken;

/// A scheduled graph of one subprocess node in `pool`, which produces
/// `asset`, with a catch-up window of three days.
fn definition_text(graph: &str, schedule: &str, asset: &str, pool: &str) -> String {
    definition_with_catchup(graph, schedule, asset, pool, "3d")
}

fn definition_with_catchup(
    graph: &str,
    schedule: &str,
    asset: &str,
    pool: &str,
    catchup: &str,
) -> String {
    format!(
        r#"
[graph]
name = "{graph}"
schedule = "{schedule}"
catchup = "{catchup}"
partition = "daily"

[[node]]
name = "extract"
produces = "{asset}"
operator = "subprocess"
pool = "{pool}"
[node.params]
argv = ["sh", "-c", "cat >/dev/null; printf '{{}}'"]
"#
    )
}

fn ms(text: &str) -> u64 {
    let time: chrono::DateTime<chrono::Utc> = text.parse().unwrap();
    time.timestamp_millis() as u64
}

struct Harness {
    objects: Arc<dyn ObjectStore>,
    queue: Arc<Queue>,
    clock: MockClock,
    definitions: Arc<DefinitionStore>,
    operators: Arc<OperatorSet>,
}

impl Harness {
    async fn start(now: &str) -> Harness {
        let clock = MockClock::new(ms(now));
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let opts = OpenOptions::default()
            .clock(Arc::new(clock.clone()))
            .default_queue_config(QueueConfig::default().retry_backoff_base(Duration::ZERO))
            .reaper_interval(Duration::from_millis(10))
            .scheduler_interval(Duration::from_millis(10));
        let queue = Arc::new(
            Queue::open_with_options(objects.clone(), "test", opts)
                .await
                .unwrap(),
        );
        let operators = Arc::new(OperatorSet::builtin());
        let definitions = Arc::new(DefinitionStore::new(objects.clone(), "", operators.clone()));
        Harness {
            objects,
            queue,
            clock,
            definitions,
            operators,
        }
    }

    /// A daemon with the `default` pool, as a process start builds it.
    fn daemon(&self) -> (Arc<Daemon>, Arc<Scheduler>) {
        let hook = RecordHook::new(self.queue.clock(), EVENTS_QUEUE);
        let pools = Arc::new(
            Pools::builder(
                self.queue.clone(),
                self.objects.clone(),
                self.operators.clone(),
                hook,
            )
            .poll_interval(Duration::from_millis(10))
            .pool("default", 4)
            .build(),
        );
        let scheduler = Arc::new(Scheduler::new(
            self.queue.clone(),
            self.definitions.clone(),
            pools.clone(),
        ));
        let daemon = Arc::new(Daemon::new(self.queue.clone(), scheduler.clone(), pools));
        (daemon, scheduler)
    }

    fn spawn(
        &self,
    ) -> (
        CancellationToken,
        tokio::task::JoinHandle<Result<(), Error>>,
    ) {
        let (daemon, _) = self.daemon();
        let stop = CancellationToken::new();
        let shutdown = stop.clone().cancelled_owned();
        let options = DaemonOptions {
            scheduler: SchedulerOptions {
                concurrency: 2,
                poll_interval: Duration::from_millis(10),
                reconcile_interval: Duration::from_secs(3600),
            },
            sync_interval: Duration::from_millis(20),
        };
        let handle = tokio::spawn(async move { daemon.run(options, shutdown).await });
        (stop, handle)
    }

    async fn graph_run(&self, partition: &str) -> Option<GraphRunRecord> {
        let key = graph_run_key("orders", &Partition::new(partition).unwrap());
        self.queue
            .kv_get(&key)
            .await
            .unwrap()
            .map(|b| GraphRunRecord::from_bytes(&b).unwrap())
    }

    async fn wait_for_complete(&self, partition: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(run) = self.graph_run(partition).await
                && run.state == GraphRunState::Complete
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the graph run of {partition} never completed"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn graph_record(&self, graph: &str) -> Option<GraphRecord> {
        self.queue
            .kv_get(&graph_key(graph))
            .await
            .unwrap()
            .map(|b| GraphRecord::from_bytes(&b).unwrap())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_graph_catches_up_firings_run_their_interval_start_and_downtime_is_backfilled() {
    let h = Harness::start("2026-09-16T01:59:59.950Z").await;
    h.definitions
        .publish(&definition_text(
            "orders",
            "0 2 * * *",
            "orders_raw",
            "default",
        ))
        .await
        .unwrap();

    // The cron scheduler rejects the window of `far`, which is beyond the
    // range of its time type. The daemon registers the set without it.
    h.definitions
        .publish(&definition_with_catchup(
            "far",
            "0 2 * * *",
            "far_raw",
            "default",
            "99999999999d",
        ))
        .await
        .unwrap();

    // The catch-up window of three days contains the firings of 13, 14 and
    // 15 September, and each runs the day before it.
    let (stop, handle) = h.spawn();
    for partition in ["20260912", "20260913", "20260914"] {
        h.wait_for_complete(partition).await;
    }
    assert!(h.graph_run("20260911").await.is_none());
    assert!(h.graph_run("20260915").await.is_none());

    // The firing of 02:00 on 16 September.
    h.clock.advance(Duration::from_secs(1));
    h.wait_for_complete("20260915").await;

    // An edit of the schedule applies on the running daemon. The entry of
    // the new expression resumes at the watermark, so its firing of 12:00
    // runs the partition of the interval start at 00:00.
    let edit = h
        .definitions
        .publish(&definition_text(
            "orders",
            "0 */12 * * *",
            "orders_raw",
            "default",
        ))
        .await
        .unwrap();
    h.clock.advance(Duration::from_secs(10 * 3600));
    h.wait_for_complete("20260916").await;
    assert_eq!(h.graph_run("20260916").await.unwrap().definition, edit.hash);

    // Four firings pass while the process is down, and two of them start a
    // partition without a graph run.
    stop.cancel();
    handle.await.unwrap().unwrap();
    h.clock.advance(Duration::from_secs(2 * 86_400));
    assert!(h.graph_run("20260917").await.is_none());
    let (stop, handle) = h.spawn();
    for partition in ["20260917", "20260918"] {
        h.wait_for_complete(partition).await;
    }
    stop.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_sync_pass_adopts_a_changed_pointer_and_refuses_a_definition_that_cannot_run() {
    let h = Harness::start("2026-09-16T12:00:00Z").await;
    let (daemon, scheduler) = h.daemon();
    let schedule = |expression: &str| {
        Schedule::new(
            "orders",
            expression.parse().unwrap(),
            TRIGGERS_QUEUE,
            Trigger::firing("orders").to_bytes(),
        )
        .backfill(Some(Backfill {
            lookback: Duration::from_secs(3 * 86_400),
            start: BackfillStart::Lookback,
        }))
    };

    // Before the adoption a trigger of the graph is refused.
    assert!(matches!(
        scheduler
            .handle_trigger(&Trigger::firing("orders"), Some(0))
            .await,
        Err(Error::UnknownGraph(graph)) if graph == "orders"
    ));

    // The adoption writes the graph record, and the cron entry has the
    // catch-up window.
    let v1 = h
        .definitions
        .publish(&definition_text(
            "orders",
            "0 2 * * *",
            "orders_raw",
            "default",
        ))
        .await
        .unwrap();
    let report = daemon.sync().await.unwrap();
    assert_eq!(report.adopted, [("orders".to_string(), v1.hash.clone())]);
    assert_eq!(report.schedules, [schedule("0 2 * * *")]);
    let record = h.graph_record("orders").await.unwrap();
    assert_eq!(record.definition, v1.hash);
    assert_eq!(record.adopted_at_ms, ms("2026-09-16T12:00:00Z"));

    // A pass without a changed pointer does not write.
    let report = daemon.sync().await.unwrap();
    assert!(report.adopted.is_empty());
    assert_eq!(report.schedules, [schedule("0 2 * * *")]);

    // A trigger without a partition and without an interval start is
    // refused.
    assert!(matches!(
        scheduler.handle_trigger(&Trigger::firing("orders"), None).await,
        Err(Error::NoPartition(graph)) if graph == "orders"
    ));

    // A pool without a runtime: the graph keeps its adopted definition.
    h.definitions
        .publish(&definition_text("orders", "0 4 * * *", "orders_raw", "gpu"))
        .await
        .unwrap();
    let report = daemon.sync().await.unwrap();
    assert!(report.adopted.is_empty());
    assert_eq!(report.refused.len(), 1);
    assert!(report.refused[0].1.contains("pool `gpu`"), "{report:?}");
    assert_eq!(report.schedules, [schedule("0 2 * * *")]);
    assert_eq!(h.graph_record("orders").await.unwrap().definition, v1.hash);

    // An edit is adopted at the time of its pass.
    h.clock.advance(Duration::from_secs(3600));
    let v3 = h
        .definitions
        .publish(&definition_text(
            "orders",
            "0 3 * * *",
            "orders_raw",
            "default",
        ))
        .await
        .unwrap();
    let report = daemon.sync().await.unwrap();
    assert_eq!(report.adopted, [("orders".to_string(), v3.hash.clone())]);
    assert_eq!(report.schedules, [schedule("0 3 * * *")]);
    let record = h.graph_record("orders").await.unwrap();
    assert_eq!(record.adopted_at_ms, ms("2026-09-16T13:00:00Z"));

    // A target request starts the partitions it lists, once.
    let request = Trigger {
        graph: "orders".into(),
        partitions: vec![Partition::new("20260901").unwrap()],
    };
    let started = scheduler.handle_trigger(&request, None).await.unwrap();
    assert_eq!(started, request.partitions);
    let started = scheduler.handle_trigger(&request, None).await.unwrap();
    assert!(started.is_empty());
    assert_eq!(h.graph_run("20260901").await.unwrap().definition, v3.hash);

    // Another tool writes a pointer object without the publish check. The
    // pass refuses its definition, which produces an asset of `orders`.
    let (hash, _) = h
        .definitions
        .put(&definition_text(
            "other",
            "0 5 * * *",
            "orders_raw",
            "default",
        ))
        .await
        .unwrap();
    h.objects
        .put(
            &ObjectPath::from("definitions/current/other"),
            hash.into_bytes().into(),
        )
        .await
        .unwrap();
    let report = daemon.sync().await.unwrap();
    assert!(
        report.refused[0].1.contains("produced by graph `orders`"),
        "{report:?}"
    );
    assert!(h.graph_record("other").await.is_none());
}
