//! The daemon on an in-memory store with the mock clock: adoption, the
//! catch-up of a new graph, a cron firing, the backfill after downtime and
//! the requests of another process.

use std::sync::Arc;
use std::time::Duration;

use swale::JsonBytes;
use swale::records::{
    GraphRunRecord, GraphRunState, NodeRecord, RecordStatus, RequestOutcome, graph_key,
    graph_run_key, request_key,
};
use swale::scheduler::{Error, firing_headers};
use swale::{
    Daemon, DaemonOptions, DefinitionStore, GraphRecord, OperatorSet, Partition, Pools, RecordHook,
    Request, RequestId, RequestStore, Scheduler, SchedulerOptions, StatusReader, TRIGGERS_QUEUE,
};
use taquba::object_store::path::Path as ObjectPath;
use taquba::object_store::{ObjectStore, ObjectStoreExt};
use taquba::{MockClock, Queue};
use taquba_cron::{Backfill, BackfillStart, Schedule};
use tokio_util::sync::CancellationToken;

mod common;

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
        let (objects, queue) = common::open_queue(clock.clone()).await;
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

    fn requests(&self) -> RequestStore {
        RequestStore::new(self.objects.clone(), "")
    }

    /// A daemon with the `default` pool, as a process start builds it.
    fn daemon(&self) -> (Arc<Daemon>, Arc<Scheduler>, Arc<Pools>) {
        let hook = RecordHook::new(self.queue.clock());
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
        let daemon = Arc::new(Daemon::new(
            self.queue.clone(),
            scheduler.clone(),
            pools.clone(),
            self.requests(),
        ));
        (daemon, scheduler, pools)
    }

    fn spawn(
        &self,
    ) -> (
        CancellationToken,
        tokio::task::JoinHandle<Result<(), Error>>,
    ) {
        let (daemon, _, _) = self.daemon();
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
        common::wait_until(
            &format!("the graph run of {partition} never completed"),
            async || {
                self.graph_run(partition)
                    .await
                    .filter(|run| run.state == GraphRunState::Complete)
            },
        )
        .await;
    }

    async fn graph_record(&self, graph: &str) -> Option<GraphRecord> {
        self.queue
            .kv_get(&graph_key(graph))
            .await
            .unwrap()
            .map(|b| GraphRecord::from_bytes(&b).unwrap())
    }

    /// The record of `extract` for `partition` once its status is `status`.
    async fn wait_for_extract(&self, partition: &str, status: RecordStatus) -> NodeRecord {
        let key = format!("swale/assets/orders_raw/{partition}");
        common::wait_until(
            &format!("extract of {partition} never reached {status}"),
            async || {
                let bytes = self.queue.kv_get(key.as_bytes()).await.unwrap()?;
                Some(NodeRecord::from_bytes(&bytes).unwrap()).filter(|r| r.status == status)
            },
        )
        .await
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
    let (daemon, scheduler, _) = h.daemon();
    let schedule = |expression: &str| {
        Schedule::new(
            "orders",
            expression.parse().unwrap(),
            TRIGGERS_QUEUE,
            Vec::new(),
        )
        .headers(firing_headers("orders"))
        .backfill(Some(Backfill {
            lookback: Duration::from_secs(3 * 86_400),
            start: BackfillStart::Lookback,
        }))
    };

    // Before the adoption a trigger of the graph is refused.
    assert!(matches!(
        scheduler.handle_trigger("orders", Some(0)).await,
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

    // A firing without an interval start is refused.
    assert!(matches!(
        scheduler.handle_trigger("orders", None).await,
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

    // A start request starts the partitions it lists, once.
    let partitions = [Partition::new("20260901").unwrap()];
    let started = scheduler.start_runs("orders", &partitions).await.unwrap();
    assert_eq!(started, partitions);
    let started = scheduler.start_runs("orders", &partitions).await.unwrap();
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

#[tokio::test(flavor = "multi_thread")]
async fn a_request_in_the_store_is_applied_once_and_its_record_is_readable() {
    let h = Harness::start("2026-09-16T12:00:00Z").await;
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("requests");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let marker = dir.join("marker");
    // The node fails with a permanent exit code until the marker exists.
    let definition = format!(
        r#"
[graph]
name = "orders"
partition = "daily"

[[node]]
name = "extract"
produces = "orders_raw"
operator = "subprocess"
[node.params]
argv = ["sh", "-c", "cat >/dev/null; test -f {} || exit 3; printf '{{}}'"]
"#,
        marker.display()
    );
    h.definitions.publish(&definition).await.unwrap();
    let (daemon, _, pools) = h.daemon();
    let stop = CancellationToken::new();
    let pool_handles = pools.spawn(&stop);
    daemon.sync().await.unwrap();
    let requests = h.requests();
    let partition = |key: &str| Partition::new(key).unwrap();
    let id = |n: u64| RequestId::new(format!("req-{n}")).unwrap();

    // A start request runs the partitions without a graph run, and the pass
    // removes the object.
    let start = Request::Start {
        graph: "orders".into(),
        partitions: vec![partition("20260915"), partition("20260916")],
    };
    requests.submit(&id(1), &start).await.unwrap();
    let report = daemon.apply_requests().await.unwrap();
    assert_eq!(
        report.applied,
        [(
            id(1),
            RequestOutcome::Started {
                partitions: vec![partition("20260915"), partition("20260916")]
            }
        )]
    );
    assert!(requests.list().await.unwrap().is_empty());
    for key in ["20260915", "20260916"] {
        assert_eq!(h.graph_run(key).await.unwrap().state, GraphRunState::Active);
        h.wait_for_extract(key, RecordStatus::Failed).await;
    }
    requests
        .submit(
            &id(2),
            &Request::Start {
                graph: "orders".into(),
                partitions: vec![partition("20260915"), partition("20260917")],
            },
        )
        .await
        .unwrap();
    let report = daemon.apply_requests().await.unwrap();
    assert_eq!(
        report.applied[0].1,
        RequestOutcome::Started {
            partitions: vec![partition("20260917")]
        }
    );

    // A rerun request submits the next count, and its record commits with
    // the submit.
    std::fs::write(&marker, b"").unwrap();
    h.clock.advance(Duration::from_secs(60));
    let rerun = Request::Rerun {
        graph: "orders".into(),
        partition: partition("20260915"),
        node: "extract".into(),
    };
    requests.submit(&id(3), &rerun).await.unwrap();
    let report = daemon.apply_requests().await.unwrap();
    assert_eq!(
        report.applied,
        [(
            id(3),
            RequestOutcome::Rerun {
                run_id: "orders-20260915-extract-r1".into()
            }
        )]
    );
    let record = h
        .wait_for_extract("20260915", RecordStatus::Succeeded)
        .await;
    assert_eq!(record.rerun, 1);
    let bytes = h.queue.kv_get(&request_key(&id(3))).await.unwrap().unwrap();
    let recorded = swale::RequestRecord::from_bytes(&bytes).unwrap();
    assert_eq!(recorded.request, rerun);
    assert_eq!(recorded.handled_at_ms, ms("2026-09-16T12:01:00Z"));

    // The object of an applied request, present again after a crash between
    // the record and the removal, is removed without a second application.
    h.clock.advance(Duration::from_secs(60));
    requests.submit(&id(3), &rerun).await.unwrap();
    let report = daemon.apply_requests().await.unwrap();
    assert!(report.applied.is_empty());
    assert!(requests.list().await.unwrap().is_empty());
    let bytes = h.queue.kv_get(&request_key(&id(3))).await.unwrap().unwrap();
    assert_eq!(swale::RequestRecord::from_bytes(&bytes).unwrap(), recorded);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        h.wait_for_extract("20260915", RecordStatus::Succeeded)
            .await
            .rerun,
        1
    );

    // A rerun of the succeeded node is admitted at the next count.
    requests.submit(&id(4), &rerun).await.unwrap();
    let report = daemon.apply_requests().await.unwrap();
    assert_eq!(
        report.applied,
        [(
            id(4),
            RequestOutcome::Rerun {
                run_id: "orders-20260915-extract-r2".into()
            }
        )]
    );

    // A refused request has its reason in the record. An object that is not
    // a request is removed without a record.
    let refused = [
        (
            id(5),
            Request::Rerun {
                graph: "orders".into(),
                partition: partition("20260915"),
                node: "nope".into(),
            },
        ),
        (
            id(6),
            Request::Start {
                graph: "nope".into(),
                partitions: vec![partition("20260915")],
            },
        ),
        (
            id(7),
            Request::Cancel {
                graph: "orders".into(),
                partition: partition("20260901"),
            },
        ),
    ];
    for (id, request) in &refused {
        requests.submit(id, request).await.unwrap();
    }
    h.objects
        .put(
            &ObjectPath::from("requests/not-a-request"),
            b"nope".to_vec().into(),
        )
        .await
        .unwrap();
    let report = daemon.apply_requests().await.unwrap();
    let reasons: Vec<String> = report
        .applied
        .iter()
        .map(|(_, outcome)| match outcome {
            RequestOutcome::Refused { reason } => reason.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        reasons,
        [
            "graph `orders` does not have a node `nope`",
            "graph `nope` does not have an adopted definition",
            "graph `orders` does not have an active run for partition `20260901`",
        ]
    );
    assert!(requests.list().await.unwrap().is_empty());
    assert!(
        h.queue
            .kv_get(b"swale/requests/not-a-request")
            .await
            .unwrap()
            .is_none()
    );

    // A cancel request in the same pass as the start of its graph run
    // cancels the run.
    requests
        .submit(
            &id(8),
            &Request::Start {
                graph: "orders".into(),
                partitions: vec![partition("20260918")],
            },
        )
        .await
        .unwrap();
    requests
        .submit(
            &id(9),
            &Request::Cancel {
                graph: "orders".into(),
                partition: partition("20260918"),
            },
        )
        .await
        .unwrap();
    let report = daemon.apply_requests().await.unwrap();
    assert_eq!(report.applied[1], (id(9), RequestOutcome::Cancelled));
    assert_eq!(
        h.graph_run("20260918").await.unwrap().state,
        GraphRunState::Cancelled
    );

    // Another process reads the record through the status reader, once the
    // writer flushed it.
    let read = common::wait_until("no reader saw the request record", async || {
        let reader =
            StatusReader::open(h.objects.clone(), common::QUEUE_PATH, h.definitions.clone())
                .await
                .unwrap();
        let record = reader.request(&id(3)).await.unwrap();
        reader.close().await.unwrap();
        record
    })
    .await;
    assert_eq!(read, recorded);

    stop.cancel();
    for handle in pool_handles {
        let _ = handle.wait().await;
    }
}
