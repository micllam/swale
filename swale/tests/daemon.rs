//! The daemon on an in-memory store with the mock clock: adoption, the catch-up
//! of a new graph, a cron firing, the backfill after downtime and the requests
//! of another process.

use std::sync::Arc;
use std::time::Duration;

use swale::JsonBytes;
use swale::records::{
    EXPIRY_PREFIX, Expiring, GraphRunRecord, GraphRunState, NodeRecord, RecordStatus,
    RequestOutcome, graph_key, graph_run_key, parse_expiry_key, request_key,
};
use swale::retention::ExpireReport;
use swale::scheduler::{Error, firing_headers};
use swale::{
    Daemon, DaemonOptions, DefinitionStore, GraphRecord, OperatorSet, Partition, Pools,
    REQUESTS_QUEUE, RecordHook, Request, RequestId, RequestRecord, RequestState, Scheduler,
    SchedulerOptions, StatusReader, TRIGGERS_QUEUE,
};
use taquba::object_store::path::Path as ObjectPath;
use taquba::object_store::{ObjectStore, ObjectStoreExt};
use taquba::{
    EnqueueEntry, EnqueueOptions, InboxSender, KvOrder, MockClock, Queue, Rejection, WorkerHandle,
    default_inbox_path,
};
use taquba_cron::{Backfill, BackfillStart, Schedule};
use tokio_util::sync::CancellationToken;

mod common;

/// A scheduled graph of one subprocess node in `pool`, which produces `asset`,
/// with a catch-up window of three days.
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

    fn inbox(&self) -> InboxSender {
        InboxSender::new(self.objects.clone(), default_inbox_path(common::QUEUE_PATH))
    }

    /// Sends `request` with `id`, runs a pass of the inbox and waits for the
    /// outcome in the request record. The scheduler must run.
    async fn request(&self, id: &RequestId, request: &Request) -> RequestOutcome {
        request.send(&self.inbox(), id).await.unwrap();
        self.queue.apply_inbox_now().await.unwrap();
        self.outcome(id).await
    }

    /// The outcome in the record of the request `id`, once the record exists.
    async fn outcome(&self, id: &RequestId) -> RequestOutcome {
        common::wait_until("the request was never applied", async || {
            let bytes = self.queue.view().kv_get(&request_key(id)).await.unwrap()?;
            Some(RequestRecord::from_bytes(&bytes).unwrap().outcome)
        })
        .await
    }

    /// Spawns the scheduler with the poll interval of ten milliseconds.
    fn spawn_scheduler(
        &self,
        scheduler: &Arc<Scheduler>,
        stop: &CancellationToken,
    ) -> WorkerHandle<Result<(), Error>> {
        scheduler.clone().spawn(
            SchedulerOptions {
                concurrency: 2,
                poll_interval: Duration::from_millis(10),
                reconcile_interval: Duration::from_secs(3600),
            },
            stop.clone().cancelled_owned(),
        )
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
        ));
        (daemon, scheduler, pools)
    }

    fn spawn(
        &self,
    ) -> (
        CancellationToken,
        tokio::task::JoinHandle<Result<(), Error>>,
    ) {
        self.spawn_with_retention(Duration::from_secs(90 * 86_400))
    }

    fn spawn_with_retention(
        &self,
        retention: Duration,
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
            retention,
        };
        let handle = tokio::spawn(async move { daemon.run(options, shutdown).await });
        (stop, handle)
    }

    async fn graph_run(&self, partition: &str) -> Option<GraphRunRecord> {
        let key = graph_run_key("orders", &Partition::new(partition).unwrap());
        self.queue
            .view()
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
            .view()
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
                let bytes = self.queue.view().kv_get(key.as_bytes()).await.unwrap()?;
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

    // The cron scheduler rejects the window of `far`, which is beyond the range
    // of its time type. The daemon registers the set without it.
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

    // An edit of the schedule applies on the running daemon. The entry of the
    // new expression resumes at the watermark, so its firing of 00:00 on 17
    // September runs the partition of the interval start, 16 September.
    let edit = h
        .definitions
        .publish(&definition_text(
            "orders",
            "0 0 * * *",
            "orders_raw",
            "default",
        ))
        .await
        .unwrap();
    h.clock.advance(Duration::from_secs(22 * 3600));
    h.wait_for_complete("20260916").await;
    assert_eq!(h.graph_run("20260916").await.unwrap().definition, edit.hash);

    // Two firings pass while the process is down, and each starts a partition
    // without a graph run.
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

    // The adoption writes the graph record, and the cron entry has the catch-up
    // window.
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

    // Another tool writes a pointer object without the publish check. The pass
    // refuses its definition, which produces an asset of `orders`.
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
async fn a_request_through_the_inbox_is_applied_once_and_its_state_is_readable() {
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
    let (daemon, scheduler, pools) = h.daemon();
    let stop = CancellationToken::new();
    let pool_stop = CancellationToken::new();
    let pool_handles = pools.spawn(&pool_stop);
    let scheduler_handle = h.spawn_scheduler(&scheduler, &stop);
    daemon.sync().await.unwrap();
    let partition = |key: &str| Partition::new(key).unwrap();
    let id = |n: u64| RequestId::new(format!("req-{n}")).unwrap();

    // A start request runs the partitions without a graph run.
    let start = Request::Start {
        graph: "orders".into(),
        partitions: vec![partition("20260915"), partition("20260916")],
    };
    assert_eq!(
        h.request(&id(1), &start).await,
        RequestOutcome::Started {
            partitions: vec![partition("20260915"), partition("20260916")]
        }
    );
    for key in ["20260915", "20260916"] {
        assert!(h.graph_run(key).await.is_some(), "{key}");
        h.wait_for_extract(key, RecordStatus::Failed).await;
    }
    let start = Request::Start {
        graph: "orders".into(),
        partitions: vec![partition("20260915"), partition("20260917")],
    };
    assert_eq!(
        h.request(&id(2), &start).await,
        RequestOutcome::Started {
            partitions: vec![partition("20260917")]
        }
    );

    // A rerun request submits the next count, and its record commits with the
    // submit.
    std::fs::write(&marker, b"").unwrap();
    h.clock.advance(Duration::from_secs(60));
    let rerun = Request::Rerun {
        graph: "orders".into(),
        partition: partition("20260915"),
        node: "extract".into(),
    };
    assert_eq!(
        h.request(&id(3), &rerun).await,
        RequestOutcome::Rerun {
            run_id: "orders-20260915-extract-r1".into()
        }
    );
    let record = h
        .wait_for_extract("20260915", RecordStatus::Succeeded)
        .await;
    assert_eq!(record.rerun, 1);
    let bytes = h
        .queue
        .view()
        .kv_get(&request_key(&id(3)))
        .await
        .unwrap()
        .unwrap();
    let recorded = RequestRecord::from_bytes(&bytes).unwrap();
    assert_eq!(recorded.request, rerun);
    assert_eq!(recorded.handled_at_ms, ms("2026-09-16T12:01:00Z"));

    // A request job delivered again after a crash before its settlement returns
    // the record without a second submit.
    h.clock.advance(Duration::from_secs(60));
    assert_eq!(
        scheduler.handle_request(&id(3), &rerun).await.unwrap(),
        recorded
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        h.wait_for_extract("20260915", RecordStatus::Succeeded)
            .await
            .rerun,
        1
    );

    // A rerun of the succeeded node is admitted at the next count.
    assert_eq!(
        h.request(&id(4), &rerun).await,
        RequestOutcome::Rerun {
            run_id: "orders-20260915-extract-r2".into()
        }
    );

    // A refused request has its reason in the record.
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
    let mut reasons = Vec::new();
    for (id, request) in &refused {
        match h.request(id, request).await {
            RequestOutcome::Refused { reason } => reasons.push(reason),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(
        reasons,
        [
            "graph `orders` does not have a node `nope`",
            "graph `nope` does not have an adopted definition",
            "graph `orders` does not have an active run for partition `20260901`",
        ]
    );

    // A request job whose payload is not a request is dead-lettered, and the
    // inbox rejects an entry that does not decode.
    let entry = EnqueueEntry {
        payload: b"nope".to_vec(),
        options: EnqueueOptions {
            id_override: Some(id(10).to_string()),
            ..EnqueueOptions::default()
        },
    };
    h.inbox()
        .send_with_id(REQUESTS_QUEUE, id(10).as_str(), entry)
        .await
        .unwrap();
    let path = format!(
        "{}/v1/{REQUESTS_QUEUE}/enqueue/{}.{}",
        default_inbox_path(common::QUEUE_PATH),
        ulid::Ulid::new(),
        id(11)
    );
    h.objects
        .put(&ObjectPath::from(path), b"nope".to_vec().into())
        .await
        .unwrap();
    h.queue.apply_inbox_now().await.unwrap();

    // With the pools stopped, the root node of a started graph run stays in the
    // queue. A cancel request sent after the start cancels the run.
    pool_stop.cancel();
    for handle in pool_handles {
        let _ = handle.wait().await;
    }
    let start = Request::Start {
        graph: "orders".into(),
        partitions: vec![partition("20260918")],
    };
    let cancel = Request::Cancel {
        graph: "orders".into(),
        partition: partition("20260918"),
    };
    start.send(&h.inbox(), &id(8)).await.unwrap();
    cancel.send(&h.inbox(), &id(9)).await.unwrap();
    h.queue.apply_inbox_now().await.unwrap();
    assert_eq!(h.outcome(&id(9)).await, RequestOutcome::Cancelled);
    assert_eq!(
        h.graph_run("20260918").await.unwrap().state,
        GraphRunState::Cancelled
    );

    // Another process reads the state of each request through the status
    // reader, once the writer flushed it.
    let states = common::wait_until("no reader saw the request states", async || {
        let reader = StatusReader::open(
            h.objects.clone(),
            "",
            common::QUEUE_PATH,
            h.definitions.clone(),
        )
        .await
        .unwrap();
        let mut states = Vec::new();
        for n in [3, 10, 11] {
            states.push(reader.request(&id(n)).await.unwrap());
        }
        reader.close().await.unwrap();
        let waiting = |state: &RequestState| matches!(state, RequestState::Waiting { .. });
        (!states.iter().any(waiting)).then_some(states)
    })
    .await;
    assert_eq!(states[0], RequestState::Applied(recorded));
    assert!(
        matches!(&states[1], RequestState::Failed(error) if error.starts_with("the payload is not a request")),
        "{:?}",
        states[1]
    );
    assert!(
        matches!(
            &states[2],
            RequestState::Rejected(Rejection::Malformed { .. })
        ),
        "{:?}",
        states[2]
    );

    stop.cancel();
    scheduler_handle.wait().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retention_pass_removes_the_records_of_settled_runs_and_of_requests_past_the_window() {
    let h = Harness::start("2026-09-16T12:00:00Z").await;
    h.definitions
        .publish(&definition_text(
            "orders",
            "0 2 * * *",
            "orders_raw",
            "default",
        ))
        .await
        .unwrap();
    let (daemon, scheduler, pools) = h.daemon();
    let stop = CancellationToken::new();
    let pool_handles = pools.spawn(&stop);
    let scheduler_handle = h.spawn_scheduler(&scheduler, &stop);
    daemon.sync().await.unwrap();
    let partition = |key: &str| Partition::new(key).unwrap();
    let id = |n: u64| RequestId::new(format!("req-{n}")).unwrap();
    let start = |keys: &[&str]| Request::Start {
        graph: "orders".into(),
        partitions: keys.iter().map(|key| partition(key)).collect(),
    };
    let retention = Duration::from_secs(90 * 86_400);
    let record_key = |key: &str| format!("swale/assets/orders_raw/{key}");
    let has_key = async |key: &[u8]| h.queue.view().kv_get(key).await.unwrap().is_some();
    let entries = async || {
        let page = h
            .queue
            .view()
            .kv_scan(EXPIRY_PREFIX.as_bytes(), .., KvOrder::Ascending, 100)
            .await
            .unwrap();
        page.entries
            .into_iter()
            .map(|(key, _)| parse_expiry_key(&key).unwrap())
            .collect::<Vec<_>>()
    };
    let t0 = ms("2026-09-16T12:00:00Z");
    let day = 86_400_000;
    let run_entry = |time: u64, key: &str| {
        (
            time,
            Expiring::Run {
                graph: "orders".into(),
                partition: partition(key),
            },
        )
    };
    let request_entry = |time: u64, n: u64| (time, Expiring::Request(id(n)));

    // Two runs settle at the time of the pass that started them, and each
    // settle and each request has an index entry at its time.
    h.request(&id(1), &start(&["20260910", "20260911"])).await;
    for key in ["20260910", "20260911"] {
        h.wait_for_complete(key).await;
        assert_eq!(h.graph_run(key).await.unwrap().settled_at_ms, Some(t0));
    }
    assert_eq!(
        entries().await,
        [
            request_entry(t0, 1),
            run_entry(t0, "20260910"),
            run_entry(t0, "20260911"),
        ]
    );
    h.clock.advance(Duration::from_secs(30 * 86_400));
    h.request(&id(2), &start(&["20260912"])).await;
    h.wait_for_complete("20260912").await;

    // A rerun leaves the entry of the first settle, and the settle after it
    // records the new time with a second entry.
    h.clock.advance(Duration::from_secs(30 * 86_400));
    let rerun = Request::Rerun {
        graph: "orders".into(),
        partition: partition("20260912"),
        node: "extract".into(),
    };
    h.request(&id(3), &rerun).await;
    h.wait_for_complete("20260912").await;
    assert_eq!(
        h.graph_run("20260912").await.unwrap().settled_at_ms,
        Some(t0 + 60 * day)
    );
    assert_eq!(
        entries().await[3..],
        [
            request_entry(t0 + 30 * day, 2),
            run_entry(t0 + 30 * day, "20260912"),
            request_entry(t0 + 60 * day, 3),
            run_entry(t0 + 60 * day, "20260912"),
        ]
    );

    // At the window a pass removes the records of the two settled runs and the
    // request record of their start, with their entries. The younger entries
    // stay, and a second pass returns without a read.
    h.clock.advance(Duration::from_secs(30 * 86_400));
    let report = scheduler.expire(retention).await.unwrap();
    assert_eq!(
        report,
        ExpireReport {
            runs: 2,
            requests: 1
        }
    );
    for key in ["20260910", "20260911"] {
        assert!(h.graph_run(key).await.is_none(), "{key}");
        assert!(!has_key(record_key(key).as_bytes()).await, "{key}");
    }
    assert!(h.graph_run("20260912").await.is_some());
    assert!(has_key(record_key("20260912").as_bytes()).await);
    assert!(!has_key(&request_key(&id(1))).await);
    assert!(has_key(&request_key(&id(2))).await);
    assert_eq!(entries().await.len(), 4);
    assert_eq!(
        scheduler.expire(retention).await.unwrap(),
        ExpireReport::default()
    );

    // The entry of the first settle of the rerun run is stale when it comes
    // due: the pass removes the entry and keeps the run.
    h.clock.advance(Duration::from_secs(30 * 86_400));
    let report = scheduler.expire(retention).await.unwrap();
    assert_eq!(
        report,
        ExpireReport {
            runs: 0,
            requests: 1
        }
    );
    assert!(h.graph_run("20260912").await.is_some());
    assert!(!has_key(&request_key(&id(2))).await);
    assert_eq!(
        entries().await,
        [
            request_entry(t0 + 60 * day, 3),
            run_entry(t0 + 60 * day, "20260912"),
        ]
    );

    // A start of a partition whose records were removed runs the graph again.
    assert_eq!(
        h.request(&id(4), &start(&["20260910"])).await,
        RequestOutcome::Started {
            partitions: vec![partition("20260910")]
        }
    );
    h.wait_for_complete("20260910").await;
    assert_eq!(
        h.wait_for_extract("20260910", RecordStatus::Succeeded)
            .await
            .rerun,
        0
    );
    stop.cancel();
    for handle in pool_handles {
        let _ = handle.wait().await;
    }
    scheduler_handle.wait().await.unwrap();

    // The daemon loop runs the pass after its sync pass, and a process start
    // reads the index once.
    h.clock.advance(Duration::from_secs(3600));
    let (stop, handle) = h.spawn_with_retention(Duration::from_secs(3600));
    common::wait_until("the daemon never expired the run", async || {
        h.graph_run("20260912").await.is_none().then_some(())
    })
    .await;
    stop.cancel();
    handle.await.unwrap().unwrap();
}
