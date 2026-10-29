//! Graph runs end to end on an in-memory store: the subprocess operator, the
//! hook, the events worker and the reconciler. The status of the members of a
//! mapped node is read without a wait between two reads.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures_util::TryStreamExt;
use swale::JsonBytes;
use swale::records::{GraphRunRecord, GraphRunState, NodeRecord, RecordStatus, graph_run_key};
use swale::scheduler::ReconcileReport;
use swale::{
    DefinitionStore, OperatorSet, Partition, Pools, RecordHook, RerunOutcome, Scheduler,
    SchedulerOptions,
};
use taquba::{KvOrder, MockClock, Queue};
use taquba_workflow::{GroupStatus, RunId, RunState};
use tokio_util::sync::CancellationToken;

mod common;

const PARTITION: &str = "20260915";

/// A graph of subprocess nodes. `transform` prints the rows it receives through
/// a template, `load` runs in the `warehouse` pool, and `notify` runs after
/// `transform` regardless of its outcome. `transform_script` is the shell
/// script of `transform`.
fn definition_text(transform_script: &str) -> String {
    format!(
        r#"
[graph]
name = "orders"
partition = "daily"

[[node]]
name = "extract"
produces = "orders_raw"
operator = "subprocess"
[node.params]
argv = ["sh", "-c", "cat >/dev/null; printf '{{\"rows\": 3}}'"]

[[node]]
name = "transform"
produces = "orders_clean"
consumes = ["orders_raw"]
operator = "subprocess"
[node.params]
argv = ["sh", "-c", "{transform_script}", "{{{{ upstream.extract.rows }}}}"]

[[node]]
name = "load"
produces = "orders_warehouse"
consumes = ["orders_clean"]
operator = "subprocess"
pool = "warehouse"
[node.params]
argv = ["sh", "-c", "cat >/dev/null; printf '{{\"loaded\": true}}'"]

[[node]]
name = "notify"
after = ["transform"]
trigger_rule = "all_done"
operator = "subprocess"
[node.params]
argv = ["sh", "-c", "cat >/dev/null; printf '%s' \"$0\"", "{{{{ run.summary }}}}"]
"#
    )
}

const TRANSFORM_OK: &str = "cat >/dev/null; printf '{\\\"rows\\\": %s}' \\\"$0\\\"";

struct Harness {
    queue: Arc<Queue>,
    clock: MockClock,
    scheduler: Arc<Scheduler>,
    pools: Arc<Pools>,
    hash: String,
    stop: CancellationToken,
}

impl Harness {
    async fn start(text: &str, spawn_workers: bool) -> Harness {
        let clock = MockClock::new(1_700_000_000_000);
        let (store, queue) = common::open_queue(clock.clone()).await;
        let operators = Arc::new(OperatorSet::builtin());
        let definitions = Arc::new(DefinitionStore::new(store.clone(), "", operators.clone()));
        let (hash, _) = definitions.put(text).await.unwrap();
        let hook = RecordHook::new(queue.clock());
        let pools = Arc::new(
            Pools::builder(queue.clone(), store, operators, hook)
                .poll_interval(Duration::from_millis(10))
                .member_poll_interval(Duration::ZERO)
                .pool("default", 4)
                .pool("warehouse", 1)
                .build(),
        );
        let scheduler = Arc::new(Scheduler::new(queue.clone(), definitions, pools.clone()));
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
            queue,
            clock,
            scheduler,
            pools,
            hash,
            stop,
        }
    }

    fn partition() -> Partition {
        Partition::new(PARTITION).unwrap()
    }

    async fn graph_run(&self) -> Option<GraphRunRecord> {
        self.queue
            .view()
            .kv_get(&graph_run_key("orders", &Self::partition()))
            .await
            .unwrap()
            .map(|b| GraphRunRecord::from_bytes(&b).unwrap())
    }

    async fn record(&self, key: &str) -> Option<NodeRecord> {
        self.queue
            .view()
            .kv_get(key.as_bytes())
            .await
            .unwrap()
            .map(|b| NodeRecord::from_bytes(&b).unwrap())
    }

    async fn wait_for_state(&self, state: GraphRunState) -> GraphRunRecord {
        common::wait_until(&format!("graph run never reached {state:?}"), async || {
            self.graph_run().await.filter(|run| run.state == state)
        })
        .await
    }

    async fn write_record(
        &self,
        key: &str,
        status: RecordStatus,
        output: Option<serde_json::Value>,
    ) {
        let record = NodeRecord {
            status,
            run_id: "manual".into(),
            definition: self.hash.clone(),
            rerun: 0,
            terminated_at_ms: 0,
            output,
            output_omitted: false,
            error: None,
        };
        self.queue
            .kv_put(key.as_bytes(), &record.to_bytes())
            .await
            .unwrap();
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_graph_run_completes_with_records_and_outputs_flow_through_templates() {
    let h = Harness::start(&definition_text(TRANSFORM_OK), true).await;
    let outcome = h
        .scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();
    assert!(outcome.started);
    assert_eq!(outcome.submitted.len(), 1);
    assert_eq!(outcome.submitted[0].as_str(), "orders-20260915-extract-r0");

    let run = h.wait_for_state(GraphRunState::Complete).await;
    assert_eq!(run.definition, h.hash);
    assert_eq!(run.settled_at_ms, Some(1_700_000_000_000));

    let extract = h.record("swale/assets/orders_raw/20260915").await.unwrap();
    assert_eq!(extract.status, RecordStatus::Succeeded);
    assert_eq!(extract.run_id, "orders-20260915-extract-r0");
    assert_eq!(extract.output, Some(serde_json::json!({"rows": 3})));
    assert_eq!(extract.definition, h.hash);

    // The rows reached `transform` through `{{ upstream.extract.rows }}`.
    let transform = h
        .record("swale/assets/orders_clean/20260915")
        .await
        .unwrap();
    assert_eq!(transform.output, Some(serde_json::json!({"rows": 3})));

    let load = h
        .record("swale/assets/orders_warehouse/20260915")
        .await
        .unwrap();
    assert_eq!(load.run_id, "orders-20260915-load-r0");
    assert_eq!(load.output, Some(serde_json::json!({"loaded": true})));

    let notify = h
        .record("swale/tasks/orders/20260915/notify")
        .await
        .unwrap();
    assert_eq!(notify.status, RecordStatus::Succeeded);
    let summary = notify.output.unwrap();
    assert_eq!(summary["graph"], "orders");
    assert_eq!(summary["upstreams"]["transform"]["status"], "succeeded");
}

fn submitted(run_id: &str) -> RerunOutcome {
    RerunOutcome::Submitted(run_id.parse().unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_start_for_the_same_partition_changes_nothing() {
    let h = Harness::start(&definition_text(TRANSFORM_OK), true).await;
    let first = h
        .scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();
    let second = h
        .scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();
    assert!(first.started);
    assert!(!second.started);
    assert!(second.submitted.is_empty());
    let run = h.wait_for_state(GraphRunState::Complete).await;
    assert_eq!(run.state, GraphRunState::Complete);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dead_lettered_node_fails_the_run_blocks_its_downstream_and_a_rerun_recovers() {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("rerun");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let marker = dir.join("marker");
    // The script fails with a permanent exit code until the marker exists.
    let script = format!(
        "cat >/dev/null; test -f {} || exit 3; printf '{{\\\"rows\\\": %s}}' \\\"$0\\\"",
        marker.display()
    );
    let h = Harness::start(&definition_text(&script), true).await;
    h.scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();

    let run = h.wait_for_state(GraphRunState::Failed).await;
    assert_eq!(run.state, GraphRunState::Failed);
    let transform = h
        .record("swale/assets/orders_clean/20260915")
        .await
        .unwrap();
    assert_eq!(transform.status, RecordStatus::Failed);
    assert_eq!(transform.run_id, "orders-20260915-transform-r0");
    assert!(
        transform
            .error
            .as_deref()
            .unwrap()
            .contains("exited with 3")
    );
    // The dead-letter is in the pool's dead set for inspection.
    let dead = h
        .queue
        .view()
        .dead_jobs("swale-pool-default", None, 10)
        .await
        .unwrap();
    assert_eq!(dead.len(), 1);
    // The asset downstream never started, and the `all_done` task ran.
    assert!(
        h.record("swale/assets/orders_warehouse/20260915")
            .await
            .is_none()
    );
    let notify = h
        .record("swale/tasks/orders/20260915/notify")
        .await
        .unwrap();
    assert_eq!(notify.status, RecordStatus::Succeeded);

    std::fs::write(&marker, b"").unwrap();
    let rerun = h
        .scheduler
        .rerun("orders", &Harness::partition(), "transform")
        .await
        .unwrap();
    assert_eq!(rerun, submitted("orders-20260915-transform-r1"));
    let run = h.wait_for_state(GraphRunState::Complete).await;
    assert_eq!(run.state, GraphRunState::Complete);
    let transform = h
        .record("swale/assets/orders_clean/20260915")
        .await
        .unwrap();
    assert_eq!(transform.run_id, "orders-20260915-transform-r1");
    assert_eq!(transform.rerun, 1);
    let load = h
        .record("swale/assets/orders_warehouse/20260915")
        .await
        .unwrap();
    assert_eq!(load.run_id, "orders-20260915-load-r0");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rerun_of_a_succeeded_node_runs_its_downstreams_again() {
    let h = Harness::start(&definition_text(TRANSFORM_OK), true).await;
    h.scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();
    h.wait_for_state(GraphRunState::Complete).await;

    // The rerun of `extract` expects the next count of `extract`, `transform`
    // and `load`. `notify` has the `all_done` rule and keeps its record.
    let rerun = h
        .scheduler
        .rerun("orders", &Harness::partition(), "extract")
        .await
        .unwrap();
    assert_eq!(rerun, submitted("orders-20260915-extract-r1"));
    let run = h.wait_for_state(GraphRunState::Complete).await;
    assert!(run.expected_reruns.is_empty(), "{run:?}");
    for (key, run_id) in [
        (
            "swale/assets/orders_raw/20260915",
            "orders-20260915-extract-r1",
        ),
        (
            "swale/assets/orders_clean/20260915",
            "orders-20260915-transform-r1",
        ),
        (
            "swale/assets/orders_warehouse/20260915",
            "orders-20260915-load-r1",
        ),
        (
            "swale/tasks/orders/20260915/notify",
            "orders-20260915-notify-r0",
        ),
    ] {
        let record = h.record(key).await.unwrap();
        assert_eq!(record.run_id, run_id);
        assert_eq!(record.status, RecordStatus::Succeeded);
    }

    // A second rerun of the same node expects the count after the new records,
    // and the run is active with the scope listed.
    let rerun = h
        .scheduler
        .rerun("orders", &Harness::partition(), "transform")
        .await
        .unwrap();
    assert_eq!(rerun, submitted("orders-20260915-transform-r2"));
    let run = h.graph_run().await.unwrap();
    assert_eq!(run.state, GraphRunState::Active);
    assert_eq!(run.settled_at_ms, None);
    assert_eq!(
        run.expected_reruns,
        BTreeMap::from([("transform".to_string(), 2), ("load".to_string(), 2)])
    );
    h.wait_for_state(GraphRunState::Complete).await;
    let load = h
        .record("swale/assets/orders_warehouse/20260915")
        .await
        .unwrap();
    assert_eq!(load.run_id, "orders-20260915-load-r2");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_graph_run_submits_no_further_node() {
    let h = Harness::start(&definition_text(TRANSFORM_OK), true).await;
    h.scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();
    assert!(
        h.scheduler
            .cancel_run("orders", &Harness::partition())
            .await
            .unwrap()
    );
    assert!(
        !h.scheduler
            .cancel_run("orders", &Harness::partition())
            .await
            .unwrap()
    );
    let run = h.graph_run().await.unwrap();
    assert_eq!(run.state, GraphRunState::Cancelled);
    assert_eq!(run.settled_at_ms, Some(1_700_000_000_000));

    // Regardless of how `extract` ended, the scheduler ignores its event, and
    // `load` is never submitted.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        h.record("swale/assets/orders_warehouse/20260915")
            .await
            .is_none()
    );
    let load = h
        .pools
        .runtime("warehouse")
        .unwrap()
        .status(&"orders-20260915-load-r0".parse().unwrap())
        .await
        .unwrap();
    assert!(load.is_none(), "{load:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_reconciler_submits_ready_nodes_and_settles_the_run_without_events() {
    // No worker runs, so nothing moves except through the reconciler.
    let h = Harness::start(&definition_text(TRANSFORM_OK), false).await;
    let key = graph_run_key("orders", &Harness::partition());
    let run = GraphRunRecord {
        definition: h.hash.clone(),
        requested_at_ms: 0,
        state: GraphRunState::Active,
        settled_at_ms: None,
        expected_reruns: BTreeMap::new(),
    };
    h.queue.kv_put(&key, &run.to_bytes()).await.unwrap();
    h.write_record(
        "swale/assets/orders_raw/20260915",
        RecordStatus::Succeeded,
        Some(serde_json::json!({"rows": 3})),
    )
    .await;

    // `extract` has a record and no event was enqueued for it, so the
    // reconciler submits `transform`, and the run stays active.
    let report = h.scheduler.reconcile().await.unwrap();
    assert_eq!(
        report,
        ReconcileReport {
            active_runs: 1,
            submitted: 1,
            settled: 0,
            cancelled: 0,
        }
    );
    let status = h
        .pools
        .runtime("default")
        .unwrap()
        .status(&"orders-20260915-transform-r0".parse().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state, RunState::Pending);
    // A second pass does not submit a new run.
    assert_eq!(h.scheduler.reconcile().await.unwrap().submitted, 0);

    // A cancelled run has its pending task instance cancelled, once.
    let cancelled = GraphRunRecord {
        state: GraphRunState::Cancelled,
        ..run.clone()
    };
    h.queue.kv_put(&key, &cancelled.to_bytes()).await.unwrap();
    assert_eq!(h.scheduler.reconcile().await.unwrap().cancelled, 1);
    assert_eq!(h.scheduler.reconcile().await.unwrap().cancelled, 0);
    // The record commits with the cancellation and its reason, without a
    // worker.
    let transform = h
        .record("swale/assets/orders_clean/20260915")
        .await
        .unwrap();
    assert_eq!(transform.status, RecordStatus::Cancelled);
    assert_eq!(
        transform.error.as_deref(),
        Some("the graph run is cancelled")
    );

    // The run is active again. Every node is recorded or waiting, so the pass
    // writes the final state.
    h.queue.kv_put(&key, &run.to_bytes()).await.unwrap();
    h.write_record(
        "swale/assets/orders_clean/20260915",
        RecordStatus::Failed,
        None,
    )
    .await;
    h.write_record(
        "swale/tasks/orders/20260915/notify",
        RecordStatus::Succeeded,
        None,
    )
    .await;
    let report = h.scheduler.reconcile().await.unwrap();
    assert_eq!(report.settled, 1);
    assert_eq!(h.graph_run().await.unwrap().state, GraphRunState::Failed);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rerun_states_the_run_id_submitted_or_why_the_node_was_not_rerun() {
    // No worker runs, so a submitted rerun stays pending.
    let h = Harness::start(&definition_text(TRANSFORM_OK), false).await;
    let key = graph_run_key("orders", &Harness::partition());
    let run = GraphRunRecord {
        definition: h.hash.clone(),
        requested_at_ms: 0,
        state: GraphRunState::Active,
        settled_at_ms: None,
        expected_reruns: BTreeMap::new(),
    };
    h.queue.kv_put(&key, &run.to_bytes()).await.unwrap();
    h.write_record(
        "swale/assets/orders_raw/20260915",
        RecordStatus::Succeeded,
        Some(serde_json::json!({"rows": 3})),
    )
    .await;
    h.write_record(
        "swale/assets/orders_clean/20260915",
        RecordStatus::Succeeded,
        Some(serde_json::json!({"rows": 3})),
    )
    .await;
    let rerun = async |node: &str| {
        h.scheduler
            .rerun("orders", &Harness::partition(), node)
            .await
            .unwrap()
    };

    assert_eq!(rerun("load").await, RerunOutcome::NoRecord);
    assert_eq!(
        rerun("extract").await,
        submitted("orders-20260915-extract-r1")
    );
    // The record of `extract` is superseded until its rerun is recorded, so
    // `transform` is not ready, and the rerun of `extract` is pending.
    assert_eq!(rerun("transform").await, RerunOutcome::NotReady);
    assert_eq!(
        rerun("extract").await,
        RerunOutcome::Active("orders-20260915-extract-r1".parse().unwrap())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_object_exists_node_polls_without_a_worker_until_the_object_exists() {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("object_exists");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let text = format!(
        r#"
[graph]
name = "orders"
partition = "daily"

[[node]]
name = "arrival"
produces = "landing_file"
operator = "object_exists"
[node.params]
url = "file://{dir}/orders-{{{{ partition }}}}.parquet"
interval = "1m"
timeout = "1h"

[[node]]
name = "load"
produces = "loaded"
consumes = ["landing_file"]
operator = "shell"
[node.params]
command = "printf '{{\"size\": {{{{ upstream.arrival.size }}}}}}'"
"#,
        dir = dir.display()
    );
    let h = Harness::start(&text, true).await;
    h.scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();

    // The first poll ends without the object, and the run waits for its second
    // step.
    let runtime = h.pools.runtime("default").unwrap();
    let run_id: RunId = "orders-20260915-arrival-r0".parse().unwrap();
    common::wait_until("the first poll never ended", async || {
        runtime
            .status(&run_id)
            .await
            .unwrap()
            .filter(|status| status.step.number >= 1)
    })
    .await;
    std::fs::write(dir.join("orders-20260915.parquet"), b"12345").unwrap();
    // The second poll is due one interval after the first, by the clock of the
    // store.
    h.clock.advance(Duration::from_secs(61));
    h.wait_for_state(GraphRunState::Complete).await;
    let arrival = h
        .record("swale/assets/landing_file/20260915")
        .await
        .unwrap();
    assert_eq!(arrival.output.as_ref().unwrap()["size"], 5);
    let load = h.record("swale/assets/loaded/20260915").await.unwrap();
    assert_eq!(load.output, Some(serde_json::json!({"size": 5})));
}

/// A graph with the mapped node `fetch` over the output of `list`, whose shell
/// commands are `list` and `fetch`.
fn mapped_definition_text(list: &str, fetch: &str) -> String {
    format!(
        r#"
[graph]
name = "orders"
partition = "daily"

[[node]]
name = "list"
produces = "urls"
operator = "shell"
[node.params]
command = '{list}'

[[node]]
name = "fetch"
produces = "pages"
consumes = ["urls"]
operator = "shell"
each = "{{{{ upstream.list }}}}"
[node.params]
command = '{fetch}'
"#
    )
}

/// An empty directory `name` for the files of a test.
fn scratch_dir(name: &str) -> std::path::PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The lines of `log` in lexical order.
fn sorted_lines(log: &Path) -> Vec<String> {
    let mut lines: Vec<String> = std::fs::read_to_string(log)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();
    lines.sort();
    lines
}

impl Harness {
    /// The status of the run group of the members of `fetch`, or `None` without
    /// a group.
    async fn fetch_group(&self) -> Option<GroupStatus> {
        let group = self
            .pools
            .runtime("default")
            .unwrap()
            .group("orders-20260915-fetch".parse().unwrap());
        match group.status().await {
            Ok(status) => Some(status),
            Err(taquba_workflow::Error::GroupNotFound(_)) => None,
            Err(e) => panic!("{e}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mapped_node_runs_a_member_per_item_and_outputs_them_in_item_order() {
    let text = format!(
        r#"{}
[[node]]
name = "report"
produces = "report"
consumes = ["pages"]
operator = "shell"
[node.params]
command = 'printf "%s" "{{{{ upstream.fetch.2.double }}}}"'
"#,
        mapped_definition_text(
            r#"printf "[{\"n\": 1}, {\"n\": 2}, {\"n\": 3}]""#,
            r#"printf "{\"double\": %s, \"run\": \"{{ run.id }}\"}" $(( {{ item.n }} * 2 ))"#,
        )
    );
    let h = Harness::start(&text, true).await;
    h.scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();
    h.wait_for_state(GraphRunState::Complete).await;

    let fetch = h.record("swale/assets/pages/20260915").await.unwrap();
    assert_eq!(fetch.run_id, "orders-20260915-fetch-r0");
    let member =
        |double: u32| serde_json::json!({"double": double, "run": "orders-20260915-fetch-r0"});
    assert_eq!(
        fetch.output,
        Some(serde_json::json!([member(2), member(4), member(6)]))
    );
    let report = h.record("swale/assets/report/20260915").await.unwrap();
    assert_eq!(report.output, Some(serde_json::json!(6)));

    // A member does not write a record, and the group is removed with the
    // success of the node.
    let records = h
        .queue
        .view()
        .kv_scan(b"swale/assets/", .., KvOrder::Ascending, 100)
        .await
        .unwrap();
    assert_eq!(records.entries.len(), 3);
    assert_eq!(h.fetch_group().await, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_member_fails_the_node_and_a_rerun_runs_the_failed_members_alone() {
    let dir = scratch_dir("mapped_rerun");
    let log = dir.join("log");
    std::fs::write(dir.join("ok-a"), b"").unwrap();
    std::fs::write(dir.join("ok-c"), b"").unwrap();
    let text = mapped_definition_text(
        r#"printf "[\"a\", \"b\", \"c\"]""#,
        &format!(
            r#"echo {{{{ item }}}} >> {dir}/log; test -e {dir}/ok-{{{{ item }}}} && printf "\"{{{{ item }}}}\"""#,
            dir = dir.display()
        ),
    );
    let h = Harness::start(&text, true).await;
    h.scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();
    h.wait_for_state(GraphRunState::Failed).await;

    let fetch = h.record("swale/assets/pages/20260915").await.unwrap();
    assert_eq!(fetch.status, RecordStatus::Failed);
    let error = fetch.error.unwrap();
    assert!(
        error.starts_with("1 of 3 members did not succeed, and member 1 is the first: "),
        "{error}"
    );
    assert_eq!(sorted_lines(&log), ["a", "b", "c"]);
    let group = h.fetch_group().await.unwrap();
    assert_eq!(
        (group.total, group.succeeded, group.failed, group.pending),
        (3, 2, 1, 0)
    );

    std::fs::write(dir.join("ok-b"), b"").unwrap();
    assert_eq!(
        h.scheduler
            .rerun("orders", &Harness::partition(), "fetch")
            .await
            .unwrap(),
        submitted("orders-20260915-fetch-r1")
    );
    h.wait_for_state(GraphRunState::Complete).await;
    let fetch = h.record("swale/assets/pages/20260915").await.unwrap();
    assert_eq!(fetch.rerun, 1);
    assert_eq!(fetch.output, Some(serde_json::json!(["a", "b", "c"])));
    assert_eq!(sorted_lines(&log), ["a", "b", "b", "c"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rerun_after_the_memo_retention_runs_every_member_again() {
    let dir = scratch_dir("mapped_retention");
    let log = dir.join("log");
    std::fs::write(dir.join("ok-a"), b"").unwrap();
    let text = mapped_definition_text(
        r#"printf "[\"a\", \"b\"]""#,
        &format!(
            r#"echo {{{{ item }}}} >> {dir}/log; test -e {dir}/ok-{{{{ item }}}} && printf "\"{{{{ item }}}}\"""#,
            dir = dir.display()
        ),
    );
    let h = Harness::start(&text, true).await;
    h.scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();
    h.wait_for_state(GraphRunState::Failed).await;
    assert_eq!(sorted_lines(&log), ["a", "b"]);

    // The sweep of the pool removes the run result record of each member once
    // the memo retention of seven days ends. The member records remain.
    h.clock.advance(Duration::from_secs(8 * 86_400));
    let group = h
        .pools
        .runtime("default")
        .unwrap()
        .group("orders-20260915-fetch".parse().unwrap());
    common::wait_until("the run result records were never removed", async || {
        let results: Vec<taquba_workflow::MemberResult> =
            group.results().await.unwrap().try_collect().await.unwrap();
        results
            .iter()
            .all(|member| member.outcome.is_none())
            .then_some(())
    })
    .await;
    let status = h.fetch_group().await.unwrap();
    assert_eq!((status.succeeded, status.failed), (1, 1));

    std::fs::write(dir.join("ok-b"), b"").unwrap();
    h.scheduler
        .rerun("orders", &Harness::partition(), "fetch")
        .await
        .unwrap();
    h.wait_for_state(GraphRunState::Complete).await;
    let fetch = h.record("swale/assets/pages/20260915").await.unwrap();
    assert_eq!(fetch.output, Some(serde_json::json!(["a", "b"])));
    // The rerun runs `b`, and then every member of the new group.
    assert_eq!(sorted_lines(&log), ["a", "a", "b", "b", "b"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rerun_with_other_items_replaces_the_group_and_runs_every_member() {
    let dir = scratch_dir("mapped_replace");
    let log = dir.join("log");
    std::fs::write(dir.join("items"), br#"["a", "b"]"#).unwrap();
    std::fs::write(dir.join("ok-a"), b"").unwrap();
    std::fs::write(dir.join("ok-c"), b"").unwrap();
    let text = mapped_definition_text(
        &format!("cat {dir}/items", dir = dir.display()),
        &format!(
            r#"echo {{{{ item }}}} >> {dir}/log; test -e {dir}/ok-{{{{ item }}}} && printf "\"{{{{ item }}}}\"""#,
            dir = dir.display()
        ),
    );
    let h = Harness::start(&text, true).await;
    h.scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();
    h.wait_for_state(GraphRunState::Failed).await;
    assert_eq!(sorted_lines(&log), ["a", "b"]);

    // The rerun of `list` reaches `fetch`, whose failed record is below the
    // expected count.
    std::fs::write(dir.join("items"), br#"["a", "c"]"#).unwrap();
    assert_eq!(
        h.scheduler
            .rerun("orders", &Harness::partition(), "list")
            .await
            .unwrap(),
        submitted("orders-20260915-list-r1")
    );
    h.wait_for_state(GraphRunState::Complete).await;
    let fetch = h.record("swale/assets/pages/20260915").await.unwrap();
    assert_eq!(fetch.output, Some(serde_json::json!(["a", "c"])));
    assert_eq!(sorted_lines(&log), ["a", "a", "b", "c"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancel_of_the_graph_run_cancels_the_members_and_the_retention_pass_removes_the_group() {
    let text = mapped_definition_text(r#"printf "[1, 2]""#, "sleep 30 # {{ item }}");
    let h = Harness::start(&text, true).await;
    h.scheduler
        .start_run(&h.hash, &Harness::partition())
        .await
        .unwrap();
    common::wait_until("the members were never submitted", async || {
        h.fetch_group().await.filter(|group| group.pending == 2)
    })
    .await;
    assert!(
        h.scheduler
            .cancel_run("orders", &Harness::partition())
            .await
            .unwrap()
    );
    common::wait_until("the members were never cancelled", async || {
        h.fetch_group().await.filter(|group| group.cancelled == 2)
    })
    .await;

    let report = h.scheduler.expire(Duration::ZERO).await.unwrap();
    assert_eq!(report.runs, 1);
    assert_eq!(h.fetch_group().await, None);
    assert!(h.graph_run().await.is_none());
}
