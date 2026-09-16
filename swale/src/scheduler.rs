//! The scheduler: it starts graph runs, submits a node when its upstreams
//! satisfy its trigger rule, and settles the state of a graph run.
//!
//! Every task instance runs on the [`WorkflowRuntime`] of its pool
//! ([`Pools`]). The terminal hook of each pool writes the node's record and
//! enqueues an [`Event`], and the [`Scheduler`] is the [`Worker`] of the
//! events queue. Every submit is idempotent on the deterministic run id, so a
//! redelivered event and a repeated reconciler pass are harmless.
//!
//! The reconciler ([`Scheduler::reconcile`]) applies the same readiness rule
//! to every node of every active graph run, cancels the active runs of a
//! cancelled graph run and writes the final state. A lost event delays a
//! graph run by one reconciler interval at most.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use taquba::object_store::ObjectStore;
use taquba::{
    Clock, JobRecord, LeaseHandle, PermanentFailure, Queue, Worker, WorkerError, WorkerHandle,
};
use taquba_workflow::{RunId, RunOptions, RunSpec, RunState, RunnerHandle, WorkflowRuntime};
use tokio_util::sync::CancellationToken;

use crate::dispatch::Dispatch;
use crate::graph::{Graph, Node, TriggerRule};
use crate::hook::{EVENTS_QUEUE, Event, RecordHook};
use crate::input::TaskInput;
use crate::operator::OperatorSet;
use crate::partition::Partition;
use crate::records::{self, GraphRunRecord, GraphRunState, NodeRecord, RecordStatus};
use crate::task::TaskIdentity;

/// A failure of the scheduler.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The queue failed.
    #[error(transparent)]
    Queue(#[from] taquba::Error),
    /// The workflow runtime failed.
    #[error(transparent)]
    Workflow(#[from] taquba_workflow::Error),
    /// A record is not valid JSON.
    #[error("record `{key}` is not a record: {source}")]
    Record {
        /// The key.
        key: String,
        /// The parser's error.
        source: serde_json::Error,
    },
    /// The graph run records a definition the store does not have.
    #[error("definition `{0}` is not in the definition store")]
    UnknownDefinition(String),
    /// The pool of a node does not have a runtime.
    #[error("node `{node}`: pool `{pool}` does not have a runtime")]
    UnknownPool {
        /// The node.
        node: String,
        /// The pool.
        pool: String,
    },
    /// The graph run does not exist.
    #[error("graph `{graph}` does not have a run for partition `{partition}`")]
    UnknownGraphRun {
        /// The graph.
        graph: String,
        /// The partition.
        partition: Partition,
    },
    /// The node is not in the graph.
    #[error("graph `{graph}` does not have a node `{node}`")]
    UnknownNode {
        /// The graph.
        graph: String,
        /// The node.
        node: String,
    },
}

/// The definitions the process runs, by hash.
#[derive(Debug, Default)]
pub struct DefinitionStore {
    graphs: RwLock<HashMap<String, Arc<Graph>>>,
}

impl DefinitionStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `graph` at `hash` (see [`crate::definition::hash`]).
    pub fn insert(&self, hash: impl Into<String>, graph: Graph) -> Arc<Graph> {
        let graph = Arc::new(graph);
        self.graphs
            .write()
            .expect("the definition store is not poisoned")
            .insert(hash.into(), graph.clone());
        graph
    }

    /// The graph at `hash`.
    pub fn get(&self, hash: &str) -> Option<Arc<Graph>> {
        self.graphs
            .read()
            .expect("the definition store is not poisoned")
            .get(hash)
            .cloned()
    }
}

/// The runtime of a pool.
pub type PoolRuntime = WorkflowRuntime<Dispatch, RecordHook>;

/// One [`WorkflowRuntime`] per pool, all over one queue and one store.
pub struct Pools {
    runtimes: HashMap<String, PoolRuntime>,
}

/// Builds a [`Pools`].
pub struct PoolsBuilder {
    queue: Arc<Queue>,
    store: Arc<dyn ObjectStore>,
    dispatch: Dispatch,
    hook: RecordHook,
    poll_interval: Duration,
    store_prefix: String,
    pools: Vec<(String, usize)>,
}

impl PoolsBuilder {
    /// Adds the pool `name` with `max_concurrent_steps` steps at a time. Its
    /// queue is `swale-pool-{name}`, and its memos are at
    /// `swale-memo-{name}` within the store prefix.
    pub fn pool(mut self, name: impl Into<String>, max_concurrent_steps: usize) -> Self {
        self.pools.push((name.into(), max_concurrent_steps));
        self
    }

    /// The poll interval of every pool's step worker.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// The path within the store that every pool writes its memos under,
    /// for a store whose queue is opened at a prefix. Empty by default.
    pub fn store_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.store_prefix = prefix.into();
        self
    }

    /// Builds the runtimes.
    pub fn build(self) -> Pools {
        let runtimes = self
            .pools
            .into_iter()
            .map(|(name, concurrency)| {
                let runtime = WorkflowRuntime::builder(
                    self.queue.clone(),
                    self.store.clone(),
                    self.dispatch.clone(),
                    self.hook.clone(),
                )
                .queue_name(format!("swale-pool-{name}"))
                .memo_prefix(if self.store_prefix.is_empty() {
                    format!("swale-memo-{name}")
                } else {
                    format!("{}/swale-memo-{name}", self.store_prefix)
                })
                .max_concurrent_steps(concurrency)
                .poll_interval(self.poll_interval)
                .build();
                (name, runtime)
            })
            .collect();
        Pools { runtimes }
    }
}

impl Pools {
    /// Starts building pools over `queue` and `store`, with `operators` as
    /// the dispatch and `hook` as the terminal hook of every pool.
    pub fn builder(
        queue: Arc<Queue>,
        store: Arc<dyn ObjectStore>,
        operators: Arc<OperatorSet>,
        hook: RecordHook,
    ) -> PoolsBuilder {
        PoolsBuilder {
            queue,
            store,
            dispatch: Dispatch::new(operators),
            hook,
            poll_interval: Duration::from_millis(250),
            store_prefix: String::new(),
            pools: Vec::new(),
        }
    }

    /// The runtime of the pool `name`.
    pub fn runtime(&self, name: &str) -> Option<&PoolRuntime> {
        self.runtimes.get(name)
    }

    /// The pool names in arbitrary order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.runtimes.keys().map(String::as_str)
    }

    /// Spawns the step worker of every pool. Each stops when `shutdown` is
    /// cancelled.
    pub fn spawn(&self, shutdown: &CancellationToken) -> Vec<RunnerHandle> {
        self.runtimes
            .values()
            .map(|runtime| runtime.spawn(shutdown.clone().cancelled_owned()))
            .collect()
    }
}

/// The settings of [`Scheduler::run`].
#[derive(Debug, Clone)]
pub struct SchedulerOptions {
    /// Events handled at a time.
    pub concurrency: usize,
    /// The poll interval of the events worker.
    pub poll_interval: Duration,
    /// The time between reconciler passes.
    pub reconcile_interval: Duration,
}

impl Default for SchedulerOptions {
    fn default() -> Self {
        SchedulerOptions {
            concurrency: 4,
            poll_interval: Duration::from_millis(250),
            reconcile_interval: Duration::from_secs(60),
        }
    }
}

/// The result of [`Scheduler::start_run`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartOutcome {
    /// Whether this call created the graph run. `false` when a run for the
    /// partition existed, in which case nothing was submitted.
    pub started: bool,
    /// The run ids of the root nodes submitted.
    pub submitted: Vec<RunId>,
}

/// Counts of one reconciler pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Active graph runs visited.
    pub active_runs: usize,
    /// Nodes newly submitted. A submit of a run that was active is not
    /// counted.
    pub submitted: usize,
    /// Graph runs whose state was written.
    pub settled: usize,
    /// Task instance runs cancelled for cancelled graph runs.
    pub cancelled: usize,
}

/// The scheduler.
pub struct Scheduler {
    queue: Arc<Queue>,
    definitions: Arc<DefinitionStore>,
    pools: Arc<Pools>,
    clock: Arc<dyn Clock>,
    events_queue: String,
}

impl Scheduler {
    /// A scheduler over `queue`, running the graphs of `definitions` on
    /// `pools`, with [`EVENTS_QUEUE`] as its events queue.
    pub fn new(queue: Arc<Queue>, definitions: Arc<DefinitionStore>, pools: Arc<Pools>) -> Self {
        let clock = queue.clock();
        Scheduler {
            queue,
            definitions,
            pools,
            clock,
            events_queue: EVENTS_QUEUE.to_string(),
        }
    }

    /// Starts the graph run of the definition `hash` for `partition`: writes
    /// the graph run record and submits the root nodes. A second call for
    /// the same graph and partition does not change anything.
    pub async fn start_run(
        &self,
        hash: &str,
        partition: &Partition,
    ) -> Result<StartOutcome, Error> {
        let graph = self.graph(hash)?;
        let record = GraphRunRecord {
            definition: hash.to_string(),
            requested_at_ms: self.clock.now_ms(),
            state: GraphRunState::Active,
        };
        let key = records::graph_run_key(graph.name(), partition);
        if !self
            .queue
            .kv_compare_put(&key, None, &record.to_bytes())
            .await?
        {
            return Ok(StartOutcome {
                started: false,
                submitted: Vec::new(),
            });
        }
        let mut submitted = Vec::new();
        for node in graph.roots() {
            let (run_id, _) = self
                .submit_node(&graph, hash, partition, node, 0, &BTreeMap::new())
                .await?;
            submitted.push(run_id);
        }
        Ok(StartOutcome {
            started: true,
            submitted,
        })
    }

    /// Runs `node` again for `partition` after a failed or cancelled record,
    /// at the next rerun count, when its upstreams satisfy its trigger rule.
    /// The graph run returns to the active state. `None` when the node's
    /// record is absent or succeeded, or when the node is not ready.
    pub async fn rerun(
        &self,
        graph_name: &str,
        partition: &Partition,
        node: &str,
    ) -> Result<Option<RunId>, Error> {
        let key = records::graph_run_key(graph_name, partition);
        let Some((run, bytes)) = self.graph_run(&key).await? else {
            return Err(Error::UnknownGraphRun {
                graph: graph_name.to_string(),
                partition: partition.clone(),
            });
        };
        let graph = self.graph(&run.definition)?;
        let node = graph.node(node).ok_or_else(|| Error::UnknownNode {
            graph: graph_name.to_string(),
            node: node.to_string(),
        })?;
        let Some(record) = self.node_record(&graph, partition, node).await? else {
            return Ok(None);
        };
        if record.status == RecordStatus::Succeeded {
            return Ok(None);
        }
        let upstreams = self.upstream_records(&graph, partition, node).await?;
        if !is_ready(node, &upstreams) {
            return Ok(None);
        }
        if run.state != GraphRunState::Active {
            let active = GraphRunRecord {
                state: GraphRunState::Active,
                ..run
            };
            self.queue
                .kv_compare_put(&key, Some(&bytes), &active.to_bytes())
                .await?;
        }
        let (run_id, _) = self
            .submit_node(
                &graph,
                &record.definition,
                partition,
                node,
                record.rerun + 1,
                &upstreams,
            )
            .await?;
        Ok(Some(run_id))
    }

    /// Cancels the graph run: writes the cancelled state and cancels every
    /// active task instance. `false` when the run does not exist or is not
    /// active.
    pub async fn cancel_run(&self, graph_name: &str, partition: &Partition) -> Result<bool, Error> {
        let key = records::graph_run_key(graph_name, partition);
        let Some((run, bytes)) = self.graph_run(&key).await? else {
            return Ok(false);
        };
        if run.state != GraphRunState::Active {
            return Ok(false);
        }
        let cancelled = GraphRunRecord {
            state: GraphRunState::Cancelled,
            ..run.clone()
        };
        if !self
            .queue
            .kv_compare_put(&key, Some(&bytes), &cancelled.to_bytes())
            .await?
        {
            return Ok(false);
        }
        let graph = self.graph(&run.definition)?;
        self.cancel_active_runs(&graph, partition).await?;
        Ok(true)
    }

    /// Handles the termination of one task instance: submits every downstream
    /// node that is ready and without a record, then settles the graph run.
    pub async fn handle_event(&self, event: &Event) -> Result<(), Error> {
        let key = records::graph_run_key(&event.graph, &event.partition);
        let Some((run, bytes)) = self.graph_run(&key).await? else {
            return Ok(());
        };
        if run.state != GraphRunState::Active {
            return Ok(());
        }
        let graph = self.graph(&run.definition)?;
        let Some(node) = graph.node(&event.node) else {
            return Ok(());
        };
        for name in node.downstreams() {
            let downstream = graph
                .node(name)
                .expect("a downstream name is a node of the graph");
            self.submit_if_ready(&graph, &run.definition, &event.partition, downstream)
                .await?;
        }
        self.settle(&graph, &event.partition, &run, &bytes).await?;
        Ok(())
    }

    /// One reconciler pass over every graph run record.
    pub async fn reconcile(&self) -> Result<ReconcileReport, Error> {
        let mut report = ReconcileReport::default();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let page = self
                .queue
                .kv_scan(
                    records::GRAPH_RUNS_PREFIX.as_bytes(),
                    cursor.as_deref(),
                    256,
                )
                .await?;
            for (key, bytes) in &page.entries {
                let Some((graph_name, partition)) = records::parse_graph_run_key(key) else {
                    continue;
                };
                let run = GraphRunRecord::from_bytes(bytes).map_err(|source| Error::Record {
                    key: String::from_utf8_lossy(key).into_owned(),
                    source,
                })?;
                let Some(graph) = self.definitions.get(&run.definition) else {
                    tracing::warn!(graph = %graph_name, %partition, definition = %run.definition, "graph run records an unknown definition");
                    continue;
                };
                match run.state {
                    GraphRunState::Active => {
                        report.active_runs += 1;
                        for node in graph.nodes() {
                            if let Some((_, true)) = self
                                .submit_if_ready(&graph, &run.definition, &partition, node)
                                .await?
                            {
                                report.submitted += 1;
                            }
                        }
                        if self.settle(&graph, &partition, &run, bytes).await? {
                            report.settled += 1;
                        }
                    }
                    GraphRunState::Cancelled => {
                        report.cancelled += self.cancel_active_runs(&graph, &partition).await?;
                    }
                    GraphRunState::Complete | GraphRunState::Failed => {}
                }
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => return Ok(report),
            }
        }
    }

    /// Runs the events worker and the reconciler until `shutdown` resolves.
    pub async fn run<F: Future<Output = ()>>(
        self: Arc<Self>,
        options: SchedulerOptions,
        shutdown: F,
    ) -> Result<(), Error> {
        let stop = CancellationToken::new();
        let worker = taquba::run_worker_concurrent(
            &self.queue,
            &self.events_queue,
            self.clone(),
            options.concurrency,
            options.poll_interval,
            stop.clone().cancelled_owned(),
        );
        let reconciler = async {
            let mut interval = tokio::time::interval(options.reconcile_interval);
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = self.reconcile().await {
                            tracing::warn!(error = %e, "reconciler pass failed");
                        }
                    }
                    () = stop.cancelled() => return,
                }
            }
        };
        let mut both = std::pin::pin!(async {
            let (worker, ()) = tokio::join!(worker, reconciler);
            worker
        });
        tokio::select! {
            result = &mut both => Ok(result?),
            () = shutdown => {
                stop.cancel();
                Ok(both.await?)
            }
        }
    }

    /// Spawns [`Self::run`] as a task.
    pub fn spawn<F>(
        self: Arc<Self>,
        options: SchedulerOptions,
        shutdown: F,
    ) -> WorkerHandle<Result<(), Error>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        WorkerHandle::spawn(shutdown, move |stop| async move {
            self.run(options, stop.cancelled_owned()).await
        })
    }

    fn graph(&self, hash: &str) -> Result<Arc<Graph>, Error> {
        self.definitions
            .get(hash)
            .ok_or_else(|| Error::UnknownDefinition(hash.to_string()))
    }

    async fn graph_run(&self, key: &[u8]) -> Result<Option<(GraphRunRecord, Vec<u8>)>, Error> {
        let Some(bytes) = self.queue.kv_get(key).await? else {
            return Ok(None);
        };
        let record = GraphRunRecord::from_bytes(&bytes).map_err(|source| Error::Record {
            key: String::from_utf8_lossy(key).into_owned(),
            source,
        })?;
        Ok(Some((record, bytes.to_vec())))
    }

    async fn node_record(
        &self,
        graph: &Graph,
        partition: &Partition,
        node: &Node,
    ) -> Result<Option<NodeRecord>, Error> {
        let key = records::node_record_key(graph.name(), partition, node);
        let Some(bytes) = self.queue.kv_get(&key).await? else {
            return Ok(None);
        };
        NodeRecord::from_bytes(&bytes)
            .map(Some)
            .map_err(|source| Error::Record {
                key: String::from_utf8_lossy(&key).into_owned(),
                source,
            })
    }

    async fn upstream_records(
        &self,
        graph: &Graph,
        partition: &Partition,
        node: &Node,
    ) -> Result<BTreeMap<String, NodeRecord>, Error> {
        let mut records = BTreeMap::new();
        for name in node.upstreams() {
            let upstream = graph
                .node(name)
                .expect("an upstream name is a node of the graph");
            if let Some(record) = self.node_record(graph, partition, upstream).await? {
                records.insert(name.clone(), record);
            }
        }
        Ok(records)
    }

    /// Submits `node` at rerun count 0 when it is without a record and its
    /// upstreams satisfy its trigger rule.
    async fn submit_if_ready(
        &self,
        graph: &Graph,
        hash: &str,
        partition: &Partition,
        node: &Node,
    ) -> Result<Option<(RunId, bool)>, Error> {
        if self.node_record(graph, partition, node).await?.is_some() {
            return Ok(None);
        }
        let upstreams = self.upstream_records(graph, partition, node).await?;
        if !is_ready(node, &upstreams) {
            return Ok(None);
        }
        Ok(Some(
            self.submit_node(graph, hash, partition, node, 0, &upstreams)
                .await?,
        ))
    }

    async fn submit_node(
        &self,
        graph: &Graph,
        hash: &str,
        partition: &Partition,
        node: &Node,
        rerun: u32,
        upstreams: &BTreeMap<String, NodeRecord>,
    ) -> Result<(RunId, bool), Error> {
        let runtime = self
            .pools
            .runtime(node.pool())
            .ok_or_else(|| Error::UnknownPool {
                node: node.name().to_string(),
                pool: node.pool().to_string(),
            })?;
        let identity = TaskIdentity {
            graph: graph.name().to_string(),
            partition: partition.clone(),
            node: node.name().to_string(),
            asset: node.asset().map(str::to_string),
            definition: hash.to_string(),
            rerun,
        };
        let run_id = identity.run_id();
        let outcome = runtime
            .submit(RunSpec {
                run_id: Some(run_id.clone()),
                input: TaskInput::new(node, upstreams).to_bytes(),
                options: RunOptions {
                    headers: identity.headers(),
                    max_attempts_per_step: Some(node.retries() + 1),
                    ..RunOptions::default()
                },
                kv_writes: HashMap::new(),
            })
            .await?;
        if outcome.newly_submitted {
            tracing::info!(run_id = %run_id, pool = node.pool(), "task instance submitted");
        }
        Ok((run_id, outcome.newly_submitted))
    }

    /// Writes the final state of the graph run when it is reached. `true`
    /// when the state was written.
    async fn settle(
        &self,
        graph: &Graph,
        partition: &Partition,
        run: &GraphRunRecord,
        bytes: &[u8],
    ) -> Result<bool, Error> {
        let mut all_succeeded = true;
        let mut any_failed = false;
        for node in graph.nodes() {
            match self.node_record(graph, partition, node).await? {
                Some(record) if record.status == RecordStatus::Succeeded => {}
                Some(record) => {
                    all_succeeded = false;
                    any_failed = true;
                    if self
                        .run_is_active(
                            node,
                            &TaskIdentity {
                                graph: graph.name().to_string(),
                                partition: partition.clone(),
                                node: node.name().to_string(),
                                asset: None,
                                definition: String::new(),
                                rerun: record.rerun + 1,
                            },
                        )
                        .await?
                    {
                        return Ok(false);
                    }
                }
                None => {
                    all_succeeded = false;
                    let upstreams = self.upstream_records(graph, partition, node).await?;
                    let identity = TaskIdentity {
                        graph: graph.name().to_string(),
                        partition: partition.clone(),
                        node: node.name().to_string(),
                        asset: None,
                        definition: String::new(),
                        rerun: 0,
                    };
                    if is_ready(node, &upstreams) || self.run_is_active(node, &identity).await? {
                        return Ok(false);
                    }
                }
            }
        }
        let state = if all_succeeded {
            GraphRunState::Complete
        } else if any_failed {
            GraphRunState::Failed
        } else {
            // No node is ready or active. A node without a record waits for
            // an upstream without a record (a cycle), and the graph checks
            // exclude a cycle.
            GraphRunState::Failed
        };
        let settled = GraphRunRecord {
            state,
            ..run.clone()
        };
        let key = records::graph_run_key(graph.name(), partition);
        let written = self
            .queue
            .kv_compare_put(&key, Some(bytes), &settled.to_bytes())
            .await?;
        if written {
            tracing::info!(graph = graph.name(), %partition, state = ?state, "graph run settled");
        }
        Ok(written)
    }

    async fn run_is_active(&self, node: &Node, identity: &TaskIdentity) -> Result<bool, Error> {
        let Some(runtime) = self.pools.runtime(node.pool()) else {
            return Ok(false);
        };
        Ok(matches!(
            runtime.status(&identity.run_id()).await?,
            Some(status) if !matches!(status.state, RunState::Terminated(_))
        ))
    }

    /// Cancels the task instance run of every node without a succeeded
    /// record: the run at count 0 of a node without a record, and the run at
    /// the next count of a node with a failed or cancelled record.
    async fn cancel_active_runs(
        &self,
        graph: &Graph,
        partition: &Partition,
    ) -> Result<usize, Error> {
        let mut cancelled = 0;
        for node in graph.nodes() {
            let rerun = match self.node_record(graph, partition, node).await? {
                Some(record) if record.status == RecordStatus::Succeeded => continue,
                Some(record) => record.rerun + 1,
                None => 0,
            };
            let Some(runtime) = self.pools.runtime(node.pool()) else {
                continue;
            };
            let identity = TaskIdentity {
                graph: graph.name().to_string(),
                partition: partition.clone(),
                node: node.name().to_string(),
                asset: None,
                definition: String::new(),
                rerun,
            };
            if runtime.cancel(&identity.run_id()).await? {
                cancelled += 1;
            }
        }
        Ok(cancelled)
    }
}

/// Whether the records of a node's upstreams satisfy its trigger rule.
pub fn is_ready(node: &Node, upstreams: &BTreeMap<String, NodeRecord>) -> bool {
    let status = |name: &String| upstreams.get(name).map(|r| r.status);
    match node.trigger_rule() {
        TriggerRule::AllSucceeded => node
            .upstreams()
            .iter()
            .all(|name| status(name) == Some(RecordStatus::Succeeded)),
        TriggerRule::AllDone => node.upstreams().iter().all(|name| status(name).is_some()),
        TriggerRule::OneFailed => node
            .upstreams()
            .iter()
            .any(|name| status(name) == Some(RecordStatus::Failed)),
    }
}

impl Worker for Scheduler {
    async fn process(&self, job: &JobRecord, _lease: &LeaseHandle) -> Result<(), WorkerError> {
        let event = Event::from_bytes(&job.payload)
            .map_err(|e| PermanentFailure::new(format!("the payload is not an event: {e}")))?;
        self.handle_event(&event)
            .await
            .map_err(|e| Box::new(e) as WorkerError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{GraphSpec, NodeKind, NodeSpec};

    fn node(name: &str, consumes: &[&str], rule: Option<TriggerRule>) -> NodeSpec {
        let kind = match rule {
            None => NodeKind::Asset {
                produces: name.to_string(),
                consumes: consumes.iter().map(|s| s.to_string()).collect(),
            },
            Some(trigger_rule) => NodeKind::Task {
                after: consumes.iter().map(|s| s.to_string()).collect(),
                trigger_rule,
            },
        };
        NodeSpec {
            name: name.into(),
            kind,
            operator: "subprocess".into(),
            pool: "default".into(),
            retries: 0,
            params: r#"argv = ["true"]"#.parse().unwrap(),
        }
    }

    fn record(status: RecordStatus) -> NodeRecord {
        NodeRecord {
            status,
            run_id: "r".into(),
            definition: "d".into(),
            rerun: 0,
            terminated_at_ms: 0,
            output: None,
            output_omitted: false,
            error: None,
        }
    }

    #[test]
    fn readiness_follows_the_trigger_rule() {
        let graph = Graph::build(
            GraphSpec {
                name: "g".into(),
                schedule: None,
                catchup: None,
                partitioning: Default::default(),
                nodes: vec![
                    node("a", &[], None),
                    node("b", &[], None),
                    node("asset", &["a", "b"], None),
                    node("done", &["a", "b"], Some(TriggerRule::AllDone)),
                    node("failed", &["a", "b"], Some(TriggerRule::OneFailed)),
                ],
            },
            &OperatorSet::builtin(),
        )
        .unwrap();
        let n = |name: &str| graph.node(name).unwrap();
        let both_ok = BTreeMap::from([
            ("a".to_string(), record(RecordStatus::Succeeded)),
            ("b".to_string(), record(RecordStatus::Succeeded)),
        ]);
        let one_failed = BTreeMap::from([
            ("a".to_string(), record(RecordStatus::Succeeded)),
            ("b".to_string(), record(RecordStatus::Failed)),
        ]);
        let one_missing = BTreeMap::from([("a".to_string(), record(RecordStatus::Succeeded))]);

        assert!(is_ready(n("asset"), &both_ok));
        assert!(!is_ready(n("asset"), &one_failed));
        assert!(!is_ready(n("asset"), &one_missing));

        assert!(is_ready(n("done"), &both_ok));
        assert!(is_ready(n("done"), &one_failed));
        assert!(!is_ready(n("done"), &one_missing));

        assert!(!is_ready(n("failed"), &both_ok));
        assert!(is_ready(n("failed"), &one_failed));
        assert!(!is_ready(n("failed"), &one_missing));

        assert!(is_ready(n("a"), &BTreeMap::new()));
    }
}
