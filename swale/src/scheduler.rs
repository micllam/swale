//! The scheduler: it starts graph runs, submits a node when its upstreams
//! satisfy its trigger rule, and settles the state of a graph run.
//!
//! Every task instance runs on the [`WorkflowRuntime`] of its pool
//! ([`Pools`]). The terminal hook of each pool writes the node's record and
//! enqueues an [`Event`], and the [`Scheduler`] is the [`Worker`] of the
//! events queue. Every submit is idempotent on the deterministic run id, so a
//! redelivered event and a repeated reconciler pass are harmless.
//!
//! A job on the triggers queue ([`TRIGGERS_QUEUE`]) is a cron firing without
//! a payload: the `swale.graph` header identifies the graph, and the
//! `cron.previous_fire_ms` header, the start of the schedule interval that
//! the firing ends, determines the partition. The [`TriggerWorker`] reads
//! both, and [`Scheduler::handle_trigger`] starts the graph run of the
//! graph's adopted definition for that partition. A start request starts the
//! run of every partition it lists ([`Scheduler::start_runs`]).
//!
//! The events worker submits the ready downstreams of a terminated task
//! instance, and the reconciler ([`Scheduler::reconcile`]) submits every
//! ready node of every active graph run. Each reads the node records of the
//! run once, applies [`crate::readiness`] and writes the final state when
//! the run is finished. The reconciler also cancels the active runs of a
//! cancelled graph run. A lost event delays a graph run by one reconciler
//! interval at most.
//!
//! A [`Request`] from another process ([`Scheduler::handle_request`]) starts
//! graph runs, reruns a node or cancels a graph run, and its outcome is
//! recorded at the request's key. The record of a rerun commits with the
//! submit of the task instance, so a request applied a second time after a
//! crash does not submit a second task instance.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use taquba::object_store::ObjectStore;
use taquba::{
    Clock, JobRecord, LeaseHandle, PermanentFailure, Queue, Worker, WorkerError, WorkerHandle,
};
use taquba_cron::PREVIOUS_FIRE_MS_HEADER;
use taquba_workflow::{RunId, RunOptions, RunSpec, RunState, RunnerHandle, WorkflowRuntime};
use tokio_util::sync::CancellationToken;

use crate::definition_store::{DefinitionError, DefinitionStore};
use crate::dispatch::Dispatch;
use crate::graph::{Graph, Node};
use crate::hook::{EVENTS_QUEUE, Event, RecordHook};
use crate::input::TaskInput;
use crate::operator::OperatorSet;
use crate::partition::Partition;
use crate::readiness::{NodeState, is_ready, node_states, settled_state};
use crate::records::JsonBytes;
use crate::records::{
    self, Entry, GraphRecord, GraphRunRecord, GraphRunState, NodeRecord, ReadError, RecordError,
    RecordStatus, RequestOutcome, RequestRecord,
};
use crate::request::{Request, RequestId};
use crate::store::store_path;
use crate::task::{self, HEADER_GRAPH, TaskIdentity};

/// The queue of the triggers.
pub const TRIGGERS_QUEUE: &str = "swale-triggers";

/// A failure of the scheduler.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The queue failed.
    #[error(transparent)]
    Queue(#[from] taquba::Error),
    /// The workflow runtime failed.
    #[error(transparent)]
    Workflow(#[from] taquba_workflow::Error),
    /// The object store failed.
    #[error(transparent)]
    ObjectStore(#[from] taquba::object_store::Error),
    /// A record is not valid JSON.
    #[error(transparent)]
    Record(#[from] RecordError),
    /// The definition store failed.
    #[error(transparent)]
    Definition(#[from] DefinitionError),
    /// The graph run records a definition the store does not have.
    #[error("definition `{0}` is not in the definition store")]
    UnknownDefinition(String),
    /// The graph does not have a graph record, so the process did not adopt
    /// a definition of the graph.
    #[error("graph `{0}` does not have an adopted definition")]
    UnknownGraph(String),
    /// The trigger does not list a partition, and it is not a cron firing
    /// with an earlier occurrence of the schedule.
    #[error("the trigger of graph `{0}` does not determine a partition")]
    NoPartition(String),
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

impl From<ReadError> for Error {
    fn from(e: ReadError) -> Self {
        match e {
            ReadError::Queue(e) => Error::Queue(e),
            ReadError::Record(e) => Error::Record(e),
        }
    }
}

impl Error {
    /// Whether a retry cannot change the outcome: a graph, a graph run or a
    /// node that does not exist, a trigger without a partition, a malformed
    /// record, or a permanent error of the queue or the runtime. A request
    /// with such an error is refused, and a worker with it dead-letters its
    /// job.
    pub fn is_permanent(&self) -> bool {
        match self {
            Error::Queue(e) => e.is_permanent(),
            Error::Workflow(e) => e.is_permanent(),
            Error::Record(_)
            | Error::UnknownGraph(_)
            | Error::UnknownGraphRun { .. }
            | Error::UnknownNode { .. }
            | Error::NoPartition(_) => true,
            Error::ObjectStore(_)
            | Error::Definition(_)
            | Error::UnknownDefinition(_)
            | Error::UnknownPool { .. } => false,
        }
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
    memo_retention: Duration,
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

    /// The time the memos and the run result record of a terminated task
    /// instance are kept, seven days by default. The scheduler does not read
    /// them, so the window serves diagnosis alone.
    pub fn memo_retention(mut self, retention: Duration) -> Self {
        self.memo_retention = retention;
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
                .memo_prefix(store_path(
                    &self.store_prefix,
                    &format!("swale-memo-{name}"),
                ))
                .max_concurrent_steps(concurrency)
                .poll_interval(self.poll_interval)
                .memo_retention(self.memo_retention)
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
            memo_retention: Duration::from_secs(7 * 86_400),
            store_prefix: String::new(),
            pools: Vec::new(),
        }
    }

    /// The runtime of the pool `name`.
    pub fn runtime(&self, name: &str) -> Option<&PoolRuntime> {
        self.runtimes.get(name)
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
    /// Events handled at a time, and triggers handled at a time.
    pub concurrency: usize,
    /// The poll interval of the events worker and of the triggers worker.
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
}

impl Scheduler {
    /// A scheduler over `queue`, running the graphs of `definitions` on
    /// `pools`, with [`EVENTS_QUEUE`] as its events queue and
    /// [`TRIGGERS_QUEUE`] as its triggers queue.
    pub fn new(queue: Arc<Queue>, definitions: Arc<DefinitionStore>, pools: Arc<Pools>) -> Self {
        let clock = queue.clock();
        Scheduler {
            queue,
            definitions,
            pools,
            clock,
        }
    }

    /// The definition store.
    pub fn definitions(&self) -> &Arc<DefinitionStore> {
        &self.definitions
    }

    /// The graph record of `graph`, or `None` when the process did not adopt
    /// a definition of the graph.
    pub async fn graph_record(&self, graph: &str) -> Result<Option<GraphRecord>, Error> {
        Ok(records::read(&*self.queue, &records::graph_key(graph)).await?)
    }

    /// Starts the graph run of the definition `hash` for `partition`: writes
    /// the graph run record and submits the root nodes. A second call for
    /// the same graph and partition does not change anything.
    pub async fn start_run(
        &self,
        hash: &str,
        partition: &Partition,
    ) -> Result<StartOutcome, Error> {
        let graph = self.graph(hash).await?;
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
                .submit_node(
                    node,
                    identity(&graph, hash, partition, node, 0),
                    &BTreeMap::new(),
                    |_| HashMap::new(),
                )
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
        let submitted = self
            .rerun_with(graph_name, partition, node, |_| HashMap::new())
            .await?;
        Ok(submitted.ok().map(|(run_id, _)| run_id))
    }

    /// [`Self::rerun`] with the KV writes of `kv_writes`, given the run id,
    /// committed with the submit. Returns the run id and whether the submit
    /// was new, or the reason the node was not rerun.
    async fn rerun_with(
        &self,
        graph_name: &str,
        partition: &Partition,
        node: &str,
        kv_writes: impl FnOnce(&RunId) -> HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<Result<(RunId, bool), NotRerun>, Error> {
        let key = records::graph_run_key(graph_name, partition);
        let Some((run, bytes)) = self.graph_run(&key).await? else {
            return Err(Error::UnknownGraphRun {
                graph: graph_name.to_string(),
                partition: partition.clone(),
            });
        };
        let graph = self.graph(&run.definition).await?;
        let node = graph.node(node).ok_or_else(|| Error::UnknownNode {
            graph: graph_name.to_string(),
            node: node.to_string(),
        })?;
        let records = self.node_records(&graph, partition).await?;
        let Some(record) = records.get(node.name()) else {
            return Ok(Err(NotRerun::NoRecord));
        };
        if record.status == RecordStatus::Succeeded {
            return Ok(Err(NotRerun::Succeeded));
        }
        if !is_ready(node, &records) {
            return Ok(Err(NotRerun::NotReady));
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
        let identity = identity(
            &graph,
            &record.definition,
            partition,
            node,
            record.rerun + 1,
        );
        let submitted = self
            .submit_node(node, identity, &upstream_records(node, &records), kv_writes)
            .await?;
        Ok(Ok(submitted))
    }

    /// Applies the request `id` and writes its record at the request's
    /// key. A request that refers to an absent graph, graph run or node, or
    /// that the state of the graph run does not admit, is refused in the
    /// record. A failure of the store is returned, and the record is not
    /// written.
    pub async fn handle_request(
        &self,
        id: &RequestId,
        request: &Request,
    ) -> Result<RequestRecord, Error> {
        let key = records::request_key(id);
        let handled_at_ms = self.clock.now_ms();
        let record = |outcome| RequestRecord {
            request: request.clone(),
            handled_at_ms,
            outcome,
        };
        let outcome = match request {
            Request::Start { graph, partitions } => {
                match self.start_runs(graph, partitions).await {
                    Ok(partitions) => RequestOutcome::Started { partitions },
                    Err(e) => RequestOutcome::Refused {
                        reason: refusal(e)?,
                    },
                }
            }
            Request::Rerun {
                graph,
                partition,
                node,
            } => {
                let rerun_record = |run_id: &RunId| {
                    record(RequestOutcome::Rerun {
                        run_id: run_id.to_string(),
                    })
                };
                let submitted = self
                    .rerun_with(graph, partition, node, |run_id| {
                        HashMap::from([(key.clone(), rerun_record(run_id).to_bytes())])
                    })
                    .await;
                match submitted {
                    Ok(Ok((run_id, true))) => return Ok(rerun_record(&run_id)),
                    Ok(Ok((run_id, false))) => RequestOutcome::Refused {
                        reason: format!("the rerun `{run_id}` is active"),
                    },
                    Ok(Err(reason)) => RequestOutcome::Refused {
                        reason: format!("node `{node}` {reason}"),
                    },
                    Err(e) => RequestOutcome::Refused {
                        reason: refusal(e)?,
                    },
                }
            }
            Request::Cancel { graph, partition } => {
                if self.cancel_run(graph, partition).await? {
                    RequestOutcome::Cancelled
                } else {
                    RequestOutcome::Refused {
                        reason: format!(
                            "graph `{graph}` does not have an active run for partition `{partition}`"
                        ),
                    }
                }
            }
        };
        let record = record(outcome);
        self.queue.kv_put(&key, &record.to_bytes()).await?;
        Ok(record)
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
        let graph = self.graph(&run.definition).await?;
        self.cancel_active_runs(&graph, partition).await?;
        Ok(true)
    }

    /// Handles a cron firing of `graph_name`: starts the graph run of the
    /// graph's adopted definition for the partition that contains
    /// `interval_start_ms`, the occurrence of the schedule before the firing
    /// time. Returns the partition, or `None` when its graph run existed.
    pub async fn handle_trigger(
        &self,
        graph_name: &str,
        interval_start_ms: Option<u64>,
    ) -> Result<Option<Partition>, Error> {
        let record = self.adopted(graph_name).await?;
        let graph = self.graph(&record.definition).await?;
        let partition = interval_start_ms
            .and_then(|ms| Partition::of_time(graph.partitioning(), ms))
            .ok_or_else(|| Error::NoPartition(graph_name.to_string()))?;
        let started = self
            .start_adopted(graph_name, &record, std::slice::from_ref(&partition))
            .await?;
        Ok(started.into_iter().next())
    }

    /// Starts the graph run of the adopted definition of `graph_name` for
    /// every partition of `partitions`. A partition with a graph run is
    /// unchanged. Returns the partitions whose graph run the call started.
    pub async fn start_runs(
        &self,
        graph_name: &str,
        partitions: &[Partition],
    ) -> Result<Vec<Partition>, Error> {
        let record = self.adopted(graph_name).await?;
        self.start_adopted(graph_name, &record, partitions).await
    }

    async fn adopted(&self, graph_name: &str) -> Result<GraphRecord, Error> {
        self.graph_record(graph_name)
            .await?
            .ok_or_else(|| Error::UnknownGraph(graph_name.to_string()))
    }

    async fn start_adopted(
        &self,
        graph_name: &str,
        record: &GraphRecord,
        partitions: &[Partition],
    ) -> Result<Vec<Partition>, Error> {
        let mut started = Vec::new();
        for partition in partitions {
            if self.start_run(&record.definition, partition).await?.started {
                tracing::info!(graph = %graph_name, %partition, "graph run started");
                started.push(partition.clone());
            }
        }
        Ok(started)
    }

    /// Handles the termination of one task instance: submits every ready
    /// downstream of its node, then settles the graph run.
    pub async fn handle_event(&self, event: &Event) -> Result<(), Error> {
        let key = records::graph_run_key(&event.graph, &event.partition);
        let Some((run, bytes)) = self.graph_run(&key).await? else {
            return Ok(());
        };
        if run.state != GraphRunState::Active {
            return Ok(());
        }
        let graph = self.graph(&run.definition).await?;
        let Some(node) = graph.node(&event.node) else {
            return Ok(());
        };
        let downstreams: Vec<&Node> = node
            .downstreams()
            .iter()
            .map(|name| {
                graph
                    .node(name)
                    .expect("a downstream name is a node of the graph")
            })
            .collect();
        self.advance(&graph, &event.partition, &run, &bytes, downstreams)
            .await?;
        Ok(())
    }

    /// One reconciler pass over every graph run record.
    pub async fn reconcile(&self) -> Result<ReconcileReport, Error> {
        let mut report = ReconcileReport::default();
        let runs: Vec<Entry<GraphRunRecord>> =
            records::scan(&*self.queue, records::GRAPH_RUNS_PREFIX.as_bytes()).await?;
        for Entry {
            key,
            bytes,
            record: run,
        } in runs
        {
            let Some((graph_name, partition)) = records::parse_graph_run_key(&key) else {
                continue;
            };
            let graph = match self.definitions.get(&run.definition).await {
                Ok(Some(graph)) => graph,
                Ok(None) => {
                    tracing::warn!(graph = %graph_name, %partition, definition = %run.definition, "graph run records an unknown definition");
                    continue;
                }
                Err(e) => {
                    tracing::warn!(graph = %graph_name, %partition, definition = %run.definition, error = %e, "the definition of a graph run does not load");
                    continue;
                }
            };
            match run.state {
                GraphRunState::Active => {
                    report.active_runs += 1;
                    let (submitted, settled) = self
                        .advance(&graph, &partition, &run, &bytes, graph.nodes())
                        .await?;
                    report.submitted += submitted;
                    if settled {
                        report.settled += 1;
                    }
                }
                GraphRunState::Cancelled => {
                    report.cancelled += self.cancel_active_runs(&graph, &partition).await?;
                }
                GraphRunState::Complete | GraphRunState::Failed => {}
            }
        }
        Ok(report)
    }

    /// Runs the events worker, the triggers worker and the reconciler until
    /// `shutdown` resolves.
    pub async fn run<F: Future<Output = ()>>(
        self: Arc<Self>,
        options: SchedulerOptions,
        shutdown: F,
    ) -> Result<(), Error> {
        let stop = CancellationToken::new();
        let worker = taquba::run_worker_concurrent(
            &self.queue,
            EVENTS_QUEUE,
            self.clone(),
            options.concurrency,
            options.poll_interval,
            stop.clone().cancelled_owned(),
        );
        let triggers = taquba::run_worker_concurrent(
            &self.queue,
            TRIGGERS_QUEUE,
            Arc::new(TriggerWorker::new(self.clone())),
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
        let mut all = std::pin::pin!(async {
            let (worker, triggers, ()) = tokio::join!(worker, triggers, reconciler);
            worker.and(triggers)
        });
        tokio::select! {
            result = &mut all => Ok(result?),
            () = shutdown => {
                stop.cancel();
                Ok(all.await?)
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

    async fn graph(&self, hash: &str) -> Result<Arc<Graph>, Error> {
        self.definitions
            .get(hash)
            .await?
            .ok_or_else(|| Error::UnknownDefinition(hash.to_string()))
    }

    async fn graph_run(&self, key: &[u8]) -> Result<Option<(GraphRunRecord, Vec<u8>)>, Error> {
        let Some(bytes) = self.queue.kv_get(key).await? else {
            return Ok(None);
        };
        let record = records::parse::<GraphRunRecord>(key, &bytes)?;
        Ok(Some((record, bytes.to_vec())))
    }

    async fn node_record(
        &self,
        graph: &Graph,
        partition: &Partition,
        node: &Node,
    ) -> Result<Option<NodeRecord>, Error> {
        let key = records::node_record_key(graph.name(), partition, node);
        Ok(records::read(&*self.queue, &key).await?)
    }

    /// The node records of the graph run of `graph` for `partition`, by
    /// node name.
    async fn node_records(
        &self,
        graph: &Graph,
        partition: &Partition,
    ) -> Result<BTreeMap<String, NodeRecord>, Error> {
        let mut records = BTreeMap::new();
        for node in graph.nodes() {
            if let Some(record) = self.node_record(graph, partition, node).await? {
                records.insert(node.name().to_string(), record);
            }
        }
        Ok(records)
    }

    /// Submits every ready node among `candidates` at rerun count 0, then
    /// settles the active graph run. Returns the count of new submits and
    /// whether the final state was written.
    async fn advance<'a>(
        &self,
        graph: &Graph,
        partition: &Partition,
        run: &GraphRunRecord,
        bytes: &[u8],
        candidates: impl IntoIterator<Item = &'a Node>,
    ) -> Result<(usize, bool), Error> {
        let records = self.node_records(graph, partition).await?;
        let states = node_states(graph, &records);
        let mut submitted = 0;
        for node in candidates {
            if states[node.name()] != NodeState::Ready {
                continue;
            }
            let (_, new) = self
                .submit_node(
                    node,
                    identity(graph, &run.definition, partition, node, 0),
                    &upstream_records(node, &records),
                    |_| HashMap::new(),
                )
                .await?;
            if new {
                submitted += 1;
            }
        }
        let settled = self
            .settle(graph, partition, run, bytes, &records, &states)
            .await?;
        Ok((submitted, settled))
    }

    /// Submits the task instance `identity` of `node`, with the KV writes of
    /// `kv_writes` committed with a new submit. Returns the run id and
    /// whether the submit was new.
    async fn submit_node(
        &self,
        node: &Node,
        identity: TaskIdentity,
        upstreams: &BTreeMap<String, NodeRecord>,
        kv_writes: impl FnOnce(&RunId) -> HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(RunId, bool), Error> {
        let runtime = self
            .pools
            .runtime(node.pool())
            .ok_or_else(|| Error::UnknownPool {
                node: node.name().to_string(),
                pool: node.pool().to_string(),
            })?;
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
                kv_writes: kv_writes(&run_id),
            })
            .await?;
        if outcome.newly_submitted {
            tracing::info!(run_id = %run_id, pool = node.pool(), "task instance submitted");
        }
        Ok((run_id, outcome.newly_submitted))
    }

    /// Writes the final state of the graph run when it is reached: the
    /// state of [`settled_state`], once no unrecorded task instance is
    /// active. `true` when the state was written.
    async fn settle(
        &self,
        graph: &Graph,
        partition: &Partition,
        run: &GraphRunRecord,
        bytes: &[u8],
        records: &BTreeMap<String, NodeRecord>,
        states: &BTreeMap<String, NodeState>,
    ) -> Result<bool, Error> {
        let Some(state) = settled_state(states) else {
            return Ok(false);
        };
        // A blocked node can have an active run at count 0 from the time it
        // was ready, and a failed or cancelled node can have an active rerun.
        for node in graph.nodes() {
            if let Some(run_id) = unrecorded_run_id(graph, partition, node, records)
                && self.run_is_active(node, &run_id).await?
            {
                return Ok(false);
            }
        }
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

    async fn run_is_active(&self, node: &Node, run_id: &RunId) -> Result<bool, Error> {
        let Some(runtime) = self.pools.runtime(node.pool()) else {
            return Ok(false);
        };
        Ok(matches!(
            runtime.status(run_id).await?,
            Some(status) if !matches!(status.state, RunState::Terminated(_))
        ))
    }

    /// Cancels the unrecorded task instance run of every node. Returns the
    /// count of runs cancelled.
    async fn cancel_active_runs(
        &self,
        graph: &Graph,
        partition: &Partition,
    ) -> Result<usize, Error> {
        let records = self.node_records(graph, partition).await?;
        let mut cancelled = 0;
        for node in graph.nodes() {
            let Some(run_id) = unrecorded_run_id(graph, partition, node, &records) else {
                continue;
            };
            let Some(runtime) = self.pools.runtime(node.pool()) else {
                continue;
            };
            if runtime.cancel(&run_id).await? {
                cancelled += 1;
            }
        }
        Ok(cancelled)
    }
}

/// The records of the upstreams of `node` among `records`, by node name.
fn upstream_records(
    node: &Node,
    records: &BTreeMap<String, NodeRecord>,
) -> BTreeMap<String, NodeRecord> {
    node.upstreams()
        .iter()
        .filter_map(|name| Some((name.clone(), records.get(name)?.clone())))
        .collect()
}

/// The run id of the task instance of `node` that does not have a record.
/// It is the run at count 0 of a node without a record, and the run at the
/// next count after a failed or cancelled record. `None` after a succeeded
/// record.
fn unrecorded_run_id(
    graph: &Graph,
    partition: &Partition,
    node: &Node,
    records: &BTreeMap<String, NodeRecord>,
) -> Option<RunId> {
    let rerun = match records.get(node.name()) {
        Some(record) if record.status == RecordStatus::Succeeded => return None,
        Some(record) => record.rerun + 1,
        None => 0,
    };
    Some(task::run_id(graph.name(), partition, node.name(), rerun))
}

/// The identity of the task instance of `node` at the rerun count `rerun`.
fn identity(
    graph: &Graph,
    hash: &str,
    partition: &Partition,
    node: &Node,
    rerun: u32,
) -> TaskIdentity {
    TaskIdentity {
        graph: graph.name().to_string(),
        partition: partition.clone(),
        node: node.name().to_string(),
        asset: node.asset().map(str::to_string),
        definition: hash.to_string(),
        rerun,
    }
}

/// Why a rerun did not submit a task instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NotRerun {
    /// The node does not have a record.
    NoRecord,
    /// The record of the node is succeeded.
    Succeeded,
    /// The records of the node's upstreams do not satisfy its trigger rule.
    NotReady,
}

impl std::fmt::Display for NotRerun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            NotRerun::NoRecord => "does not have a record",
            NotRerun::Succeeded => "succeeded",
            NotRerun::NotReady => "is not ready: its upstreams do not satisfy its trigger rule",
        })
    }
}

/// The reason a request is refused for `error`, or the error when it is a
/// failure of the store, which the caller retries.
fn refusal(error: Error) -> Result<String, Error> {
    if error.is_permanent() {
        Ok(error.to_string())
    } else {
        Err(error)
    }
}

/// The failure of a worker for `error`: permanent when the error is, and
/// retried otherwise.
fn worker_error(error: Error) -> WorkerError {
    if error.is_permanent() {
        PermanentFailure::new(error.to_string()).into()
    } else {
        Box::new(error)
    }
}

/// The headers of the cron schedule of `graph`, which every firing of the
/// schedule includes.
pub fn firing_headers(graph: &str) -> HashMap<String, String> {
    HashMap::from([(HEADER_GRAPH.to_string(), graph.to_string())])
}

/// The [`Worker`] of the triggers queue.
pub struct TriggerWorker {
    scheduler: Arc<Scheduler>,
}

impl TriggerWorker {
    /// A worker that starts graph runs on `scheduler`.
    pub fn new(scheduler: Arc<Scheduler>) -> Self {
        TriggerWorker { scheduler }
    }
}

impl Worker for TriggerWorker {
    async fn process(&self, job: &JobRecord, _lease: &LeaseHandle) -> Result<(), WorkerError> {
        let graph = job.headers.get(HEADER_GRAPH).ok_or_else(|| {
            PermanentFailure::new(format!("the job does not have the `{HEADER_GRAPH}` header"))
        })?;
        let interval_start_ms = job
            .headers
            .get(PREVIOUS_FIRE_MS_HEADER)
            .and_then(|value| value.parse().ok());
        self.scheduler
            .handle_trigger(graph, interval_start_ms)
            .await
            .map(|_| ())
            .map_err(worker_error)
    }
}

impl Worker for Scheduler {
    async fn process(&self, job: &JobRecord, _lease: &LeaseHandle) -> Result<(), WorkerError> {
        let event = Event::from_bytes(&job.payload)
            .map_err(|e| PermanentFailure::new(format!("the payload is not an event: {e}")))?;
        self.handle_event(&event).await.map_err(worker_error)
    }
}
