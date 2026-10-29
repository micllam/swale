//! The view of a deployment from another process: the graphs, the graph runs,
//! the state of every node of a graph run and the queues.
//!
//! A [`StatusReader`] reads the records of [`crate::records`] through a
//! [`QueueReader`] and the definitions through a [`DefinitionStore`]. It only
//! reads from the store, so it can run alongside the process that opened the
//! store. Its view lags that process by the flush interval of the writer.
//!
//! The state of a node ([`NodeState`]) is derived from the records alone, by
//! the readiness rule of [`crate::readiness`]. A node whose record a rerun
//! superseded has the state of a node without a record and keeps the record in
//! its status. The task instance of a node that is not recorded as succeeded
//! ([`TaskInstance`]) is read from the runtime state of the node's pool through
//! a [`WorkflowView`], at the run id of [`crate::task::unrecorded_run_id`].

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use serde::Serialize;
use taquba::object_store::ObjectStore;
use taquba::{
    InboxOutcome, JobRecord, JobStatus, QueueReader, QueueStats, ReaderMode, ReaderOptions,
    Rejection,
};
use taquba_workflow::{GroupStatus, MemoStore, RunId, RunState, WorkflowView};

use crate::definition_store::{DefinitionError, DefinitionStore};
use crate::partition::Partition;
use crate::pools::memo_prefix;
use crate::readiness::{NodeState, current_records, node_states};
use crate::records::{
    self, Entry, GRAPH_RUNS_PREFIX, GRAPHS_PREFIX, GraphRecord, GraphRunRecord, GraphRunState,
    NodeRecord, ReadError, RecordError, RequestRecord,
};
use crate::request::{REQUESTS_QUEUE, RequestId};
use crate::task;

/// A failure of a status read.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The reader failed.
    #[error(transparent)]
    Queue(#[from] taquba::Error),
    /// The workflow view failed.
    #[error(transparent)]
    Workflow(#[from] taquba_workflow::Error),
    /// The definition store failed.
    #[error(transparent)]
    Definition(#[from] DefinitionError),
    /// A record is not valid JSON.
    #[error(transparent)]
    Record(#[from] RecordError),
    /// The graph run records a definition the store does not have.
    #[error("definition `{0}` is not in the definition store")]
    UnknownDefinition(String),
}

impl From<ReadError> for Error {
    fn from(e: ReadError) -> Self {
        match e {
            ReadError::Queue(e) => Error::Queue(e),
            ReadError::Record(e) => Error::Record(e),
        }
    }
}

/// The count of the graph runs of one graph in each state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RunCounts {
    /// The active graph runs.
    pub active: usize,
    /// The cancelled graph runs.
    pub cancelled: usize,
    /// The complete graph runs.
    pub complete: usize,
    /// The failed graph runs.
    pub failed: usize,
}

/// A graph run in a listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunSummary {
    /// The partition.
    pub partition: Partition,
    /// The graph run record.
    pub record: GraphRunRecord,
}

/// A graph with a graph record or a graph run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GraphStatus {
    /// The graph name.
    pub name: String,
    /// The graph record, or `None` for a graph that the daemon did not adopt.
    pub adopted: Option<GraphRecord>,
    /// The count of the graph runs in each state.
    pub runs: RunCounts,
    /// The graph run of the partition that sorts last.
    pub latest: Option<RunSummary>,
}

/// The state of a task instance that the runtime of its pool has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceState {
    /// The task instance waits in the queue of its pool for a worker.
    Pending,
    /// The task instance waits for a delay: a retry, or the next read of a
    /// sensor or of the members of a mapped node.
    Scheduled,
    /// The task instance waits for a signal.
    Waiting,
    /// A worker of the pool runs the task instance.
    Running,
    /// The graph run is cancelled, and the task instance did not terminate.
    Cancelling,
    /// The queue dead-lettered the task instance outside a worker, and a worker
    /// of the pool terminates it as failed.
    DeadLettered,
}

impl InstanceState {
    /// The name of the JSON form.
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceState::Pending => "pending",
            InstanceState::Scheduled => "scheduled",
            InstanceState::Waiting => "waiting",
            InstanceState::Running => "running",
            InstanceState::Cancelling => "cancelling",
            InstanceState::DeadLettered => "dead_lettered",
        }
    }
}

impl std::fmt::Display for InstanceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The state of a request, as [`StatusReader::request`] reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestState {
    /// The request waits for a pass of the inbox or for the request worker.
    Waiting {
        /// The error of the last attempt of the request job, or `None` before a
        /// failed attempt.
        last_error: Option<String>,
    },
    /// The scheduler applied the request, with the record of its outcome.
    Applied(RequestRecord),
    /// The inbox rejected the entry of the request for the reason.
    Rejected(Rejection),
    /// The request job was dead-lettered with the error of its last attempt.
    Failed(String),
}

/// The task instance of a node that the runtime of its pool has.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskInstance {
    /// The run id.
    pub run_id: String,
    /// The state.
    pub state: InstanceState,
    /// The member counts of a mapped node, or `None` for a node without `each`
    /// and at the first step of the task instance, which submits the members.
    pub members: Option<MemberCounts>,
}

/// The members of a mapped node, counted by state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MemberCounts {
    /// The members of the node.
    pub total: usize,
    /// The members submitted and not terminated.
    pub pending: usize,
    /// The succeeded members.
    pub succeeded: usize,
    /// The failed members.
    pub failed: usize,
    /// The cancelled members.
    pub cancelled: usize,
}

impl MemberCounts {
    /// The terminated members.
    pub fn terminated(&self) -> usize {
        self.succeeded + self.failed + self.cancelled
    }
}

impl From<GroupStatus> for MemberCounts {
    fn from(status: GroupStatus) -> Self {
        MemberCounts {
            total: status.total,
            pending: status.pending,
            succeeded: status.succeeded,
            failed: status.failed,
            cancelled: status.cancelled,
        }
    }
}

/// A node of a graph run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeStatus {
    /// The node name.
    pub name: String,
    /// The pool of the node.
    pub pool: String,
    /// The state.
    pub state: NodeState,
    /// The node record, current or superseded by a rerun.
    pub record: Option<NodeRecord>,
    /// The task instance at the run id of [`task::unrecorded_run_id`], while
    /// the runtime of the pool has not terminated it.
    pub instance: Option<TaskInstance>,
}

/// A graph run with its nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GraphRunStatus {
    /// The graph name.
    pub graph: String,
    /// The partition.
    pub partition: Partition,
    /// The graph run record.
    pub record: GraphRunRecord,
    /// The nodes, in the order of the definition.
    pub nodes: Vec<NodeStatus>,
}

/// A read-only view of a deployment.
pub struct StatusReader {
    reader: QueueReader,
    store: Arc<dyn ObjectStore>,
    store_prefix: String,
    definitions: Arc<DefinitionStore>,
}

impl StatusReader {
    /// Opens a reader of the queue at `queue_path` of `store`, whose pools
    /// write their memos within `store_prefix`. The reader follows the latest
    /// state without a checkpoint, so it does not write to the store.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        store_prefix: &str,
        queue_path: &str,
        definitions: Arc<DefinitionStore>,
    ) -> Result<Self, Error> {
        let options = ReaderOptions::default().mode(ReaderMode::FollowLatest);
        let reader = QueueReader::open_with_options(store.clone(), queue_path, options).await?;
        Ok(StatusReader::new(reader, store, store_prefix, definitions))
    }

    /// A view through `reader`, for a caller that sets the reader options. The
    /// pools of the store write their memos within `store_prefix`.
    pub fn new(
        reader: QueueReader,
        store: Arc<dyn ObjectStore>,
        store_prefix: &str,
        definitions: Arc<DefinitionStore>,
    ) -> Self {
        StatusReader {
            reader,
            store,
            store_prefix: store_prefix.to_string(),
            definitions,
        }
    }

    /// The workflow view of the pool `pool`, over the reader's view and the
    /// memos of the pool.
    fn pool_view(&self, pool: &str) -> WorkflowView {
        WorkflowView::new(
            self.reader.view().clone(),
            MemoStore::new(self.store.clone(), memo_prefix(&self.store_prefix, pool)),
        )
    }

    /// Every graph with a graph record or a graph run, by name.
    pub async fn graphs(&self) -> Result<Vec<GraphStatus>, Error> {
        let mut graphs: BTreeMap<String, GraphStatus> = BTreeMap::new();
        let adopted: Vec<Entry<GraphRecord>> =
            records::scan(self.reader.view(), GRAPHS_PREFIX.as_bytes(), ..).await?;
        for Entry { key, record, .. } in adopted {
            let Some(name) = records::parse_graph_key(&key) else {
                continue;
            };
            graph_entry(&mut graphs, &name).adopted = Some(record);
        }
        let runs: Vec<Entry<GraphRunRecord>> =
            records::scan(self.reader.view(), GRAPH_RUNS_PREFIX.as_bytes(), ..).await?;
        for Entry { key, record, .. } in runs {
            let Some((name, partition)) = records::parse_graph_run_key(&key) else {
                continue;
            };
            let graph = graph_entry(&mut graphs, &name);
            match record.state {
                GraphRunState::Active => graph.runs.active += 1,
                GraphRunState::Cancelled => graph.runs.cancelled += 1,
                GraphRunState::Complete => graph.runs.complete += 1,
                GraphRunState::Failed => graph.runs.failed += 1,
            }
            // The scan is in key order, so the last run of a graph has the
            // partition that sorts last.
            graph.latest = Some(RunSummary { partition, record });
        }
        Ok(graphs.into_values().collect())
    }

    /// The latest `limit` graph runs of `graph` in partition order, at or after
    /// the partition `from` when given. The read starts at the last key.
    pub async fn runs(
        &self,
        graph: &str,
        from: Option<&Partition>,
        limit: usize,
    ) -> Result<Vec<RunSummary>, Error> {
        let prefix = format!("{GRAPH_RUNS_PREFIX}{graph}/");
        let start = from.map(|partition| records::graph_run_key(graph, partition));
        let range = (
            start.map_or(Bound::Unbounded, Bound::Included),
            Bound::Unbounded,
        );
        let mut runs = Vec::new();
        let entries: Vec<Entry<GraphRunRecord>> =
            records::scan_last(self.reader.view(), prefix.as_bytes(), range, limit).await?;
        for Entry { key, record, .. } in entries {
            let Some((_, partition)) = records::parse_graph_run_key(&key) else {
                continue;
            };
            runs.push(RunSummary { partition, record });
        }
        Ok(runs)
    }

    /// The graph run of `graph` for `partition` with the state of every node of
    /// the definition it records and the task instance of every node that the
    /// runtime of its pool has, or `None` without a graph run record.
    pub async fn run(
        &self,
        graph: &str,
        partition: &Partition,
    ) -> Result<Option<GraphRunStatus>, Error> {
        let key = records::graph_run_key(graph, partition);
        let Some(record) = records::read::<GraphRunRecord>(self.reader.view(), &key).await? else {
            return Ok(None);
        };
        let definition = self
            .definitions
            .get(&record.definition)
            .await?
            .ok_or_else(|| Error::UnknownDefinition(record.definition.clone()))?;
        let mut node_records = BTreeMap::new();
        for node in definition.nodes() {
            let key = records::node_record_key(graph, partition, node);
            if let Some(node_record) = records::read::<NodeRecord>(self.reader.view(), &key).await?
            {
                node_records.insert(node.name().to_string(), node_record);
            }
        }
        let states = node_states(&definition, &current_records(&record, &node_records));
        let mut views: BTreeMap<&str, WorkflowView> = BTreeMap::new();
        let mut nodes = Vec::with_capacity(definition.nodes().len());
        for node in definition.nodes() {
            let instance =
                match task::unrecorded_run_id(&definition, partition, &record, node, &node_records)
                {
                    Some(run_id) => {
                        let view = views
                            .entry(node.pool())
                            .or_insert_with(|| self.pool_view(node.pool()));
                        let group = node
                            .each()
                            .map(|_| task::group_id(graph, partition, node.name()));
                        instance(view, &run_id, group.as_ref()).await?
                    }
                    None => None,
                };
            nodes.push(NodeStatus {
                name: node.name().to_string(),
                pool: node.pool().to_string(),
                state: states[node.name()],
                record: node_records.remove(node.name()),
                instance,
            });
        }
        Ok(Some(GraphRunStatus {
            graph: graph.to_string(),
            partition: partition.clone(),
            record,
            nodes,
        }))
    }

    /// The state of the request `id`, read from its record, from the record of
    /// its inbox entry and from its job.
    pub async fn request(&self, id: &RequestId) -> Result<RequestState, Error> {
        let view = self.reader.view();
        let key = records::request_key(id);
        if let Some(record) = records::read(view, &key).await? {
            return Ok(RequestState::Applied(record));
        }
        let inbox = view.inbox_record(REQUESTS_QUEUE, id.as_str()).await?;
        if let Some(InboxOutcome::Rejected(reason)) = inbox.map(|record| record.outcome) {
            return Ok(RequestState::Rejected(reason));
        }
        let job = view.job_record(id.as_str()).await?;
        Ok(match job {
            Some(job) if job.status == JobStatus::Dead => {
                RequestState::Failed(job.last_error.unwrap_or_default())
            }
            Some(job) if job.status != JobStatus::Done => RequestState::Waiting {
                last_error: job.last_error,
            },
            // The record commits before the job settles.
            _ => match records::read(view, &key).await? {
                Some(record) => RequestState::Applied(record),
                None => RequestState::Waiting { last_error: None },
            },
        })
    }

    /// The job counts of every queue of the store, by queue name.
    pub async fn queues(&self) -> Result<Vec<QueueStats>, Error> {
        let mut names = self.reader.view().list_queues().await?;
        names.sort();
        let mut stats = Vec::with_capacity(names.len());
        for name in names {
            stats.push(self.reader.view().stats(&name).await?);
        }
        Ok(stats)
    }

    /// The first `limit` dead jobs of `queue` in enqueue order, or `None` when
    /// the store does not have the queue.
    pub async fn dead_jobs(
        &self,
        queue: &str,
        limit: usize,
    ) -> Result<Option<Vec<JobRecord>>, Error> {
        if !self
            .reader
            .view()
            .list_queues()
            .await?
            .iter()
            .any(|q| q == queue)
        {
            return Ok(None);
        }
        Ok(Some(
            self.reader.view().dead_jobs(queue, None, limit).await?,
        ))
    }

    /// Closes the reader.
    pub async fn close(self) -> Result<(), Error> {
        Ok(self.reader.close().await?)
    }
}

/// The task instance at `run_id` with the member counts of the run group
/// `group` after its first step, or `None` for a run that `view` does not have
/// or that has terminated.
async fn instance(
    view: &WorkflowView,
    run_id: &RunId,
    group: Option<&RunId>,
) -> Result<Option<TaskInstance>, Error> {
    let Some(status) = view.status(run_id).await? else {
        return Ok(None);
    };
    let Some(state) = instance_state(&status.state) else {
        return Ok(None);
    };
    let members = match group {
        Some(group) if status.step.number > 0 => {
            view.group_status(group).await?.map(MemberCounts::from)
        }
        _ => None,
    };
    Ok(Some(TaskInstance {
        run_id: run_id.to_string(),
        state,
        members,
    }))
}

/// The state of a task instance from the state of its run, or `None` for a
/// terminated run.
fn instance_state(state: &RunState) -> Option<InstanceState> {
    match state {
        RunState::Pending => Some(InstanceState::Pending),
        RunState::Scheduled { .. } => Some(InstanceState::Scheduled),
        RunState::Waiting { .. } => Some(InstanceState::Waiting),
        RunState::Running => Some(InstanceState::Running),
        RunState::Cancelling { .. } => Some(InstanceState::Cancelling),
        RunState::DeadLettered => Some(InstanceState::DeadLettered),
        RunState::Terminated(_) => None,
    }
}

fn graph_entry<'a>(
    graphs: &'a mut BTreeMap<String, GraphStatus>,
    name: &str,
) -> &'a mut GraphStatus {
    graphs
        .entry(name.to_string())
        .or_insert_with(|| GraphStatus {
            name: name.to_string(),
            adopted: None,
            runs: RunCounts::default(),
            latest: None,
        })
}

#[cfg(test)]
mod tests {
    use taquba_workflow::{RunTermination, StepRef, TerminalStatus};

    use super::*;

    #[test]
    fn every_run_state_but_a_termination_is_an_instance_state() {
        let waiting = RunState::Waiting { timeout_at_ms: 0 };
        let cancelling = RunState::Cancelling {
            reason: None,
            requested_at_ms: 0,
        };
        for (state, instance) in [
            (RunState::Pending, InstanceState::Pending),
            (
                RunState::Scheduled { run_at_ms: 0 },
                InstanceState::Scheduled,
            ),
            (waiting, InstanceState::Waiting),
            (RunState::Running, InstanceState::Running),
            (cancelling, InstanceState::Cancelling),
            (RunState::DeadLettered, InstanceState::DeadLettered),
        ] {
            assert_eq!(instance_state(&state), Some(instance));
        }
        let terminated = RunTermination {
            status: TerminalStatus::Succeeded,
            error: None,
            error_kind: None,
            cancelled_by: None,
            final_step: StepRef {
                number: 0,
                job_id: "j".into(),
            },
            terminated_at_ms: 0,
        };
        assert_eq!(instance_state(&RunState::Terminated(terminated)), None);
        assert_eq!(InstanceState::DeadLettered.to_string(), "dead_lettered");
    }
}
