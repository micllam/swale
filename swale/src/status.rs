//! The view of a deployment from another process: the graphs, the graph
//! runs, the state of every node of a graph run and the queues.
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
use taquba::{JobRecord, QueueReader, QueueStats, ReaderMode, ReaderOptions};
use taquba_workflow::{MemoStore, RunState, WorkflowView};

use crate::definition_store::{DefinitionError, DefinitionStore};
use crate::partition::Partition;
use crate::pools::memo_prefix;
use crate::readiness::{NodeState, current_records, node_states};
use crate::records::{
    self, Entry, GRAPH_RUNS_PREFIX, GRAPHS_PREFIX, GraphRecord, GraphRunRecord, GraphRunState,
    NodeRecord, ReadError, RecordError, RequestRecord,
};
use crate::request::RequestId;
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
    /// The graph record, absent for a graph that the daemon did not adopt.
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
    /// The task instance waits in the queue of its pool, or for a retry.
    Pending,
    /// A worker of the pool runs the task instance.
    Running,
    /// The graph run is cancelled, and the task instance did not terminate.
    Cancelling,
}

impl InstanceState {
    /// The lowercase name, as in the JSON form.
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceState::Pending => "pending",
            InstanceState::Running => "running",
            InstanceState::Cancelling => "cancelling",
        }
    }
}

impl std::fmt::Display for InstanceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The task instance of a node that the runtime of its pool has.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskInstance {
    /// The run id.
    pub run_id: String,
    /// The state.
    pub state: InstanceState,
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
    /// the runtime of the pool has it as pending, running or cancelling.
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

    /// A view through `reader`, for a caller that sets the reader options.
    /// The pools of the store write their memos within `store_prefix`.
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

    /// The graph runs of `graph` in partition order, at or after the partition
    /// `from` when given. The read starts at the key of `from`.
    pub async fn runs(
        &self,
        graph: &str,
        from: Option<&Partition>,
    ) -> Result<Vec<RunSummary>, Error> {
        let prefix = format!("{GRAPH_RUNS_PREFIX}{graph}/");
        let start = from.map(|partition| records::graph_run_key(graph, partition));
        let range = (
            start.map_or(Bound::Unbounded, Bound::Included),
            Bound::Unbounded,
        );
        let mut runs = Vec::new();
        let entries: Vec<Entry<GraphRunRecord>> =
            records::scan(self.reader.view(), prefix.as_bytes(), range).await?;
        for Entry { key, record, .. } in entries {
            let Some((_, partition)) = records::parse_graph_run_key(&key) else {
                continue;
            };
            runs.push(RunSummary { partition, record });
        }
        Ok(runs)
    }

    /// The graph run of `graph` for `partition` with the state of every node
    /// of the definition it records and the task instance of every node that
    /// the runtime of its pool has, or `None` without a graph run record.
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
                    Some(run_id) => views
                        .entry(node.pool())
                        .or_insert_with(|| self.pool_view(node.pool()))
                        .status(&run_id)
                        .await?
                        .and_then(|status| instance_state(&status.state))
                        .map(|state| TaskInstance {
                            run_id: run_id.to_string(),
                            state,
                        }),
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

    /// The record of the request `id`, or `None` while the daemon did not
    /// apply the request.
    pub async fn request(&self, id: &RequestId) -> Result<Option<RequestRecord>, Error> {
        Ok(records::read(self.reader.view(), &records::request_key(id)).await?)
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

    /// The first `limit` dead jobs of `queue`, in enqueue order, or `None`
    /// when the store does not have the queue.
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

/// The state of a task instance from the state of its run, or `None` for a
/// terminated run.
fn instance_state(state: &RunState) -> Option<InstanceState> {
    match state {
        RunState::Pending => Some(InstanceState::Pending),
        RunState::Running => Some(InstanceState::Running),
        RunState::Cancelling => Some(InstanceState::Cancelling),
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
    use taquba_workflow::{RunTermination, TerminalStatus};

    use super::*;

    #[test]
    fn a_terminated_run_is_not_an_instance() {
        assert_eq!(
            instance_state(&RunState::Pending),
            Some(InstanceState::Pending)
        );
        assert_eq!(
            instance_state(&RunState::Running),
            Some(InstanceState::Running)
        );
        assert_eq!(
            instance_state(&RunState::Cancelling),
            Some(InstanceState::Cancelling)
        );
        let terminated = RunTermination {
            status: TerminalStatus::Succeeded,
            error: None,
            error_kind: None,
            final_step: 0,
            terminated_at_ms: 0,
        };
        assert_eq!(instance_state(&RunState::Terminated(terminated)), None);
        assert_eq!(InstanceState::Cancelling.to_string(), "cancelling");
    }
}
