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
//! superseded has the state of a node without a record and keeps the record
//! in its status.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;
use taquba::object_store::ObjectStore;
use taquba::{JobRecord, QueueReader, QueueStats, ReaderMode, ReaderOptions};

use crate::definition_store::{DefinitionError, DefinitionStore};
use crate::partition::Partition;
use crate::readiness::{NodeState, current_records, node_states};
use crate::records::{
    self, Entry, GRAPH_RUNS_PREFIX, GRAPHS_PREFIX, GraphRecord, GraphRunRecord, GraphRunState,
    NodeRecord, ReadError, RecordError, RequestRecord,
};
use crate::request::RequestId;

/// A failure of a status read.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The reader failed.
    #[error(transparent)]
    Queue(#[from] taquba::Error),
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
    definitions: Arc<DefinitionStore>,
}

impl StatusReader {
    /// Opens a reader of the queue at `queue_path` of `store`. The reader
    /// follows the latest state without a checkpoint, so it does not write
    /// to the store.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        queue_path: &str,
        definitions: Arc<DefinitionStore>,
    ) -> Result<Self, Error> {
        let options = ReaderOptions::default().mode(ReaderMode::FollowLatest);
        let reader = QueueReader::open_with_options(store, queue_path, options).await?;
        Ok(StatusReader::new(reader, definitions))
    }

    /// A view through `reader`, for a caller that sets the reader options.
    pub fn new(reader: QueueReader, definitions: Arc<DefinitionStore>) -> Self {
        StatusReader {
            reader,
            definitions,
        }
    }

    /// Every graph with a graph record or a graph run, by name.
    pub async fn graphs(&self) -> Result<Vec<GraphStatus>, Error> {
        let mut graphs: BTreeMap<String, GraphStatus> = BTreeMap::new();
        let adopted: Vec<Entry<GraphRecord>> =
            records::scan(&self.reader, GRAPHS_PREFIX.as_bytes()).await?;
        for Entry { key, record, .. } in adopted {
            let Some(name) = records::parse_graph_key(&key) else {
                continue;
            };
            graph_entry(&mut graphs, &name).adopted = Some(record);
        }
        let runs: Vec<Entry<GraphRunRecord>> =
            records::scan(&self.reader, GRAPH_RUNS_PREFIX.as_bytes()).await?;
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

    /// The graph runs of `graph`, in partition order.
    pub async fn runs(&self, graph: &str) -> Result<Vec<RunSummary>, Error> {
        let prefix = format!("{GRAPH_RUNS_PREFIX}{graph}/");
        let mut runs = Vec::new();
        let entries: Vec<Entry<GraphRunRecord>> =
            records::scan(&self.reader, prefix.as_bytes()).await?;
        for Entry { key, record, .. } in entries {
            let Some((_, partition)) = records::parse_graph_run_key(&key) else {
                continue;
            };
            runs.push(RunSummary { partition, record });
        }
        Ok(runs)
    }

    /// The graph run of `graph` for `partition` with the state of every node
    /// of the definition it records, or `None` without a graph run record.
    pub async fn run(
        &self,
        graph: &str,
        partition: &Partition,
    ) -> Result<Option<GraphRunStatus>, Error> {
        let key = records::graph_run_key(graph, partition);
        let Some(record) = records::read::<GraphRunRecord>(&self.reader, &key).await? else {
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
            if let Some(node_record) = records::read::<NodeRecord>(&self.reader, &key).await? {
                node_records.insert(node.name().to_string(), node_record);
            }
        }
        let states = node_states(&definition, &current_records(&record, &node_records));
        let nodes = definition
            .nodes()
            .iter()
            .map(|node| NodeStatus {
                name: node.name().to_string(),
                pool: node.pool().to_string(),
                state: states[node.name()],
                record: node_records.remove(node.name()),
            })
            .collect();
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
        Ok(records::read(&self.reader, &records::request_key(id)).await?)
    }

    /// The job counts of every queue of the store, by queue name.
    pub async fn queues(&self) -> Result<Vec<QueueStats>, Error> {
        let mut names = self.reader.list_queues().await?;
        names.sort();
        let mut stats = Vec::with_capacity(names.len());
        for name in names {
            stats.push(self.reader.stats(&name).await?);
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
        if !self.reader.list_queues().await?.iter().any(|q| q == queue) {
            return Ok(None);
        }
        Ok(Some(self.reader.dead_jobs(queue, None, limit).await?))
    }

    /// Closes the reader.
    pub async fn close(self) -> Result<(), Error> {
        Ok(self.reader.close().await?)
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
