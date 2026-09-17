//! The view of a deployment from another process: the graphs, the graph
//! runs, the state of every node of a graph run and the queues.
//!
//! A [`StatusReader`] reads the records of [`crate::records`] through a
//! [`QueueReader`] and the definitions through a [`DefinitionStore`]. It only
//! reads from the store, so it can run alongside the process that opened the
//! store. Its view lags that process by the flush interval of the writer.
//!
//! The state of a node ([`NodeState`]) is derived from the records alone
//! ([`node_states`]). A node without a record is ready, waiting or blocked,
//! by the readiness rule of the scheduler.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;
use taquba::object_store::ObjectStore;
use taquba::{JobRecord, QueueReader, QueueStats, ReaderMode, ReaderOptions};

use crate::definition_store::{DefinitionError, DefinitionStore};
use crate::graph::{Graph, Node, TriggerRule};
use crate::partition::Partition;
use crate::records::{
    self, GRAPH_RUNS_PREFIX, GRAPHS_PREFIX, GraphRecord, GraphRunRecord, GraphRunState, NodeRecord,
    RecordStatus,
};
use crate::scheduler::is_ready;

/// The entries of one scan page.
const PAGE: usize = 256;

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
}

/// The state of a node in a graph run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    /// The record is succeeded.
    Succeeded,
    /// The record is failed.
    Failed,
    /// The record is cancelled.
    Cancelled,
    /// The node does not have a record, and the records of its upstreams
    /// satisfy its trigger rule. The scheduler submits the node, and its
    /// task instance runs or waits in the queue of its pool.
    Ready,
    /// The node does not have a record, and an upstream without a record can
    /// still satisfy its trigger rule.
    Waiting,
    /// The node does not have a record, and the records of its upstreams
    /// cannot satisfy its trigger rule until a rerun changes a record.
    Blocked,
}

impl NodeState {
    /// The lowercase name, as in the JSON form.
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeState::Succeeded => "succeeded",
            NodeState::Failed => "failed",
            NodeState::Cancelled => "cancelled",
            NodeState::Ready => "ready",
            NodeState::Waiting => "waiting",
            NodeState::Blocked => "blocked",
        }
    }

    /// Whether a record can still follow: the node is ready or waiting.
    fn is_open(&self) -> bool {
        matches!(self, NodeState::Ready | NodeState::Waiting)
    }
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<RecordStatus> for NodeState {
    fn from(status: RecordStatus) -> Self {
        match status {
            RecordStatus::Succeeded => NodeState::Succeeded,
            RecordStatus::Failed => NodeState::Failed,
            RecordStatus::Cancelled => NodeState::Cancelled,
        }
    }
}

/// The state of every node of `graph`, by node name, from the node records
/// of one partition, by node name.
pub fn node_states(
    graph: &Graph,
    records: &BTreeMap<String, NodeRecord>,
) -> BTreeMap<String, NodeState> {
    let mut states: BTreeMap<String, NodeState> = BTreeMap::new();
    // The graph is acyclic, so every pass resolves at least one node.
    while states.len() < graph.nodes().len() {
        for node in graph.nodes() {
            if states.contains_key(node.name()) {
                continue;
            }
            if let Some(record) = records.get(node.name()) {
                states.insert(node.name().to_string(), record.status.into());
                continue;
            }
            let upstreams: Option<Vec<NodeState>> = node
                .upstreams()
                .iter()
                .map(|name| states.get(name).copied())
                .collect();
            let Some(upstreams) = upstreams else {
                continue;
            };
            let upstream_records = node
                .upstreams()
                .iter()
                .filter_map(|name| Some((name.clone(), records.get(name)?.clone())))
                .collect();
            let state = if is_ready(node, &upstream_records) {
                NodeState::Ready
            } else if can_become_ready(node, &upstreams) {
                NodeState::Waiting
            } else {
                NodeState::Blocked
            };
            states.insert(node.name().to_string(), state);
        }
    }
    states
}

/// Whether a record that follows for an open upstream can satisfy the
/// trigger rule of `node`, which its present records do not satisfy.
fn can_become_ready(node: &Node, upstreams: &[NodeState]) -> bool {
    match node.trigger_rule() {
        TriggerRule::AllSucceeded => upstreams
            .iter()
            .all(|state| *state == NodeState::Succeeded || state.is_open()),
        TriggerRule::AllDone => upstreams.iter().all(|state| *state != NodeState::Blocked),
        TriggerRule::OneFailed => upstreams.iter().any(NodeState::is_open),
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
    /// The node record.
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
        for (key, bytes) in self.scan(GRAPHS_PREFIX.as_bytes()).await? {
            let Some(name) = records::parse_graph_key(&key) else {
                continue;
            };
            let record = parse(&key, &bytes, GraphRecord::from_bytes)?;
            graph_entry(&mut graphs, &name).adopted = Some(record);
        }
        for (key, bytes) in self.scan(GRAPH_RUNS_PREFIX.as_bytes()).await? {
            let Some((name, partition)) = records::parse_graph_run_key(&key) else {
                continue;
            };
            let record = parse(&key, &bytes, GraphRunRecord::from_bytes)?;
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
        for (key, bytes) in self.scan(prefix.as_bytes()).await? {
            let Some((_, partition)) = records::parse_graph_run_key(&key) else {
                continue;
            };
            let record = parse(&key, &bytes, GraphRunRecord::from_bytes)?;
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
        let Some(bytes) = self.reader.kv_get(&key).await? else {
            return Ok(None);
        };
        let record = parse(&key, &bytes, GraphRunRecord::from_bytes)?;
        let definition = self
            .definitions
            .get(&record.definition)
            .await?
            .ok_or_else(|| Error::UnknownDefinition(record.definition.clone()))?;
        let mut node_records = BTreeMap::new();
        for node in definition.nodes() {
            let key = records::node_record_key(graph, partition, node);
            if let Some(bytes) = self.reader.kv_get(&key).await? {
                let node_record = parse(&key, &bytes, NodeRecord::from_bytes)?;
                node_records.insert(node.name().to_string(), node_record);
            }
        }
        let states = node_states(&definition, &node_records);
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

    /// The first `limit` dead jobs of `queue`, in enqueue order.
    pub async fn dead_jobs(&self, queue: &str, limit: usize) -> Result<Vec<JobRecord>, Error> {
        Ok(self.reader.dead_jobs(queue, None, limit).await?)
    }

    /// Closes the reader.
    pub async fn close(self) -> Result<(), Error> {
        Ok(self.reader.close().await?)
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let mut entries = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let page = self.reader.kv_scan(prefix, cursor.as_deref(), PAGE).await?;
            entries.extend(
                page.entries
                    .into_iter()
                    .map(|(key, value)| (key, value.to_vec())),
            );
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => return Ok(entries),
            }
        }
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

fn parse<T>(
    key: &[u8],
    bytes: &[u8],
    from_bytes: fn(&[u8]) -> Result<T, serde_json::Error>,
) -> Result<T, Error> {
    from_bytes(bytes).map_err(|source| Error::Record {
        key: String::from_utf8_lossy(key).into_owned(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::OperatorSet;

    /// `on_failure` runs when `a` fails, `join` needs `b` and `on_failure`,
    /// and `report` runs when `join` is done.
    const DEFINITION: &str = r#"
[graph]
name = "g"

[[node]]
name = "a"
produces = "a_out"
operator = "subprocess"
[node.params]
argv = ["true"]

[[node]]
name = "b"
produces = "b_out"
consumes = ["a_out"]
operator = "subprocess"
[node.params]
argv = ["true"]

[[node]]
name = "on_failure"
after = ["a"]
trigger_rule = "one_failed"
operator = "subprocess"
[node.params]
argv = ["true"]

[[node]]
name = "join"
after = ["b", "on_failure"]
operator = "subprocess"
[node.params]
argv = ["true"]

[[node]]
name = "report"
after = ["join"]
trigger_rule = "all_done"
operator = "subprocess"
[node.params]
argv = ["true"]
"#;

    fn record(status: RecordStatus) -> NodeRecord {
        NodeRecord {
            status,
            run_id: "run".into(),
            definition: "abc".into(),
            rerun: 0,
            terminated_at_ms: 0,
            output: None,
            output_omitted: false,
            error: None,
        }
    }

    fn states(records: &[(&str, RecordStatus)]) -> Vec<NodeState> {
        let graph = crate::load_str(DEFINITION, &OperatorSet::builtin()).unwrap();
        let records = records
            .iter()
            .map(|(name, status)| (name.to_string(), record(*status)))
            .collect();
        let states = node_states(&graph, &records);
        ["a", "b", "on_failure", "join", "report"]
            .map(|name| states[name])
            .to_vec()
    }

    #[test]
    fn a_node_without_a_record_is_ready_waiting_or_blocked_by_its_trigger_rule() {
        use NodeState::*;
        assert_eq!(states(&[]), [Ready, Waiting, Waiting, Waiting, Waiting]);
        // A succeeded `a` blocks `on_failure`, which blocks `join` and
        // `report`.
        assert_eq!(
            states(&[("a", RecordStatus::Succeeded)]),
            [Succeeded, Ready, Blocked, Blocked, Blocked]
        );
        assert_eq!(
            states(&[("a", RecordStatus::Failed)]),
            [Failed, Blocked, Ready, Blocked, Blocked]
        );
        assert_eq!(
            states(&[("a", RecordStatus::Cancelled)]),
            [Cancelled, Blocked, Blocked, Blocked, Blocked]
        );
        assert_eq!(
            states(&[
                ("a", RecordStatus::Failed),
                ("b", RecordStatus::Succeeded),
                ("on_failure", RecordStatus::Succeeded),
                ("join", RecordStatus::Failed),
            ]),
            [Failed, Succeeded, Succeeded, Failed, Ready]
        );
    }
}
