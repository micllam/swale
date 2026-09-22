//! The KV records of the orchestrator and their keys. Every key has the
//! `swale/` prefix, and every value is JSON written as an absolute value.
//!
//! - `swale/graphs/{graph}`: the [`GraphRecord`].
//! - `swale/runs/{graph}/{partition}`: the [`GraphRunRecord`].
//! - `swale/assets/{asset}/{partition}`: the [`NodeRecord`] of an asset node.
//! - `swale/tasks/{graph}/{partition}/{node}`: the [`NodeRecord`] of a task
//!   node.
//! - `swale/requests/{id}`: the [`RequestRecord`] of an applied request.

use std::any::type_name;
use std::collections::BTreeMap;
use std::future::Future;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use taquba::{KvPage, Queue, QueueReader};
use taquba_workflow::TerminalStatus;

use crate::graph::Node;
use crate::partition::Partition;
use crate::request::{Request, RequestId};

/// The prefix of every key of this crate.
pub const KV_PREFIX: &str = "swale/";

/// The prefix of every graph record.
pub const GRAPHS_PREFIX: &str = "swale/graphs/";

/// The prefix of every graph run record.
pub const GRAPH_RUNS_PREFIX: &str = "swale/runs/";

/// The key of the graph record.
pub fn graph_key(graph: &str) -> Vec<u8> {
    format!("{GRAPHS_PREFIX}{graph}").into_bytes()
}

/// The graph of a graph record key, or `None` for another key.
pub fn parse_graph_key(key: &[u8]) -> Option<String> {
    let graph = std::str::from_utf8(key).ok()?.strip_prefix(GRAPHS_PREFIX)?;
    crate::graph::is_name(graph).then(|| graph.to_string())
}

/// The key of the graph run record.
pub fn graph_run_key(graph: &str, partition: &Partition) -> Vec<u8> {
    format!("{GRAPH_RUNS_PREFIX}{graph}/{partition}").into_bytes()
}

/// The graph and partition of a graph run key, or `None` for another key.
pub fn parse_graph_run_key(key: &[u8]) -> Option<(String, Partition)> {
    let rest = std::str::from_utf8(key)
        .ok()?
        .strip_prefix(GRAPH_RUNS_PREFIX)?;
    let (graph, partition) = rest.split_once('/')?;
    Some((graph.to_string(), Partition::new(partition).ok()?))
}

/// The key of the asset record.
pub fn asset_key(asset: &str, partition: &Partition) -> Vec<u8> {
    format!("{KV_PREFIX}assets/{asset}/{partition}").into_bytes()
}

/// The key of the task record.
pub fn task_key(graph: &str, partition: &Partition, node: &str) -> Vec<u8> {
    format!("{KV_PREFIX}tasks/{graph}/{partition}/{node}").into_bytes()
}

/// The prefix of every request record.
pub const REQUESTS_PREFIX: &str = "swale/requests/";

/// The key of the request record.
pub fn request_key(id: &RequestId) -> Vec<u8> {
    format!("{REQUESTS_PREFIX}{id}").into_bytes()
}

/// The key of the record of `node` for `partition`: the asset key of an asset
/// node or the task key of a task node.
pub fn node_record_key(graph: &str, partition: &Partition, node: &Node) -> Vec<u8> {
    record_key(graph, partition, node.name(), node.asset())
}

/// The key of the record of the node `node` for `partition`: the asset key
/// when the node produces `asset`, the task key otherwise.
pub fn record_key(graph: &str, partition: &Partition, node: &str, asset: Option<&str>) -> Vec<u8> {
    match asset {
        Some(asset) => asset_key(asset, partition),
        None => task_key(graph, partition, node),
    }
}

/// The terminal status of a task instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordStatus {
    /// The run succeeded.
    Succeeded,
    /// The run failed: the operator reported a failure, or the step was
    /// dead-lettered.
    Failed,
    /// The run was cancelled.
    Cancelled,
}

impl RecordStatus {
    /// The lowercase name, as in the JSON form.
    pub fn as_str(&self) -> &'static str {
        match self {
            RecordStatus::Succeeded => "succeeded",
            RecordStatus::Failed => "failed",
            RecordStatus::Cancelled => "cancelled",
        }
    }
}

impl std::fmt::Display for RecordStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<TerminalStatus> for RecordStatus {
    fn from(status: TerminalStatus) -> Self {
        match status {
            TerminalStatus::Succeeded => RecordStatus::Succeeded,
            TerminalStatus::Failed => RecordStatus::Failed,
            TerminalStatus::Cancelled => RecordStatus::Cancelled,
        }
    }
}

/// The record a terminated task instance leaves: the asset record of an asset
/// node or the task record of a task node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    /// The terminal status.
    pub status: RecordStatus,
    /// The run id of the task instance.
    pub run_id: String,
    /// The definition hash the graph run records.
    pub definition: String,
    /// The rerun count of the task instance.
    pub rerun: u32,
    /// The time of the termination, in milliseconds from the Unix epoch.
    pub terminated_at_ms: u64,
    /// The output of a succeeded run, when the record stays within the KV
    /// value cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    /// Whether the output was left out because the record exceeded the cap.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub output_omitted: bool,
    /// The error of a failed or cancelled run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The state of a graph run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphRunState {
    /// Nodes are running or ready to run.
    Active,
    /// The run was cancelled, and no further node is submitted.
    Cancelled,
    /// Every node has a succeeded record.
    Complete,
    /// No node is ready or running and at least one record is failed.
    Failed,
}

impl GraphRunState {
    /// The lowercase name, as in the JSON form.
    pub fn as_str(&self) -> &'static str {
        match self {
            GraphRunState::Active => "active",
            GraphRunState::Cancelled => "cancelled",
            GraphRunState::Complete => "complete",
            GraphRunState::Failed => "failed",
        }
    }
}

impl std::fmt::Display for GraphRunState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The record of a graph run for one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphRunRecord {
    /// The definition hash the run records.
    pub definition: String,
    /// The time of the request, in milliseconds from the Unix epoch.
    pub requested_at_ms: u64,
    /// The state.
    pub state: GraphRunState,
    /// The rerun count each listed node must reach for its record to be
    /// current, by node name. A rerun of a succeeded node lists the node and
    /// the nodes downstream of it through an all-succeeded edge, and the
    /// settle of the run removes an entry that a record reached.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub expected_reruns: BTreeMap<String, u32>,
}

impl GraphRunRecord {
    /// Whether `record`, the record of `node`, is at or past the rerun
    /// count the run expects of the node. A record below the count is
    /// superseded by a rerun, and the readiness rule treats it as absent.
    pub fn is_current(&self, node: &str, record: &NodeRecord) -> bool {
        record.rerun >= self.expected_reruns.get(node).copied().unwrap_or(0)
    }
}

/// The record of a graph the process adopted: the definition that a trigger
/// of the graph starts. The daemon is the only writer of the record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphRecord {
    /// The hash of the adopted definition.
    pub definition: String,
    /// The time of the adoption, in milliseconds from the Unix epoch.
    pub adopted_at_ms: u64,
}

/// The outcome of a request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RequestOutcome {
    /// The graph runs of the listed partitions were started. A partition of
    /// the request that is absent from the list had a graph run before.
    Started {
        /// The partitions whose graph run the request started.
        partitions: Vec<Partition>,
    },
    /// The task instance of the rerun was submitted.
    Rerun {
        /// The run id of the task instance.
        run_id: String,
    },
    /// The graph run was cancelled.
    Cancelled,
    /// The request was refused.
    Refused {
        /// The reason.
        reason: String,
    },
}

/// The record of an applied request. The daemon is the only writer of the
/// record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestRecord {
    /// The request.
    pub request: Request,
    /// The time the request was applied, in milliseconds from the Unix
    /// epoch.
    pub handled_at_ms: u64,
    /// The outcome.
    pub outcome: RequestOutcome,
}

/// A type with a JSON byte form: the records, and the payloads of the
/// queues and the request objects.
pub trait JsonBytes: Serialize + DeserializeOwned {
    /// The JSON form, with maps in key order.
    fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self)
            .unwrap_or_else(|e| panic!("{} serializes to JSON: {e}", type_name::<Self>()))
    }

    /// Parses the JSON form.
    fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

impl JsonBytes for NodeRecord {}
impl JsonBytes for GraphRunRecord {}
impl JsonBytes for GraphRecord {}
impl JsonBytes for RequestRecord {}

/// The value at a key is not the record the key names.
#[derive(Debug, thiserror::Error)]
#[error("record `{key}` is not a record: {source}")]
pub struct RecordError {
    /// The key.
    pub key: String,
    /// The parser's error.
    #[source]
    pub source: serde_json::Error,
}

/// Parses the value `bytes` at `key` as a `T`.
pub fn parse<T: JsonBytes>(key: &[u8], bytes: &[u8]) -> Result<T, RecordError> {
    T::from_bytes(bytes).map_err(|source| RecordError {
        key: String::from_utf8_lossy(key).into_owned(),
        source,
    })
}

/// A failure of a record read.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// The queue failed.
    #[error(transparent)]
    Queue(#[from] taquba::Error),
    /// The value is not the record.
    #[error(transparent)]
    Record(#[from] RecordError),
}

/// A reader of the KV namespace: the queue of the process that opened the
/// store, or a reader of another process.
pub trait KvRead: Sync {
    /// The value at `key`.
    fn kv_get(&self, key: &[u8]) -> impl Future<Output = taquba::Result<Option<Bytes>>> + Send;

    /// One page of the entries with `prefix`, from `cursor`.
    fn kv_scan(
        &self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> impl Future<Output = taquba::Result<KvPage>> + Send;
}

impl KvRead for Queue {
    async fn kv_get(&self, key: &[u8]) -> taquba::Result<Option<Bytes>> {
        Queue::kv_get(self, key).await
    }

    async fn kv_scan(
        &self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> taquba::Result<KvPage> {
        Queue::kv_scan(self, prefix, cursor, limit).await
    }
}

impl KvRead for QueueReader {
    async fn kv_get(&self, key: &[u8]) -> taquba::Result<Option<Bytes>> {
        QueueReader::kv_get(self, key).await
    }

    async fn kv_scan(
        &self,
        prefix: &[u8],
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> taquba::Result<KvPage> {
        QueueReader::kv_scan(self, prefix, cursor, limit).await
    }
}

/// The entries of one scan page.
const PAGE: usize = 256;

/// The record at `key`, or `None` when there is no value at the key.
pub async fn read<T: JsonBytes>(kv: &impl KvRead, key: &[u8]) -> Result<Option<T>, ReadError> {
    match kv.kv_get(key).await? {
        Some(bytes) => Ok(Some(parse(key, &bytes)?)),
        None => Ok(None),
    }
}

/// A record of a listing, with its key and the bytes it was parsed from.
#[derive(Debug, Clone)]
pub struct Entry<T> {
    /// The key.
    pub key: Vec<u8>,
    /// The stored bytes, the expected value of a compare-and-put.
    pub bytes: Bytes,
    /// The record.
    pub record: T,
}

/// Every record with `prefix`, in key order. A value that is not a `T` is
/// logged and skipped, so one malformed record does not end a listing.
pub async fn scan<T: JsonBytes>(
    kv: &impl KvRead,
    prefix: &[u8],
) -> Result<Vec<Entry<T>>, taquba::Error> {
    let mut records = Vec::new();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let page = kv.kv_scan(prefix, cursor.as_deref(), PAGE).await?;
        for (key, bytes) in page.entries {
            match parse::<T>(&key, &bytes) {
                Ok(record) => records.push(Entry { key, bytes, record }),
                Err(e) => tracing::warn!(error = %e, "record skipped"),
            }
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(records),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_have_the_documented_layout() {
        let partition = Partition::new("20260915").unwrap();
        assert_eq!(graph_key("orders_daily"), b"swale/graphs/orders_daily");
        assert_eq!(
            graph_run_key("orders_daily", &partition),
            b"swale/runs/orders_daily/20260915"
        );
        assert_eq!(
            asset_key("orders_raw", &partition),
            b"swale/assets/orders_raw/20260915"
        );
        assert_eq!(
            task_key("orders_daily", &partition, "notify"),
            b"swale/tasks/orders_daily/20260915/notify"
        );
        assert_eq!(
            request_key(&RequestId::new("01J").unwrap()),
            b"swale/requests/01J"
        );
        assert_eq!(
            parse_graph_key(b"swale/graphs/orders_daily"),
            Some("orders_daily".to_string())
        );
        assert_eq!(parse_graph_key(b"swale/runs/orders_daily/20260915"), None);
        assert_eq!(
            parse_graph_run_key(b"swale/runs/orders_daily/20260915"),
            Some(("orders_daily".to_string(), partition))
        );
        assert_eq!(
            parse_graph_run_key(b"swale/assets/orders_raw/20260915"),
            None
        );
        assert_eq!(
            parse_graph_run_key(b"swale/runs/orders_daily/2026-09"),
            None
        );
    }

    #[test]
    fn records_round_trip_through_json_with_optional_fields_left_out() {
        let record = NodeRecord {
            status: RecordStatus::Succeeded,
            run_id: "g-p-n-r0".into(),
            definition: "abc".into(),
            rerun: 0,
            terminated_at_ms: 5,
            output: None,
            output_omitted: false,
            error: None,
        };
        let json = String::from_utf8(record.to_bytes()).unwrap();
        assert!(!json.contains("output"), "{json}");
        assert!(!json.contains("error"), "{json}");
        assert_eq!(NodeRecord::from_bytes(json.as_bytes()).unwrap(), record);

        let run = GraphRunRecord {
            definition: "abc".into(),
            requested_at_ms: 7,
            state: GraphRunState::Active,
            expected_reruns: BTreeMap::new(),
        };
        let json = String::from_utf8(run.to_bytes()).unwrap();
        assert!(!json.contains("expected_reruns"), "{json}");
        assert_eq!(GraphRunRecord::from_bytes(json.as_bytes()).unwrap(), run);
        let run = GraphRunRecord {
            expected_reruns: BTreeMap::from([("transform".to_string(), 1)]),
            ..run
        };
        assert_eq!(GraphRunRecord::from_bytes(&run.to_bytes()).unwrap(), run);
        assert!(run.is_current("extract", &record));
        assert!(!run.is_current("transform", &record));
        assert_eq!(
            RecordStatus::from(TerminalStatus::Cancelled),
            RecordStatus::Cancelled
        );
        assert_eq!(RecordStatus::Failed.to_string(), "failed");
        assert_eq!(GraphRunState::Complete.to_string(), "complete");

        let request = RequestRecord {
            request: Request::Cancel {
                graph: "g".into(),
                partition: Partition::new("20260915").unwrap(),
            },
            handled_at_ms: 9,
            outcome: RequestOutcome::Refused {
                reason: "no".into(),
            },
        };
        let json = String::from_utf8(request.to_bytes()).unwrap();
        assert_eq!(
            json,
            r#"{"request":{"kind":"cancel","graph":"g","partition":"20260915"},"handled_at_ms":9,"outcome":{"kind":"refused","reason":"no"}}"#
        );
        assert_eq!(RequestRecord::from_bytes(json.as_bytes()).unwrap(), request);
    }
}
