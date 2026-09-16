//! The identity of a task instance: the graph, the partition, the node and
//! the rerun count, which together form the run id and the `swale.` headers
//! of the run.

use std::collections::HashMap;

use taquba_workflow::RunId;

use crate::partition::{InvalidPartition, Partition};
use crate::records;

/// Header with the graph name.
pub const HEADER_GRAPH: &str = "swale.graph";
/// Header with the partition key.
pub const HEADER_PARTITION: &str = "swale.partition";
/// Header with the node name.
pub const HEADER_NODE: &str = "swale.node";
/// Header with the asset name, present on an asset node only.
pub const HEADER_ASSET: &str = "swale.asset";
/// Header with the definition hash the graph run records.
pub const HEADER_DEFINITION: &str = "swale.definition";
/// Header with the rerun count.
pub const HEADER_RERUN: &str = "swale.rerun";

/// The identity of one task instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskIdentity {
    /// The graph name.
    pub graph: String,
    /// The partition.
    pub partition: Partition,
    /// The node name.
    pub node: String,
    /// The asset the node produces, absent for a task node.
    pub asset: Option<String>,
    /// The definition hash the graph run records.
    pub definition: String,
    /// The rerun count of the node for the partition.
    pub rerun: u32,
}

/// The headers of a run do not identify a task instance.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentityError {
    /// A required header is absent.
    #[error("header `{0}` is absent")]
    MissingHeader(&'static str),
    /// The partition header is not a partition key.
    #[error("header `{HEADER_PARTITION}`: {0}")]
    Partition(#[from] InvalidPartition),
    /// The rerun header is not a count.
    #[error("header `{HEADER_RERUN}` is not a count: `{0}`")]
    Rerun(String),
}

impl TaskIdentity {
    /// The run id `{graph}-{partition}-{node}-r{rerun}`. The graph and node
    /// names are bounded by the graph checks so that the id is within the
    /// runtime's limit.
    pub fn run_id(&self) -> RunId {
        RunId::new(format!(
            "{}-{}-{}-r{}",
            self.graph, self.partition, self.node, self.rerun
        ))
        .expect("graph and node names are bounded so that every run id is valid")
    }

    /// The `swale.` headers of the run.
    pub fn headers(&self) -> HashMap<String, String> {
        let mut headers = HashMap::from([
            (HEADER_GRAPH.to_string(), self.graph.clone()),
            (HEADER_PARTITION.to_string(), self.partition.to_string()),
            (HEADER_NODE.to_string(), self.node.clone()),
            (HEADER_DEFINITION.to_string(), self.definition.clone()),
            (HEADER_RERUN.to_string(), self.rerun.to_string()),
        ]);
        if let Some(asset) = &self.asset {
            headers.insert(HEADER_ASSET.to_string(), asset.clone());
        }
        headers
    }

    /// Reads the identity from the `swale.` headers of a run.
    pub fn from_headers(headers: &HashMap<String, String>) -> Result<Self, IdentityError> {
        let required = |name: &'static str| {
            headers
                .get(name)
                .cloned()
                .ok_or(IdentityError::MissingHeader(name))
        };
        let rerun = required(HEADER_RERUN)?;
        Ok(TaskIdentity {
            graph: required(HEADER_GRAPH)?,
            partition: Partition::new(required(HEADER_PARTITION)?)?,
            node: required(HEADER_NODE)?,
            asset: headers.get(HEADER_ASSET).cloned(),
            definition: required(HEADER_DEFINITION)?,
            rerun: rerun.parse().map_err(|_| IdentityError::Rerun(rerun))?,
        })
    }

    /// The KV key of the node's record: the asset record of an asset node or
    /// the task record of a task node (see [`records`]).
    pub fn record_key(&self) -> Vec<u8> {
        match &self.asset {
            Some(asset) => records::asset_key(asset, &self.partition),
            None => records::task_key(&self.graph, &self.partition, &self.node),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(asset: Option<&str>) -> TaskIdentity {
        TaskIdentity {
            graph: "orders_daily".into(),
            partition: Partition::new("20260915").unwrap(),
            node: "extract".into(),
            asset: asset.map(str::to_string),
            definition: "abc".into(),
            rerun: 2,
        }
    }

    #[test]
    fn run_id_has_the_documented_form() {
        assert_eq!(
            identity(Some("orders_raw")).run_id().as_str(),
            "orders_daily-20260915-extract-r2"
        );
    }

    #[test]
    fn headers_round_trip_with_and_without_an_asset() {
        for asset in [Some("orders_raw"), None] {
            let identity = identity(asset);
            let headers = identity.headers();
            assert_eq!(headers.contains_key(HEADER_ASSET), asset.is_some());
            assert!(headers.keys().all(|k| k.starts_with("swale.")));
            assert_eq!(TaskIdentity::from_headers(&headers).unwrap(), identity);
        }
    }

    #[test]
    fn record_key_is_the_asset_key_or_the_task_key() {
        assert_eq!(
            identity(Some("orders_raw")).record_key(),
            b"swale/assets/orders_raw/20260915"
        );
        assert_eq!(
            identity(None).record_key(),
            b"swale/tasks/orders_daily/20260915/extract"
        );
    }

    #[test]
    fn missing_and_malformed_headers_are_reported() {
        let mut headers = identity(None).headers();
        headers.insert(HEADER_RERUN.into(), "x".into());
        assert_eq!(
            TaskIdentity::from_headers(&headers),
            Err(IdentityError::Rerun("x".into()))
        );
        headers.insert(HEADER_RERUN.into(), "0".into());
        headers.insert(HEADER_PARTITION.into(), "2026-09-15".into());
        assert!(matches!(
            TaskIdentity::from_headers(&headers),
            Err(IdentityError::Partition(_))
        ));
        headers.remove(HEADER_GRAPH);
        headers.insert(HEADER_PARTITION.into(), "20260915".into());
        assert_eq!(
            TaskIdentity::from_headers(&headers),
            Err(IdentityError::MissingHeader(HEADER_GRAPH))
        );
    }
}
