//! The readiness rule: when a node runs, and when a graph run is finished.
//!
//! The rule reads the graph run record and the node records of one graph run
//! and nothing else. A record below the rerun count the graph run expects of
//! its node is superseded by a rerun, and the rule reads the current records
//! alone ([`current_records`]). A node without a current record is ready when
//! the records of its upstreams satisfy its trigger rule ([`is_ready`]). It
//! is waiting when a record that follows can satisfy the rule, and blocked
//! otherwise ([`node_states`]). A graph run is finished when no node is ready
//! or waiting ([`settled_state`]): complete when every node is succeeded, and
//! failed otherwise. A rerun of a node reaches the nodes downstream of it
//! through an all-succeeded edge ([`rerun_scope`]). The scheduler applies the
//! rule to submit and to settle, and the status view applies the same rule
//! to derive the state of a node.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::graph::{Graph, Node, TriggerRule};
use crate::records::{GraphRunRecord, GraphRunState, NodeRecord, RecordStatus};

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

/// The records among `records`, by node name, that `run` treats as
/// current: a record at or past the rerun count the run expects of its node.
pub fn current_records(
    run: &GraphRunRecord,
    records: &BTreeMap<String, NodeRecord>,
) -> BTreeMap<String, NodeRecord> {
    records
        .iter()
        .filter(|(name, record)| run.is_current(name, record))
        .map(|(name, record)| (name.clone(), record.clone()))
        .collect()
}

/// The nodes a rerun of `node` runs again, in definition order: the node
/// and every node downstream of it through a node with the all-succeeded
/// rule. A task node with the all-done or one-failed rule keeps its record,
/// and the nodes downstream of it are not reached through it.
pub fn rerun_scope<'a>(graph: &'a Graph, node: &'a Node) -> Vec<&'a Node> {
    let mut scope = BTreeSet::from([node.name()]);
    let mut pending = vec![node];
    while let Some(current) = pending.pop() {
        for name in current.downstreams() {
            let downstream = graph
                .node(name)
                .expect("a downstream name is a node of the graph");
            if downstream.trigger_rule() == TriggerRule::AllSucceeded
                && scope.insert(downstream.name())
            {
                pending.push(downstream);
            }
        }
    }
    graph
        .nodes()
        .iter()
        .filter(|node| scope.contains(node.name()))
        .collect()
}

/// Whether the records of a node's upstreams satisfy its trigger rule.
/// `records` is by node name, and an entry of another node is not read.
pub fn is_ready(node: &Node, records: &BTreeMap<String, NodeRecord>) -> bool {
    let status = |name: &String| records.get(name).map(|r| r.status);
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
            let state = if is_ready(node, records) {
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

/// The final state of a graph run from the states of its nodes, or `None`
/// while a node is ready or waiting: complete when every node is succeeded,
/// and failed otherwise.
pub fn settled_state(states: &BTreeMap<String, NodeState>) -> Option<GraphRunState> {
    if states.values().any(NodeState::is_open) {
        return None;
    }
    if states.values().all(|state| *state == NodeState::Succeeded) {
        Some(GraphRunState::Complete)
    } else {
        Some(GraphRunState::Failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{GraphSpec, NodeKind, NodeSpec};
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
            run_id: "run".into(),
            definition: "abc".into(),
            rerun: 0,
            terminated_at_ms: 0,
            output: None,
            output_omitted: false,
            error: None,
        }
    }

    fn records(records: &[(&str, RecordStatus)]) -> BTreeMap<String, NodeRecord> {
        records
            .iter()
            .map(|(name, status)| (name.to_string(), record(*status)))
            .collect()
    }

    fn states(records: &[(&str, RecordStatus)]) -> BTreeMap<String, NodeState> {
        let graph = crate::load_str(DEFINITION, &OperatorSet::builtin()).unwrap();
        node_states(&graph, &self::records(records))
    }

    fn in_order(states: &BTreeMap<String, NodeState>) -> Vec<NodeState> {
        ["a", "b", "on_failure", "join", "report"]
            .map(|name| states[name])
            .to_vec()
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
        let both_ok = records(&[
            ("a", RecordStatus::Succeeded),
            ("b", RecordStatus::Succeeded),
        ]);
        let one_failed = records(&[("a", RecordStatus::Succeeded), ("b", RecordStatus::Failed)]);
        let one_missing = records(&[("a", RecordStatus::Succeeded)]);

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

    #[test]
    fn a_node_without_a_record_is_ready_waiting_or_blocked_by_its_trigger_rule() {
        use NodeState::*;
        assert_eq!(
            in_order(&states(&[])),
            [Ready, Waiting, Waiting, Waiting, Waiting]
        );
        // A succeeded `a` blocks `on_failure`, which blocks `join` and
        // `report`.
        assert_eq!(
            in_order(&states(&[("a", RecordStatus::Succeeded)])),
            [Succeeded, Ready, Blocked, Blocked, Blocked]
        );
        assert_eq!(
            in_order(&states(&[("a", RecordStatus::Failed)])),
            [Failed, Blocked, Ready, Blocked, Blocked]
        );
        assert_eq!(
            in_order(&states(&[("a", RecordStatus::Cancelled)])),
            [Cancelled, Blocked, Blocked, Blocked, Blocked]
        );
        assert_eq!(
            in_order(&states(&[
                ("a", RecordStatus::Failed),
                ("b", RecordStatus::Succeeded),
                ("on_failure", RecordStatus::Succeeded),
                ("join", RecordStatus::Failed),
            ])),
            [Failed, Succeeded, Succeeded, Failed, Ready]
        );
    }

    #[test]
    fn a_superseded_record_reads_as_absent() {
        use NodeState::*;
        let graph = crate::load_str(DEFINITION, &OperatorSet::builtin()).unwrap();
        let all_succeeded =
            ["a", "b", "on_failure", "join", "report"].map(|name| (name, RecordStatus::Succeeded));
        let records = self::records(&all_succeeded);
        let run = GraphRunRecord {
            definition: "abc".into(),
            requested_at_ms: 0,
            state: GraphRunState::Active,
            settled_at_ms: None,
            expected_reruns: BTreeMap::from([("b".to_string(), 1), ("join".to_string(), 1)]),
        };
        let current = current_records(&run, &records);
        assert_eq!(
            current.keys().collect::<Vec<_>>(),
            ["a", "on_failure", "report"]
        );
        assert_eq!(
            in_order(&node_states(&graph, &current)),
            [Succeeded, Ready, Succeeded, Waiting, Succeeded]
        );
        assert_eq!(settled_state(&node_states(&graph, &current)), None);
    }

    #[test]
    fn a_rerun_reaches_the_downstreams_through_all_succeeded_edges() {
        let graph = crate::load_str(DEFINITION, &OperatorSet::builtin()).unwrap();
        let scope = |name: &str| -> Vec<&str> {
            rerun_scope(&graph, graph.node(name).unwrap())
                .iter()
                .map(|node| node.name())
                .collect()
        };
        // `on_failure` and `report` keep their records, and `join` is
        // reached through `b`.
        assert_eq!(scope("a"), ["a", "b", "join"]);
        assert_eq!(scope("b"), ["b", "join"]);
        assert_eq!(scope("on_failure"), ["on_failure", "join"]);
        assert_eq!(scope("report"), ["report"]);
    }

    #[test]
    fn a_graph_run_settles_when_no_node_is_ready_or_waiting() {
        // `a` is ready, then `b` is ready, then `on_failure` is ready.
        assert_eq!(settled_state(&states(&[])), None);
        assert_eq!(
            settled_state(&states(&[("a", RecordStatus::Succeeded)])),
            None
        );
        assert_eq!(settled_state(&states(&[("a", RecordStatus::Failed)])), None);
        // A cancelled `a` blocks every other node.
        assert_eq!(
            settled_state(&states(&[("a", RecordStatus::Cancelled)])),
            Some(GraphRunState::Failed)
        );
        let all_but_report = [
            ("a", RecordStatus::Failed),
            ("b", RecordStatus::Succeeded),
            ("on_failure", RecordStatus::Succeeded),
            ("join", RecordStatus::Failed),
        ];
        assert_eq!(settled_state(&states(&all_but_report)), None);
        let mut every_node = all_but_report.to_vec();
        every_node.push(("report", RecordStatus::Succeeded));
        assert_eq!(
            settled_state(&states(&every_node)),
            Some(GraphRunState::Failed)
        );
        let all_succeeded =
            ["a", "b", "on_failure", "join", "report"].map(|name| (name, RecordStatus::Succeeded));
        assert_eq!(
            settled_state(&states(&all_succeeded)),
            Some(GraphRunState::Complete)
        );
    }
}
