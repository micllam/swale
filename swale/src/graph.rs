//! The internal graph model: nodes, their edges and the checks a graph passes
//! before it runs. Every front-end builds a [`Graph`] from a [`GraphSpec`]
//! through [`Graph::build`].

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use taquba_cron::Expression;

use crate::operator::{OperatorError, OperatorSet};
use crate::template::Template;

/// Maximum length of a graph name and a node name together. A run id is
/// `{graph}-{partition}-{node}-r{n}` with a partition of up to 16 bytes and a
/// rerun count of up to 10 digits, and the runtime caps the id at 128 bytes.
pub const MAX_GRAPH_AND_NODE_NAME_LEN: usize = 98;

/// How the assets of a graph are partitioned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Partitioning {
    /// One partition per day, formatted `20260915`.
    Daily,
    /// One partition per hour, formatted `20260915T02`.
    Hourly,
    /// A single partition, formatted `none`.
    #[default]
    Unpartitioned,
}

/// The condition on the upstream task instances of a task node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TriggerRule {
    /// Every upstream succeeded.
    #[default]
    AllSucceeded,
    /// Every upstream terminated.
    AllDone,
    /// At least one upstream failed.
    OneFailed,
}

/// What a node is: an asset node or a task node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeKind {
    /// A node that materialises an asset. Its upstreams are the producers of
    /// the assets it consumes.
    Asset {
        /// The asset the node produces.
        produces: String,
        /// The assets the node consumes.
        consumes: Vec<String>,
    },
    /// A node without an asset. Its upstreams are declared directly.
    Task {
        /// The upstream nodes.
        after: Vec<String>,
        /// The condition on the upstreams.
        trigger_rule: TriggerRule,
    },
}

/// The input of one node to [`Graph::build`].
#[derive(Debug, Clone, PartialEq)]
pub struct NodeSpec {
    /// The node name, unique in the graph.
    pub name: String,
    /// Asset node or task node.
    pub kind: NodeKind,
    /// The operator name, registered in the [`OperatorSet`].
    pub operator: String,
    /// The pool the node runs in.
    pub pool: String,
    /// The retries after the first attempt.
    pub retries: u32,
    /// The operator parameters. A string value can contain template
    /// references.
    pub params: toml::Table,
}

/// The input to [`Graph::build`].
#[derive(Debug, Clone, PartialEq)]
pub struct GraphSpec {
    /// The graph name.
    pub name: String,
    /// A cron expression with five fields.
    pub schedule: Option<String>,
    /// The catch-up window of a newly published graph.
    pub catchup: Option<Duration>,
    /// The partitioning of the graph's assets.
    pub partitioning: Partitioning,
    /// The nodes, in any order.
    pub nodes: Vec<NodeSpec>,
}

/// A node of a built [`Graph`].
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    spec: NodeSpec,
    upstreams: Vec<String>,
    downstreams: Vec<String>,
}

impl Node {
    /// The node name.
    pub fn name(&self) -> &str {
        &self.spec.name
    }

    /// Asset node or task node.
    pub fn kind(&self) -> &NodeKind {
        &self.spec.kind
    }

    /// The asset the node produces, absent for a task node.
    pub fn asset(&self) -> Option<&str> {
        match &self.spec.kind {
            NodeKind::Asset { produces, .. } => Some(produces),
            NodeKind::Task { .. } => None,
        }
    }

    /// The condition on the upstreams. An asset node has
    /// [`TriggerRule::AllSucceeded`].
    pub fn trigger_rule(&self) -> TriggerRule {
        match &self.spec.kind {
            NodeKind::Asset { .. } => TriggerRule::AllSucceeded,
            NodeKind::Task { trigger_rule, .. } => *trigger_rule,
        }
    }

    /// The operator name.
    pub fn operator(&self) -> &str {
        &self.spec.operator
    }

    /// The pool the node runs in.
    pub fn pool(&self) -> &str {
        &self.spec.pool
    }

    /// The retries after the first attempt.
    pub fn retries(&self) -> u32 {
        self.spec.retries
    }

    /// The operator parameters.
    pub fn params(&self) -> &toml::Table {
        &self.spec.params
    }

    /// The names of the upstream nodes, in lexical order.
    pub fn upstreams(&self) -> &[String] {
        &self.upstreams
    }

    /// The names of the downstream nodes, in lexical order.
    pub fn downstreams(&self) -> &[String] {
        &self.downstreams
    }
}

/// A checked graph. The nodes are stored in definition order.
#[derive(Debug, Clone, PartialEq)]
pub struct Graph {
    name: String,
    schedule: Option<Expression>,
    catchup: Option<Duration>,
    partitioning: Partitioning,
    nodes: Vec<Node>,
    index: BTreeMap<String, usize>,
}

/// A fault in a [`GraphSpec`]. The message identifies the node where one
/// applies.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Problem {
    /// The graph name is outside `[a-z0-9_]+`.
    #[error("graph name `{0}` is not `[a-z0-9_]+`")]
    InvalidGraphName(String),
    /// The schedule is not a cron expression.
    #[error("schedule `{expression}` is not a cron expression: {message}")]
    InvalidSchedule {
        /// The schedule text.
        expression: String,
        /// The parser's message.
        message: String,
    },
    /// The graph has a schedule and a single partition. Every firing of a
    /// schedule runs a partition of its own.
    #[error("a graph with a schedule declares `partition` as `daily` or `hourly`")]
    ScheduleWithoutPartition,
    /// The catch-up window is not a duration.
    #[error("catchup: {0}")]
    InvalidCatchup(String),
    /// The graph is empty.
    #[error("the graph is empty")]
    NoNodes,
    /// An asset node declares `after`.
    #[error(
        "node `{0}`: an asset node declares its upstreams with `consumes` and does not declare `after`"
    )]
    AssetNodeAfter(String),
    /// An asset node declares a trigger rule.
    #[error(
        "node `{0}`: an asset node does not declare `trigger_rule`, because its upstreams must all succeed"
    )]
    AssetNodeTriggerRule(String),
    /// A task node declares `consumes`.
    #[error(
        "node `{0}`: a task node declares its upstreams with `after` and does not declare `consumes`"
    )]
    TaskNodeConsumes(String),
    /// Two nodes have the same name.
    #[error("node `{0}` is defined more than once")]
    DuplicateNode(String),
    /// A node name is outside `[a-z0-9_]+`.
    #[error("node `{0}`: the name is not `[a-z0-9_]+`")]
    InvalidNodeName(String),
    /// The graph name and the node name exceed
    /// [`MAX_GRAPH_AND_NODE_NAME_LEN`] together.
    #[error(
        "node `{0}`: the graph and node names exceed {MAX_GRAPH_AND_NODE_NAME_LEN} bytes together"
    )]
    NamesTooLong(String),
    /// An asset name is outside `[a-z0-9_]+`.
    #[error("node `{node}`: asset name `{asset}` is not `[a-z0-9_]+`")]
    InvalidAssetName {
        /// The node that declares the asset.
        node: String,
        /// The asset name.
        asset: String,
    },
    /// Two nodes produce the same asset.
    #[error("asset `{asset}` is produced by more than one node: {}", .nodes.join(", "))]
    DuplicateAsset {
        /// The asset name.
        asset: String,
        /// The producing nodes.
        nodes: Vec<String>,
    },
    /// A consumed asset is not produced in the graph.
    #[error("node `{node}`: consumes `{asset}`, which no node produces")]
    UnknownAsset {
        /// The consuming node.
        node: String,
        /// The asset name.
        asset: String,
    },
    /// An `after` entry is not a node name.
    #[error("node `{node}`: `after` refers to unknown node `{after}`")]
    UnknownNode {
        /// The declaring node.
        node: String,
        /// The unknown name.
        after: String,
    },
    /// The operator is not registered.
    #[error("node `{node}`: unknown operator `{operator}`")]
    UnknownOperator {
        /// The node.
        node: String,
        /// The operator name.
        operator: String,
    },
    /// The parameters do not deserialize into the operator's type.
    #[error("node `{node}`: the parameters of operator `{operator}` are invalid: {message}")]
    InvalidParams {
        /// The node.
        node: String,
        /// The operator name.
        operator: String,
        /// The deserializer's message.
        message: String,
    },
    /// A parameter string is not a template.
    #[error("node `{node}`: parameter `{param}` is not a template: {message}")]
    InvalidTemplate {
        /// The node.
        node: String,
        /// The parameter path, dotted.
        param: String,
        /// The parser's message.
        message: String,
    },
    /// A template refers to a node that is not an upstream of the node.
    #[error(
        "node `{node}`: parameter `{param}` refers to `{upstream}`, which is not an upstream of the node"
    )]
    UnknownTemplateUpstream {
        /// The node.
        node: String,
        /// The parameter path, dotted.
        param: String,
        /// The referenced node.
        upstream: String,
    },
    /// The edges form a cycle. The path lists the nodes along the cycle, each
    /// an upstream of the one before it, with the first node repeated at the
    /// end.
    #[error("the graph has a cycle: {}", .0.join(" -> "))]
    Cycle(Vec<String>),
}

impl Graph {
    /// Builds and checks a graph. Every fault found is returned, and a graph
    /// is returned only when no fault is found.
    pub fn build(spec: GraphSpec, operators: &OperatorSet) -> Result<Graph, Vec<Problem>> {
        let mut problems = Vec::new();

        if !is_name(&spec.name) {
            problems.push(Problem::InvalidGraphName(spec.name.clone()));
        }
        let mut schedule = None;
        if let Some(expression) = &spec.schedule {
            match expression.parse::<Expression>() {
                Ok(parsed) => schedule = Some(parsed),
                Err(e) => {
                    let message = match e {
                        taquba_cron::Error::InvalidExpression { message, .. } => message,
                        other => other.to_string(),
                    };
                    problems.push(Problem::InvalidSchedule {
                        expression: expression.clone(),
                        message,
                    });
                }
            }
        }
        if spec.schedule.is_some() && spec.partitioning == Partitioning::Unpartitioned {
            problems.push(Problem::ScheduleWithoutPartition);
        }
        if spec.nodes.is_empty() {
            problems.push(Problem::NoNodes);
        }

        let mut names = BTreeSet::new();
        for node in &spec.nodes {
            if !is_name(&node.name) {
                problems.push(Problem::InvalidNodeName(node.name.clone()));
            } else if spec.name.len() + node.name.len() > MAX_GRAPH_AND_NODE_NAME_LEN {
                problems.push(Problem::NamesTooLong(node.name.clone()));
            }
            if !names.insert(node.name.as_str()) {
                problems.push(Problem::DuplicateNode(node.name.clone()));
            }
        }

        let mut producers: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for node in &spec.nodes {
            if let NodeKind::Asset { produces, consumes } = &node.kind {
                for asset in std::iter::once(produces).chain(consumes) {
                    if !is_name(asset) {
                        problems.push(Problem::InvalidAssetName {
                            node: node.name.clone(),
                            asset: asset.clone(),
                        });
                    }
                }
                producers.entry(produces).or_default().push(&node.name);
            }
        }
        for (asset, nodes) in &producers {
            if nodes.len() > 1 {
                problems.push(Problem::DuplicateAsset {
                    asset: asset.to_string(),
                    nodes: nodes.iter().map(|n| n.to_string()).collect(),
                });
            }
        }

        let mut upstreams: Vec<BTreeSet<String>> = Vec::with_capacity(spec.nodes.len());
        for node in &spec.nodes {
            let mut set = BTreeSet::new();
            match &node.kind {
                NodeKind::Asset { consumes, .. } => {
                    for asset in consumes {
                        match producers.get(asset.as_str()) {
                            Some(nodes) => set.extend(nodes.iter().map(|n| n.to_string())),
                            None => problems.push(Problem::UnknownAsset {
                                node: node.name.clone(),
                                asset: asset.clone(),
                            }),
                        }
                    }
                }
                NodeKind::Task { after, .. } => {
                    for name in after {
                        if names.contains(name.as_str()) {
                            set.insert(name.clone());
                        } else {
                            problems.push(Problem::UnknownNode {
                                node: node.name.clone(),
                                after: name.clone(),
                            });
                        }
                    }
                }
            }
            upstreams.push(set);
        }

        for (node, upstream) in spec.nodes.iter().zip(&upstreams) {
            match operators.check(&node.operator, &node.params) {
                Ok(()) => {}
                Err(OperatorError::Unknown) => problems.push(Problem::UnknownOperator {
                    node: node.name.clone(),
                    operator: node.operator.clone(),
                }),
                Err(OperatorError::InvalidParams(message)) => {
                    problems.push(Problem::InvalidParams {
                        node: node.name.clone(),
                        operator: node.operator.clone(),
                        message,
                    })
                }
            }
            check_templates(
                &node.name,
                "",
                &toml::Value::Table(node.params.clone()),
                upstream,
                &mut problems,
            );
        }

        if !problems.is_empty() {
            return Err(problems);
        }

        if let Some(cycle) = find_cycle(&spec.nodes, &upstreams) {
            return Err(vec![Problem::Cycle(cycle)]);
        }

        let mut downstreams: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
        for (node, upstream) in spec.nodes.iter().zip(&upstreams) {
            for up in upstream {
                downstreams
                    .entry(up.as_str())
                    .or_default()
                    .insert(node.name.clone());
            }
        }

        let mut nodes = Vec::with_capacity(spec.nodes.len());
        let mut index = BTreeMap::new();
        for (i, node_spec) in spec.nodes.into_iter().enumerate() {
            let downstream = downstreams
                .remove(node_spec.name.as_str())
                .unwrap_or_default();
            index.insert(node_spec.name.clone(), i);
            nodes.push(Node {
                upstreams: upstreams[i].iter().cloned().collect(),
                downstreams: downstream.into_iter().collect(),
                spec: node_spec,
            });
        }

        Ok(Graph {
            name: spec.name,
            schedule,
            catchup: spec.catchup,
            partitioning: spec.partitioning,
            nodes,
            index,
        })
    }

    /// The graph name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The cron expression, absent for a graph run by target requests only.
    /// It displays as the text the parser normalises, so `0 9 * * mon-fri`
    /// displays as `0 9 * * 1-5`.
    pub fn schedule(&self) -> Option<&Expression> {
        self.schedule.as_ref()
    }

    /// The catch-up window of a newly published graph.
    pub fn catchup(&self) -> Option<Duration> {
        self.catchup
    }

    /// The partitioning of the graph's assets.
    pub fn partitioning(&self) -> Partitioning {
        self.partitioning
    }

    /// The nodes in definition order.
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// The node with `name`.
    pub fn node(&self, name: &str) -> Option<&Node> {
        self.index.get(name).map(|&i| &self.nodes[i])
    }

    /// The nodes without an upstream, in the order of [`Graph::nodes`].
    pub fn roots(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter(|n| n.upstreams.is_empty())
    }

    /// The nodes without a downstream, in the order of [`Graph::nodes`].
    pub fn leaves(&self) -> impl Iterator<Item = &Node> {
        self.nodes.iter().filter(|n| n.downstreams.is_empty())
    }

    /// The number of edges.
    pub fn edge_count(&self) -> usize {
        self.nodes.iter().map(|n| n.upstreams.len()).sum()
    }
}

/// Whether `text` matches `[a-z0-9_]+`.
pub fn is_name(text: &str) -> bool {
    !text.is_empty()
        && text
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn check_templates(
    node: &str,
    path: &str,
    value: &toml::Value,
    upstreams: &BTreeSet<String>,
    problems: &mut Vec<Problem>,
) {
    let child = |key: &str| {
        if path.is_empty() {
            key.to_string()
        } else {
            format!("{path}.{key}")
        }
    };
    match value {
        toml::Value::String(text) => match Template::parse(text) {
            Ok(template) => {
                let mut seen = BTreeSet::new();
                for upstream in template.upstream_nodes() {
                    if !upstreams.contains(upstream) && seen.insert(upstream) {
                        problems.push(Problem::UnknownTemplateUpstream {
                            node: node.to_string(),
                            param: path.to_string(),
                            upstream: upstream.to_string(),
                        });
                    }
                }
            }
            Err(e) => problems.push(Problem::InvalidTemplate {
                node: node.to_string(),
                param: path.to_string(),
                message: e.to_string(),
            }),
        },
        toml::Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                check_templates(node, &child(&i.to_string()), item, upstreams, problems);
            }
        }
        toml::Value::Table(table) => {
            for (key, item) in table {
                check_templates(node, &child(key), item, upstreams, problems);
            }
        }
        _ => {}
    }
}

/// The path of a cycle as node names, each an upstream of the one before it
/// and the first repeated at the end, or `None` when the edges are acyclic.
fn find_cycle(nodes: &[NodeSpec], upstreams: &[BTreeSet<String>]) -> Option<Vec<String>> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Mark {
        Unvisited,
        Active,
        Done,
    }

    fn visit(
        i: usize,
        nodes: &[NodeSpec],
        upstreams: &[BTreeSet<String>],
        index: &BTreeMap<&str, usize>,
        marks: &mut [Mark],
        path: &mut Vec<usize>,
    ) -> Option<Vec<String>> {
        marks[i] = Mark::Active;
        path.push(i);
        for up in &upstreams[i] {
            let j = index[up.as_str()];
            match marks[j] {
                Mark::Active => {
                    let start = path
                        .iter()
                        .position(|&p| p == j)
                        .expect("an active node is on the path");
                    let mut cycle: Vec<String> = path[start..]
                        .iter()
                        .map(|&p| nodes[p].name.clone())
                        .collect();
                    cycle.push(nodes[j].name.clone());
                    return Some(cycle);
                }
                Mark::Unvisited => {
                    if let Some(cycle) = visit(j, nodes, upstreams, index, marks, path) {
                        return Some(cycle);
                    }
                }
                Mark::Done => {}
            }
        }
        path.pop();
        marks[i] = Mark::Done;
        None
    }

    let index: BTreeMap<&str, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.name.as_str(), i))
        .collect();
    let mut marks = vec![Mark::Unvisited; nodes.len()];
    let mut path = Vec::new();
    for i in 0..nodes.len() {
        if marks[i] == Mark::Unvisited
            && let Some(cycle) = visit(i, nodes, upstreams, &index, &mut marks, &mut path)
        {
            return Some(cycle);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asset(name: &str, produces: &str, consumes: &[&str]) -> NodeSpec {
        NodeSpec {
            name: name.into(),
            kind: NodeKind::Asset {
                produces: produces.into(),
                consumes: consumes.iter().map(|s| s.to_string()).collect(),
            },
            operator: "subprocess".into(),
            pool: "default".into(),
            retries: 0,
            params: r#"argv = ["true"]"#.parse().unwrap(),
        }
    }

    fn task(name: &str, after: &[&str]) -> NodeSpec {
        NodeSpec {
            name: name.into(),
            kind: NodeKind::Task {
                after: after.iter().map(|s| s.to_string()).collect(),
                trigger_rule: TriggerRule::AllDone,
            },
            ..asset(name, "", &[])
        }
    }

    fn spec(nodes: Vec<NodeSpec>) -> GraphSpec {
        GraphSpec {
            name: "g".into(),
            schedule: Some("0 2 * * *".into()),
            catchup: None,
            partitioning: Partitioning::Daily,
            nodes,
        }
    }

    fn build(nodes: Vec<NodeSpec>) -> Result<Graph, Vec<Problem>> {
        Graph::build(spec(nodes), &OperatorSet::builtin())
    }

    #[test]
    fn edges_follow_from_assets_and_after_and_nodes_keep_definition_order() {
        let graph = build(vec![
            asset("load", "c", &["b"]),
            asset("transform", "b", &["a"]),
            task("notify", &["load", "transform"]),
            asset("extract", "a", &[]),
        ])
        .unwrap();
        let names: Vec<&str> = graph.nodes().iter().map(Node::name).collect();
        assert_eq!(names, ["load", "transform", "notify", "extract"]);
        assert_eq!(graph.node("load").unwrap().upstreams(), ["transform"]);
        assert_eq!(
            graph.node("transform").unwrap().downstreams(),
            ["load", "notify"]
        );
        assert_eq!(
            graph.node("notify").unwrap().upstreams(),
            ["load", "transform"]
        );
        assert_eq!(
            graph.roots().map(Node::name).collect::<Vec<_>>(),
            ["extract"]
        );
        assert_eq!(
            graph.leaves().map(Node::name).collect::<Vec<_>>(),
            ["notify"]
        );
        assert_eq!(graph.edge_count(), 4);
        assert_eq!(graph.node("load").unwrap().asset(), Some("c"));
        assert_eq!(graph.node("notify").unwrap().asset(), None);
        assert_eq!(
            graph.node("load").unwrap().trigger_rule(),
            TriggerRule::AllSucceeded
        );
        assert_eq!(
            graph.node("notify").unwrap().trigger_rule(),
            TriggerRule::AllDone
        );
    }

    #[test]
    fn every_fault_is_reported_at_once() {
        let mut bad = spec(vec![
            asset("Extract", "Raw", &["missing"]),
            asset("x", "dup", &[]),
            asset("x", "dup", &[]),
            NodeSpec {
                operator: "sql".into(),
                ..asset("y", "y", &[])
            },
            NodeSpec {
                params: "".parse().unwrap(),
                ..asset("z", "z", &[])
            },
            task("t", &["nowhere"]),
        ]);
        bad.name = "Bad-Graph".into();
        bad.schedule = Some("every day".into());
        bad.partitioning = Partitioning::Unpartitioned;
        let problems = Graph::build(bad, &OperatorSet::builtin()).unwrap_err();
        assert!(problems.contains(&Problem::InvalidGraphName("Bad-Graph".into())));
        assert!(
            matches!(&problems[1], Problem::InvalidSchedule { expression, .. } if expression == "every day")
        );
        assert!(problems.contains(&Problem::ScheduleWithoutPartition));
        assert!(problems.contains(&Problem::InvalidNodeName("Extract".into())));
        assert!(problems.contains(&Problem::InvalidAssetName {
            node: "Extract".into(),
            asset: "Raw".into(),
        }));
        assert!(problems.contains(&Problem::UnknownAsset {
            node: "Extract".into(),
            asset: "missing".into(),
        }));
        assert!(problems.contains(&Problem::DuplicateNode("x".into())));
        assert!(problems.contains(&Problem::DuplicateAsset {
            asset: "dup".into(),
            nodes: vec!["x".into(), "x".into()],
        }));
        assert!(problems.contains(&Problem::UnknownOperator {
            node: "y".into(),
            operator: "sql".into(),
        }));
        assert!(matches!(
            problems.iter().find(|p| matches!(p, Problem::InvalidParams { .. })),
            Some(Problem::InvalidParams { node, operator, .. }) if node == "z" && operator == "subprocess"
        ));
        assert!(problems.contains(&Problem::UnknownNode {
            node: "t".into(),
            after: "nowhere".into(),
        }));
    }

    #[test]
    fn empty_graph_is_rejected() {
        assert_eq!(build(vec![]), Err(vec![Problem::NoNodes]));
    }

    #[test]
    fn names_are_bounded_so_every_run_id_fits() {
        let long = "n".repeat(MAX_GRAPH_AND_NODE_NAME_LEN - 1);
        assert!(build(vec![asset(&long, "a", &[])]).is_ok());
        let too_long = "n".repeat(MAX_GRAPH_AND_NODE_NAME_LEN);
        assert_eq!(
            build(vec![asset(&too_long, "a", &[])]),
            Err(vec![Problem::NamesTooLong(too_long)])
        );
    }

    #[test]
    fn templates_are_checked_at_every_depth_against_declared_upstreams() {
        let mut load = asset("load", "c", &["a"]);
        load.params = r#"
            argv = ["{{ upstream.extract.rows }} {{ upstream.extract.rows }}"]
            [nested]
            list = ["ok {{ partition }}", "{{ upstream.other.x }}"]
            broken = "{{ partition"
        "#
        .parse()
        .unwrap();
        let problems = build(vec![
            asset("extract", "a", &[]),
            asset("other", "o", &[]),
            load,
        ])
        .unwrap_err();
        assert_eq!(
            problems,
            vec![
                Problem::InvalidParams {
                    node: "load".into(),
                    operator: "subprocess".into(),
                    message: problems
                        .iter()
                        .find_map(|p| match p {
                            Problem::InvalidParams { message, .. } => Some(message.clone()),
                            _ => None,
                        })
                        .unwrap(),
                },
                Problem::InvalidTemplate {
                    node: "load".into(),
                    param: "nested.broken".into(),
                    message: "`{{` at byte 0 is not closed".into(),
                },
                Problem::UnknownTemplateUpstream {
                    node: "load".into(),
                    param: "nested.list.1".into(),
                    upstream: "other".into(),
                },
            ]
        );
    }

    #[test]
    fn cycle_is_reported_as_its_path() {
        // `after_cycle` depends on the cycle and is not on it. The search
        // reaches the cycle from `a`, the first node in definition order that
        // is on it.
        let problems = build(vec![
            asset("root", "r", &[]),
            asset("a", "a", &["b", "r"]),
            asset("b", "b", &["a"]),
            asset("after_cycle", "d", &["b"]),
        ])
        .unwrap_err();
        assert_eq!(
            problems,
            vec![Problem::Cycle(vec!["a".into(), "b".into(), "a".into()])]
        );
        assert_eq!(
            problems[0].to_string(),
            "the graph has a cycle: a -> b -> a"
        );
    }

    #[test]
    fn is_name_accepts_lowercase_digits_and_underscore_only() {
        assert!(is_name("orders_daily_2"));
        for bad in ["", "Orders", "orders-daily", "orders daily", "ordérs"] {
            assert!(!is_name(bad), "{bad}");
        }
    }
}
