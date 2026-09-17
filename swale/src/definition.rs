//! The definition file: one TOML document per graph.
//!
//! ```toml
//! [graph]
//! name = "orders_daily"        # [a-z0-9_]+
//! schedule = "0 2 * * *"       # optional, five-field cron in UTC
//! catchup = "7d"               # optional, the catch-up and backfill window
//! partition = "daily"          # daily, hourly or none (the default)
//!
//! [[node]]
//! name = "extract"             # [a-z0-9_]+
//! produces = "orders_raw"      # makes the node an asset node
//! consumes = []                # asset node only
//! after = []                   # task node only
//! trigger_rule = "all_succeeded"  # task node only, or all_done or one_failed
//! operator = "subprocess"
//! pool = "default"
//! retries = 0
//! [node.params]                # the operator's parameters
//! argv = ["python", "tasks/extract.py"]
//! ```
//!
//! A graph with a `schedule` declares `partition` as `daily` or `hourly`,
//! because every firing runs a partition of its own (see [`crate::daemon`]).
//! The `schedule` is a [`taquba_cron::Expression`]: a step follows a range or
//! `*`, as in `5-59/5 * * * *`.

use std::path::Path;

use serde::Deserialize;

use crate::duration;
use crate::error::Error;
use crate::graph::{Graph, GraphSpec, NodeKind, NodeSpec, Partitioning, Problem, TriggerRule};
use crate::operator::OperatorSet;

/// The content hash of a definition text, as 64 lowercase hex characters of
/// SHA-256. A graph run records the hash of the definition it started from,
/// and every node of the run uses that definition.
pub fn hash(text: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(text.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// Loads a graph from the definition at `path`.
pub fn load_path(path: &Path, operators: &OperatorSet) -> Result<Graph, Error> {
    let text = std::fs::read_to_string(path)?;
    load_str(&text, operators)
}

/// Loads a graph from the text of a definition.
pub fn load_str(text: &str, operators: &OperatorSet) -> Result<Graph, Error> {
    let file: File = toml::from_str(text)?;
    let mut problems = Vec::new();

    let catchup = match &file.graph.catchup {
        Some(text) => match duration::parse(text) {
            Ok(d) => Some(d),
            Err(e) => {
                problems.push(Problem::InvalidCatchup(e.to_string()));
                None
            }
        },
        None => None,
    };

    let nodes = file
        .nodes
        .into_iter()
        .map(|node| {
            let kind = match node.produces {
                Some(produces) => {
                    if !node.after.is_empty() {
                        problems.push(Problem::AssetNodeAfter(node.name.clone()));
                    }
                    if node.trigger_rule.is_some() {
                        problems.push(Problem::AssetNodeTriggerRule(node.name.clone()));
                    }
                    NodeKind::Asset {
                        produces,
                        consumes: node.consumes,
                    }
                }
                None => {
                    if !node.consumes.is_empty() {
                        problems.push(Problem::TaskNodeConsumes(node.name.clone()));
                    }
                    NodeKind::Task {
                        after: node.after,
                        trigger_rule: node.trigger_rule.unwrap_or_default().into(),
                    }
                }
            };
            NodeSpec {
                name: node.name,
                kind,
                operator: node.operator,
                pool: node.pool,
                retries: node.retries,
                params: node.params,
            }
        })
        .collect();

    if !problems.is_empty() {
        return Err(Error::Invalid(problems));
    }

    let spec = GraphSpec {
        name: file.graph.name,
        schedule: file.graph.schedule,
        catchup,
        partitioning: file.graph.partition.into(),
        nodes,
    };
    Graph::build(spec, operators).map_err(Error::Invalid)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    graph: GraphSection,
    #[serde(default, rename = "node")]
    nodes: Vec<NodeSection>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphSection {
    name: String,
    schedule: Option<String>,
    catchup: Option<String>,
    #[serde(default)]
    partition: PartitionField,
}

#[derive(Deserialize, Default, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum PartitionField {
    Daily,
    Hourly,
    #[default]
    None,
}

impl From<PartitionField> for Partitioning {
    fn from(field: PartitionField) -> Self {
        match field {
            PartitionField::Daily => Partitioning::Daily,
            PartitionField::Hourly => Partitioning::Hourly,
            PartitionField::None => Partitioning::Unpartitioned,
        }
    }
}

#[derive(Deserialize, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum TriggerRuleField {
    #[default]
    AllSucceeded,
    AllDone,
    OneFailed,
}

impl From<TriggerRuleField> for TriggerRule {
    fn from(field: TriggerRuleField) -> Self {
        match field {
            TriggerRuleField::AllSucceeded => TriggerRule::AllSucceeded,
            TriggerRuleField::AllDone => TriggerRule::AllDone,
            TriggerRuleField::OneFailed => TriggerRule::OneFailed,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSection {
    name: String,
    produces: Option<String>,
    #[serde(default)]
    consumes: Vec<String>,
    #[serde(default)]
    after: Vec<String>,
    trigger_rule: Option<TriggerRuleField>,
    operator: String,
    #[serde(default = "default_pool")]
    pool: String,
    #[serde(default)]
    retries: u32,
    #[serde(default)]
    params: toml::Table,
}

fn default_pool() -> String {
    "default".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
        [graph]
        name = "g"
        [[node]]
        name = "a"
        produces = "a"
        operator = "subprocess"
        [node.params]
        argv = ["true"]
    "#;

    fn load(text: &str) -> Result<Graph, Error> {
        load_str(text, &OperatorSet::builtin())
    }

    fn problems(text: &str) -> Vec<Problem> {
        match load(text) {
            Err(Error::Invalid(problems)) => problems,
            other => panic!("expected problems, got {other:?}"),
        }
    }

    #[test]
    fn defaults_apply_to_omitted_fields() {
        let graph = load(MINIMAL).unwrap();
        assert_eq!(graph.schedule(), None);
        assert_eq!(graph.catchup(), None);
        assert_eq!(graph.partitioning(), Partitioning::Unpartitioned);
        let node = graph.node("a").unwrap();
        assert_eq!(node.pool(), "default");
        assert_eq!(node.retries(), 0);
        assert_eq!(
            node.kind(),
            &NodeKind::Asset {
                produces: "a".into(),
                consumes: vec![],
            }
        );
    }

    #[test]
    fn graph_fields_are_parsed() {
        let text = MINIMAL.replace(
            "name = \"g\"",
            "name = \"g\"\nschedule = \"0 2 * * *\"\ncatchup = \"7d\"\npartition = \"hourly\"",
        );
        let graph = load(&text).unwrap();
        assert_eq!(graph.schedule().unwrap().to_string(), "0 2 * * *");
        assert_eq!(
            graph.catchup(),
            Some(std::time::Duration::from_secs(7 * 86_400))
        );
        assert_eq!(graph.partitioning(), Partitioning::Hourly);
    }

    #[test]
    fn task_node_fields_are_parsed() {
        let text = format!(
            "{MINIMAL}\n[[node]]\nname = \"t\"\nafter = [\"a\"]\ntrigger_rule = \"one_failed\"\noperator = \"subprocess\"\npool = \"p\"\nretries = 2\n[node.params]\nargv = [\"x\"]"
        );
        let graph = load(&text).unwrap();
        let node = graph.node("t").unwrap();
        assert_eq!(
            node.kind(),
            &NodeKind::Task {
                after: vec!["a".into()],
                trigger_rule: TriggerRule::OneFailed,
            }
        );
        assert_eq!(node.pool(), "p");
        assert_eq!(node.retries(), 2);
    }

    #[test]
    fn asset_and_task_fields_do_not_mix() {
        let asset_with_task_fields = MINIMAL.replace(
            "operator = \"subprocess\"",
            "operator = \"subprocess\"\nafter = [\"a\"]\ntrigger_rule = \"all_done\"",
        );
        let text = format!(
            "{asset_with_task_fields}\n[[node]]\nname = \"t\"\nconsumes = [\"a\"]\noperator = \"subprocess\"\n[node.params]\nargv = [\"x\"]"
        );
        assert_eq!(
            problems(&text),
            vec![
                Problem::AssetNodeAfter("a".into()),
                Problem::AssetNodeTriggerRule("a".into()),
                Problem::TaskNodeConsumes("t".into()),
            ]
        );
    }

    #[test]
    fn invalid_catchup_is_a_problem() {
        let text = MINIMAL.replace("name = \"g\"", "name = \"g\"\ncatchup = \"7w\"");
        assert_eq!(
            problems(&text),
            vec![Problem::InvalidCatchup(
                "`7w` is not a duration such as `30s`, `5m`, `6h` or `7d`".into()
            )]
        );
    }

    #[test]
    fn unknown_field_and_bad_toml_are_parse_errors() {
        for text in [
            &MINIMAL.replace(
                "operator = \"subprocess\"",
                "operator = \"subprocess\"\nretry = 1",
            ),
            &MINIMAL.replace("name = \"g\"", "name = \"g\"\nunknown = 1"),
            "[graph\n",
        ] {
            assert!(matches!(load(text), Err(Error::Parse(_))), "{text}");
        }
    }

    #[test]
    fn hash_is_the_sha256_of_the_text() {
        assert_eq!(
            hash("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn missing_file_is_an_io_error() {
        let result = load_path(
            Path::new("/nonexistent/graph.toml"),
            &OperatorSet::builtin(),
        );
        assert!(matches!(result, Err(Error::Io(_))));
    }
}
