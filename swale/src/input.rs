//! The input of a task instance: the operator, the node's parameters before
//! rendering and the records of its upstreams. The input is the run's payload,
//! so a definition edit that changes a node's parameters while its run is
//! active fails the resubmission with an input mismatch. The payload of a later
//! step of the run is the input with the state of the previous step.
//!
//! The input of a mapped node includes the template of its items, and the input
//! of a member adds its item to the input of the node.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::graph::Node;
use crate::records::{JsonBytes, NodeRecord, RecordStatus};
use crate::task::TaskIdentity;
use crate::template::{RenderContext, RenderError, RunContext, Template};

/// The payload of a task instance's run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInput {
    /// The operator name.
    pub operator: String,
    /// The node's parameters, with template references unrendered.
    pub params: Value,
    /// The output of each upstream node with a succeeded record.
    pub inputs: BTreeMap<String, Value>,
    /// The status of each upstream node with a record.
    pub upstreams: BTreeMap<String, UpstreamSummary>,
    /// The state of the previous step of the run. `None` on the first step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<Value>,
    /// The template of the items of a mapped node. `None` for a node that runs
    /// once and for a member.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub each: Option<String>,
    /// The item of a member of a mapped node. `None` for every other run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item: Option<Value>,
}

/// The `each` template of a mapped node does not give the items.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ItemsError {
    /// A reference of the template cannot be resolved.
    #[error("`each`: {0}")]
    Render(#[from] RenderError),
    /// The rendered text is not a JSON array. The string is the parser's
    /// message, or the type of the value.
    #[error("`each` does not render to a JSON array: {0}")]
    NotAnArray(String),
}

/// The terminal status of an upstream node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamSummary {
    /// The status.
    pub status: RecordStatus,
    /// The error of a failed or cancelled run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl JsonBytes for TaskInput {}

impl TaskInput {
    /// The input of `node` given the records of its upstreams.
    pub fn new(node: &Node, upstream_records: &BTreeMap<String, NodeRecord>) -> Self {
        let params = serde_json::to_value(node.params()).expect("a TOML table serializes to JSON");
        let mut inputs = BTreeMap::new();
        let mut upstreams = BTreeMap::new();
        for (name, record) in upstream_records {
            if record.status == RecordStatus::Succeeded {
                inputs.insert(name.clone(), record.output.clone().unwrap_or(Value::Null));
            }
            upstreams.insert(
                name.clone(),
                UpstreamSummary {
                    status: record.status,
                    error: record.error.clone(),
                },
            );
        }
        TaskInput {
            operator: node.operator().to_string(),
            params,
            inputs,
            upstreams,
            state: None,
            each: node.each().map(str::to_string),
            item: None,
        }
    }

    /// The input of the member of a mapped node with `item`: the input of the
    /// node with the item, and without the template of the items and the state.
    /// Of the upstream outputs, it includes those that the parameters refer to.
    pub fn member(&self, item: Value) -> Self {
        let mut referred = BTreeSet::new();
        referred_upstreams(&self.params, &mut referred);
        TaskInput {
            inputs: self
                .inputs
                .iter()
                .filter(|(name, _)| referred.contains(*name))
                .map(|(name, output)| (name.clone(), output.clone()))
                .collect(),
            state: None,
            each: None,
            item: Some(item),
            ..self.clone()
        }
    }

    /// The summary of the graph run for `{{ run.summary }}`: the graph, the
    /// partition, the node and the status of each upstream, as JSON text.
    pub fn run_summary(&self, identity: &TaskIdentity) -> String {
        serde_json::json!({
            "graph": identity.graph,
            "partition": identity.partition,
            "node": identity.node,
            "upstreams": self.upstreams,
        })
        .to_string()
    }

    /// The parameters with every template reference rendered, with `env` as the
    /// environment variables of `{{ env.<NAME> }}`. In a member of a mapped
    /// node, `{{ run.id }}` is the run id of the task instance of the node.
    pub fn rendered_params(
        &self,
        identity: &TaskIdentity,
        env: &BTreeMap<String, String>,
    ) -> Result<Value, RenderError> {
        let run_id = identity.run_id();
        let summary = self.run_summary(identity);
        let ctx = RenderContext {
            partition: identity.partition.as_str(),
            run: RunContext {
                id: run_id.as_str(),
                summary: &summary,
            },
            upstream: &self.inputs,
            env,
            item: self.item.as_ref(),
        };
        render_value(&self.params, &ctx)
    }

    /// The items of a mapped node: the array that the template of `each`
    /// renders to, with `env` as in [`Self::rendered_params`]. An input without
    /// the template does not have an item.
    pub fn rendered_items(
        &self,
        identity: &TaskIdentity,
        env: &BTreeMap<String, String>,
    ) -> Result<Vec<Value>, ItemsError> {
        let Some(each) = &self.each else {
            return Ok(Vec::new());
        };
        let run_id = identity.run_id();
        let summary = self.run_summary(identity);
        let ctx = RenderContext {
            partition: identity.partition.as_str(),
            run: RunContext {
                id: run_id.as_str(),
                summary: &summary,
            },
            upstream: &self.inputs,
            env,
            item: None,
        };
        let text = match Template::parse(each) {
            Ok(template) => template.render(&ctx)?,
            Err(_) => each.clone(),
        };
        match serde_json::from_str(&text) {
            Ok(Value::Array(items)) => Ok(items),
            Ok(other) => Err(ItemsError::NotAnArray(
                match other {
                    Value::Null => "the value is null",
                    Value::Bool(_) => "the value is a boolean",
                    Value::Number(_) => "the value is a number",
                    Value::String(_) => "the value is a string",
                    Value::Array(_) | Value::Object(_) => "the value is an object",
                }
                .to_string(),
            )),
            Err(e) => Err(ItemsError::NotAnArray(e.to_string())),
        }
    }
}

/// Adds the upstream nodes that the templates of `value` refer to.
fn referred_upstreams(value: &Value, nodes: &mut BTreeSet<String>) {
    match value {
        Value::String(text) => {
            if let Ok(template) = Template::parse(text) {
                nodes.extend(template.upstream_nodes().map(str::to_string));
            }
        }
        Value::Array(items) => items
            .iter()
            .for_each(|item| referred_upstreams(item, nodes)),
        Value::Object(map) => map
            .values()
            .for_each(|item| referred_upstreams(item, nodes)),
        _ => {}
    }
}

/// Renders every string in a JSON value. The templates were checked at load
/// time, so a string that does not parse is copied unchanged.
fn render_value(value: &Value, ctx: &RenderContext<'_>) -> Result<Value, RenderError> {
    Ok(match value {
        Value::String(text) => match Template::parse(text) {
            Ok(template) => Value::String(template.render(ctx)?),
            Err(_) => value.clone(),
        },
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| render_value(item, ctx))
                .collect::<Result<_, _>>()?,
        ),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| render_value(v, ctx).map(|v| (k.clone(), v)))
                .collect::<Result<_, _>>()?,
        ),
        other => other.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{NodeKind, NodeSpec};
    use crate::partition::Partition;

    fn node() -> Node {
        let spec = crate::graph::GraphSpec {
            name: "g".into(),
            schedule: None,
            catchup: None,
            partitioning: Default::default(),
            nodes: vec![
                NodeSpec {
                    name: "extract".into(),
                    kind: NodeKind::Asset {
                        produces: "raw".into(),
                        consumes: vec![],
                    },
                    operator: "shell".into(),
                    pool: "default".into(),
                    retries: 0,
                    params: "command = \"true\"".parse().unwrap(),
                    each: None,
                },
                NodeSpec {
                    name: "load".into(),
                    kind: NodeKind::Asset {
                        produces: "clean".into(),
                        consumes: vec!["raw".into()],
                    },
                    operator: "subprocess".into(),
                    pool: "default".into(),
                    retries: 0,
                    params: r#"
                        argv = ["sh", "-c", "echo {{ upstream.extract.rows }}", "{{ partition }}"]
                        [nested]
                        id = "{{ run.id }}"
                        n = 1
                        token = "{{ env.TOKEN }}"
                    "#
                    .parse()
                    .unwrap(),
                    each: None,
                },
            ],
        };
        // The test parameters include a nested table, which the built-in
        // operator types reject, so the set checks the table form alone.
        let mut operators = crate::operator::OperatorSet::new();
        operators.register::<toml::Table>("shell");
        operators.register::<toml::Table>("subprocess");
        let graph = crate::graph::Graph::build(spec, &operators).unwrap();
        graph.node("load").unwrap().clone()
    }

    fn record(status: RecordStatus, output: Option<Value>) -> NodeRecord {
        NodeRecord {
            status,
            run_id: "g-20260915-extract-r0".into(),
            definition: "abc".into(),
            rerun: 0,
            terminated_at_ms: 1,
            output,
            output_omitted: false,
            error: (status != RecordStatus::Succeeded).then(|| "boom".to_string()),
        }
    }

    fn identity() -> TaskIdentity {
        TaskIdentity {
            graph: "g".into(),
            partition: Partition::new("20260915").unwrap(),
            node: "load".into(),
            asset: Some("clean".into()),
            definition: "abc".into(),
            rerun: 0,
        }
    }

    #[test]
    fn input_includes_outputs_of_succeeded_upstreams_and_the_status_of_every_upstream() {
        let records = BTreeMap::from([
            (
                "extract".to_string(),
                record(
                    RecordStatus::Succeeded,
                    Some(serde_json::json!({"rows": 3})),
                ),
            ),
            ("other".to_string(), record(RecordStatus::Failed, None)),
        ]);
        let input = TaskInput::new(&node(), &records);
        assert_eq!(input.operator, "subprocess");
        assert_eq!(input.inputs.keys().collect::<Vec<_>>(), ["extract"]);
        assert_eq!(input.upstreams["other"].error.as_deref(), Some("boom"));
        assert_eq!(TaskInput::from_bytes(&input.to_bytes()).unwrap(), input);
    }

    #[test]
    fn rendered_params_substitute_at_every_depth_and_keep_other_values() {
        let records = BTreeMap::from([(
            "extract".to_string(),
            record(
                RecordStatus::Succeeded,
                Some(serde_json::json!({"rows": 3})),
            ),
        )]);
        let input = TaskInput::new(&node(), &records);
        let env = BTreeMap::from([("TOKEN".to_string(), "t0k".to_string())]);
        let params = input.rendered_params(&identity(), &env).unwrap();
        assert_eq!(
            params,
            serde_json::json!({
                "argv": ["sh", "-c", "echo 3", "20260915"],
                "nested": {"id": "g-20260915-load-r0", "n": 1, "token": "t0k"},
            })
        );
        assert_eq!(
            input.run_summary(&identity()),
            r#"{"graph":"g","node":"load","partition":"20260915","upstreams":{"extract":{"status":"succeeded"}}}"#
        );
    }

    #[test]
    fn rendering_fails_when_an_upstream_output_is_absent() {
        let input = TaskInput::new(&node(), &BTreeMap::new());
        assert_eq!(
            input.rendered_params(&identity(), &BTreeMap::new()),
            Err(RenderError::MissingUpstream {
                node: "extract".into()
            })
        );
    }

    #[test]
    fn items_render_from_each_and_a_member_renders_its_item() {
        let identity = identity();
        let env = BTreeMap::new();
        let mut input = TaskInput {
            operator: "shell".into(),
            params: serde_json::json!({
                "command": "fetch {{ item.url }} {{ run.id }}",
                "env": {"TOKEN": "{{ upstream.login.token }}"},
            }),
            inputs: BTreeMap::from([
                (
                    "extract".to_string(),
                    serde_json::json!({"pages": [{"url": "a"}, {"url": "b"}], "rows": 3}),
                ),
                ("login".to_string(), serde_json::json!({"token": "t0k"})),
            ]),
            upstreams: BTreeMap::new(),
            state: Some(serde_json::json!({"phase": "poll"})),
            each: Some("{{ upstream.extract.pages }}".into()),
            item: None,
        };
        let items = input.rendered_items(&identity, &env).unwrap();
        assert_eq!(
            items,
            [
                serde_json::json!({"url": "a"}),
                serde_json::json!({"url": "b"})
            ]
        );

        let member = input.member(items[1].clone());
        assert_eq!((&member.each, &member.state), (&None, &None));
        // The parameters refer to `login` alone.
        assert_eq!(member.inputs.keys().collect::<Vec<_>>(), ["login"]);
        assert_eq!(TaskInput::from_bytes(&member.to_bytes()).unwrap(), member);
        assert_eq!(
            member.rendered_params(&identity, &env).unwrap(),
            serde_json::json!({
                "command": "fetch b g-20260915-load-r0",
                "env": {"TOKEN": "t0k"},
            })
        );

        input.each = Some("{{ upstream.extract.rows }}".into());
        assert_eq!(
            input.rendered_items(&identity, &env),
            Err(ItemsError::NotAnArray("the value is a number".into()))
        );
        input.each = Some("{{ upstream.other }}".into());
        assert_eq!(
            input
                .rendered_items(&identity, &env)
                .unwrap_err()
                .to_string(),
            "`each`: no output is recorded for upstream `other`"
        );
    }
}
