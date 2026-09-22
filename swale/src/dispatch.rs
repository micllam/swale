//! The step runner of every pool: it reads the task input from the payload
//! and the identity from the headers, renders the parameters and runs the
//! operator. An `{{ env.<NAME> }}` reference renders from the environment
//! of the process, read once when the runner is built.

use std::collections::BTreeMap;
use std::sync::Arc;

use taquba_workflow::{Step, StepError, StepOutcome, StepRunner};

use crate::input::TaskInput;
use crate::operator::{OperatorSet, Outcome, Task};
use crate::records::JsonBytes;
use crate::task::TaskIdentity;

/// The step runner over an [`OperatorSet`].
#[derive(Debug, Clone)]
pub struct Dispatch {
    operators: Arc<OperatorSet>,
    env: Arc<BTreeMap<String, String>>,
}

impl Dispatch {
    /// A runner over `operators` with the environment of the process as the
    /// variables of `{{ env.<NAME> }}`. A variable whose name or value is
    /// not UTF-8 is omitted.
    pub fn new(operators: Arc<OperatorSet>) -> Self {
        let env = std::env::vars_os()
            .filter_map(|(name, value)| Some((name.into_string().ok()?, value.into_string().ok()?)))
            .collect();
        Dispatch::with_env(operators, env)
    }

    /// A runner over `operators` with `env` as the variables of
    /// `{{ env.<NAME> }}`.
    pub fn with_env(operators: Arc<OperatorSet>, env: BTreeMap<String, String>) -> Self {
        Dispatch {
            operators,
            env: Arc::new(env),
        }
    }
}

impl StepRunner for Dispatch {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        let input = TaskInput::from_bytes(&step.payload)
            .map_err(|e| StepError::permanent(format!("the payload is not a task input: {e}")))?;
        let identity = TaskIdentity::from_headers(&step.headers).map_err(|e| {
            StepError::permanent(format!("the headers do not identify a task: {e}"))
        })?;
        // A reference to an absent output is a failure of the task. The hook
        // records it and the downstream rules see it.
        let params = match input.rendered_params(&identity, &self.env) {
            Ok(params) => params,
            Err(e) => {
                return Ok(StepOutcome::Fail {
                    reason: e.to_string(),
                });
            }
        };
        let task = Task {
            step,
            identity: &identity,
            params: &params,
            inputs: &input.inputs,
        };
        match self
            .operators
            .run(&input.operator, &task, params.clone())
            .await?
        {
            Outcome::Succeeded(value) => Ok(StepOutcome::Succeed {
                result: serde_json::to_vec(&value).expect("an output serializes to JSON"),
            }),
            Outcome::Failed(reason) => Ok(StepOutcome::Fail { reason }),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use taquba_workflow::StepErrorKind;

    use super::*;
    use crate::operator::Operator;
    use crate::partition::Partition;

    struct Echo;

    #[derive(Deserialize)]
    struct EchoParams {
        text: String,
    }

    impl Operator for Echo {
        type Params = EchoParams;

        async fn run(&self, _task: &Task<'_>, params: EchoParams) -> Result<Outcome, StepError> {
            if params.text == "fail" {
                return Ok(Outcome::Failed("asked to".into()));
            }
            Ok(Outcome::Succeeded(serde_json::json!({"text": params.text})))
        }
    }

    fn dispatch() -> Dispatch {
        let mut set = OperatorSet::new();
        set.add("echo", Echo);
        Dispatch::with_env(
            Arc::new(set),
            BTreeMap::from([("TOKEN".to_string(), "t0k".to_string())]),
        )
    }

    fn step(operator: &str, text: &str, inputs: BTreeMap<String, serde_json::Value>) -> Step {
        let input = TaskInput {
            operator: operator.into(),
            params: serde_json::json!({"text": text}),
            inputs,
            upstreams: BTreeMap::new(),
        };
        let mut step = Step::detached(input.to_bytes());
        step.delivery.headers = TaskIdentity {
            graph: "g".into(),
            partition: Partition::none(),
            node: "n".into(),
            asset: None,
            definition: "d".into(),
            rerun: 0,
        }
        .headers();
        step
    }

    #[tokio::test]
    async fn renders_params_then_maps_the_operator_outcome_to_the_step_outcome() {
        let inputs = BTreeMap::from([("up".to_string(), serde_json::json!({"rows": 3}))]);
        let outcome = dispatch()
            .run_step(&step("echo", "rows={{ upstream.up.rows }}", inputs))
            .await
            .unwrap();
        assert!(
            matches!(&outcome, StepOutcome::Succeed { result } if result == br#"{"text":"rows=3"}"#),
            "{outcome:?}"
        );
        let outcome = dispatch()
            .run_step(&step("echo", "fail", BTreeMap::new()))
            .await
            .unwrap();
        assert!(matches!(outcome, StepOutcome::Fail { reason } if reason == "asked to"));
    }

    #[tokio::test]
    async fn an_env_reference_renders_from_the_environment_of_the_runner() {
        let outcome = dispatch()
            .run_step(&step("echo", "Bearer {{ env.TOKEN }}", BTreeMap::new()))
            .await
            .unwrap();
        assert!(
            matches!(&outcome, StepOutcome::Succeed { result } if result == br#"{"text":"Bearer t0k"}"#),
            "{outcome:?}"
        );
        let outcome = dispatch()
            .run_step(&step("echo", "{{ env.OTHER }}", BTreeMap::new()))
            .await
            .unwrap();
        assert!(
            matches!(&outcome, StepOutcome::Fail { reason } if reason == "environment variable `OTHER` is not set"),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn an_absent_upstream_output_fails_the_task() {
        let outcome = dispatch()
            .run_step(&step("echo", "{{ upstream.up.rows }}", BTreeMap::new()))
            .await
            .unwrap();
        assert!(
            matches!(&outcome, StepOutcome::Fail { reason } if reason.contains("upstream `up`")),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn a_bad_payload_or_an_unknown_operator_is_a_permanent_error() {
        let err = dispatch()
            .run_step(&Step::detached(b"not json".to_vec()))
            .await
            .unwrap_err();
        assert_eq!(err.kind, StepErrorKind::Permanent);
        let err = dispatch()
            .run_step(&step("missing", "x", BTreeMap::new()))
            .await
            .unwrap_err();
        assert_eq!(err.kind, StepErrorKind::Permanent);
        let mut step = step("echo", "x", BTreeMap::new());
        step.delivery.headers.clear();
        let err = dispatch().run_step(&step).await.unwrap_err();
        assert_eq!(err.kind, StepErrorKind::Permanent);
    }
}
