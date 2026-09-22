//! The `shell` operator: runs a command line with `sh -c` and reads its
//! output JSON from stdout.
//!
//! The environment of the command contains the identity of the task
//! instance (`SWALE_RUN_ID`, `SWALE_GRAPH`, `SWALE_PARTITION`, `SWALE_NODE`
//! and `SWALE_ATTEMPT`), the outputs of the upstream nodes as JSON in
//! `SWALE_INPUTS`, and the `env` parameters. Stdin is closed. The exit code
//! protocol is that of the `subprocess` operator ([`super::subprocess`]):
//! 0 is success with stdout as the output, 75 is a transient error and any
//! other code is a permanent error.

use std::collections::BTreeMap;

use serde::Deserialize;
use taquba_workflow::StepError;
use tokio::process::Command;

use super::subprocess::run_program;
use super::{Lease, Operator, Outcome, Task};

/// Parameters of the `shell` operator: a command line and its environment.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShellParams {
    /// The command line, run by `sh -c`.
    pub command: String,
    /// Environment variables set for the command.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

/// The `shell` operator.
#[derive(Debug, Clone)]
pub struct Shell {
    /// The shell program, `sh` from the search path by default.
    pub shell: String,
    /// The lease extension while the command runs.
    pub lease: Lease,
}

impl Default for Shell {
    fn default() -> Self {
        Shell {
            shell: "sh".to_string(),
            lease: Lease::default(),
        }
    }
}

impl Operator for Shell {
    type Params = ShellParams;

    async fn run(&self, task: &Task<'_>, params: ShellParams) -> Result<Outcome, StepError> {
        let inputs = serde_json::to_string(task.inputs).expect("the inputs serialize to JSON");
        let mut command = Command::new(&self.shell);
        command
            .arg("-c")
            .arg(&params.command)
            .env("SWALE_RUN_ID", task.identity.run_id().as_str())
            .env("SWALE_GRAPH", &task.identity.graph)
            .env("SWALE_PARTITION", task.identity.partition.as_str())
            .env("SWALE_NODE", &task.identity.node)
            .env("SWALE_ATTEMPT", task.step.attempts.to_string())
            .env("SWALE_INPUTS", inputs)
            .envs(&params.env);
        run_program(task, command, None, self.lease).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::Partition;
    use crate::task::TaskIdentity;
    use serde_json::Value;
    use taquba_workflow::{Step, StepErrorKind};

    async fn run(command: &str, env: &[(&str, &str)]) -> Result<Outcome, StepError> {
        let step = Step::detached(Vec::new());
        let identity = TaskIdentity {
            graph: "g".into(),
            partition: Partition::new("20260915").unwrap(),
            node: "n".into(),
            asset: None,
            definition: "d".into(),
            rerun: 1,
        };
        let params = Value::Null;
        let inputs = BTreeMap::from([("up".to_string(), serde_json::json!({"rows": 3}))]);
        let task = Task {
            step: &step,
            identity: &identity,
            params: &params,
            inputs: &inputs,
            state: None,
            now_ms: 0,
        };
        Shell::default()
            .run(
                &task,
                ShellParams {
                    command: command.into(),
                    env: env
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                },
            )
            .await
    }

    #[tokio::test]
    async fn the_identity_the_inputs_and_the_env_params_are_in_the_environment() {
        let outcome = run(
            r#"printf '{"id": "%s", "graph": "%s", "partition": "%s", "node": "%s", "attempt": %s, "inputs": %s, "extra": "%s"}' "$SWALE_RUN_ID" "$SWALE_GRAPH" "$SWALE_PARTITION" "$SWALE_NODE" "$SWALE_ATTEMPT" "$SWALE_INPUTS" "$EXTRA""#,
            &[("EXTRA", "x")],
        )
        .await
        .unwrap();
        let Outcome::Succeeded(value) = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(value["id"], "g-20260915-n-r1");
        assert_eq!(value["graph"], "g");
        assert_eq!(value["partition"], "20260915");
        assert_eq!(value["node"], "n");
        assert_eq!(value["attempt"], 1);
        assert_eq!(value["inputs"]["up"]["rows"], 3);
        assert_eq!(value["extra"], "x");
    }

    #[tokio::test]
    async fn stdin_is_closed_and_the_exit_code_protocol_applies() {
        // A read of stdin ends at once.
        assert_eq!(
            run("cat; printf '{\"n\": 1}'", &[]).await.unwrap(),
            Outcome::Succeeded(serde_json::json!({"n": 1}))
        );
        let err = run("echo busy >&2; exit 75", &[]).await.unwrap_err();
        assert_eq!(err.kind, StepErrorKind::Transient);
        assert!(err.message.contains("busy"), "{}", err.message);
        let err = run("exit 3", &[]).await.unwrap_err();
        assert_eq!(err.kind, StepErrorKind::Permanent);
        assert!(
            err.message.contains("`sh` exited with 3"),
            "{}",
            err.message
        );
    }
}
