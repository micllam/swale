//! The `subprocess` operator: runs a program with a JSON document on stdin
//! and reads its output JSON from stdout.
//!
//! The stdin document is `{run_id, graph, partition, node, attempt, params,
//! inputs}`, where `inputs` maps each upstream node name to its output. Exit
//! code 0 is success and stdout is the output (an empty stdout is `null`).
//! Exit code 75 (`EX_TEMPFAIL`) is a transient error, and any other code, a
//! signal or a program that cannot start is a permanent error. Stderr is
//! logged, and its tail is included in an error message. The program must be
//! idempotent per attempt.
//!
//! The `shell` operator ([`super::shell`]) runs a command line with the same
//! exit code protocol.

use std::collections::BTreeMap;
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use taquba_workflow::StepError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

use super::{Operator, Outcome, Task, keep_lease};

/// Exit code for a transient failure.
pub const EX_TEMPFAIL: i32 = 75;

/// Parameters of the `subprocess` operator: a program and its arguments.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubprocessParams {
    /// The program followed by its arguments. It must not be empty.
    #[serde(deserialize_with = "non_empty_argv")]
    pub argv: Vec<String>,
}

fn non_empty_argv<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<String>, D::Error> {
    let argv = Vec::<String>::deserialize(deserializer)?;
    if argv.is_empty() {
        return Err(serde::de::Error::custom("argv must not be empty"));
    }
    Ok(argv)
}

/// The `subprocess` operator.
#[derive(Debug, Clone)]
pub struct Subprocess {
    /// The time the lease is extended to at each extension.
    pub lease_extension: Duration,
    /// The time between lease extensions while the program runs.
    pub lease_interval: Duration,
}

impl Default for Subprocess {
    fn default() -> Self {
        Subprocess {
            lease_extension: Duration::from_secs(60),
            lease_interval: Duration::from_secs(20),
        }
    }
}

#[derive(Serialize)]
struct StdinDocument<'a> {
    run_id: &'a str,
    graph: &'a str,
    partition: &'a str,
    node: &'a str,
    attempt: u32,
    params: &'a Value,
    inputs: &'a BTreeMap<String, Value>,
}

impl Operator for Subprocess {
    type Params = SubprocessParams;

    async fn run(&self, task: &Task<'_>, params: SubprocessParams) -> Result<Outcome, StepError> {
        let document = serde_json::to_vec(&StdinDocument {
            run_id: task.identity.run_id().as_str(),
            graph: &task.identity.graph,
            partition: task.identity.partition.as_str(),
            node: &task.identity.node,
            attempt: task.step.attempts,
            params: task.params,
            inputs: task.inputs,
        })
        .expect("the stdin document serializes to JSON");
        let mut command = Command::new(&params.argv[0]);
        command.args(&params.argv[1..]);
        run_program(
            task,
            command,
            Some(document),
            self.lease_extension,
            self.lease_interval,
        )
        .await
    }
}

/// Runs `command` to its end with the exit code protocol. `stdin` is written
/// to the program and closed, or stdin is closed at the start. The lease is
/// extended while the program runs, and a cancellation of the run kills the
/// program.
pub(crate) async fn run_program(
    task: &Task<'_>,
    mut command: Command,
    stdin: Option<Vec<u8>>,
    lease_extension: Duration,
    lease_interval: Duration,
) -> Result<Outcome, StepError> {
    let run_id = task.identity.run_id();
    let program = command
        .as_std()
        .get_program()
        .to_string_lossy()
        .into_owned();
    let mut child = command
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| StepError::permanent(format!("cannot start `{program}`: {e}")))?;

    let mut stdout = child.stdout.take().expect("stdout is piped");
    let mut stderr = child.stderr.take().expect("stderr is piped");
    let output = tokio::spawn(async move {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let _ = stdout.read_to_end(&mut out).await;
        let _ = stderr.read_to_end(&mut err).await;
        (out, err)
    });
    if let Some(document) = stdin {
        let mut pipe = child.stdin.take().expect("stdin is piped");
        // A program that ignores stdin closes the pipe. The write error is
        // then expected and is dropped.
        let _ = pipe.write_all(&document).await;
    }

    let status = tokio::select! {
        status = child.wait() => status
            .map_err(|e| StepError::permanent(format!("cannot wait for `{program}`: {e}")))?,
        e = keep_lease(task.step, lease_extension, lease_interval) => return Err(e),
        () = task.step.cancel_token.cancelled() => {
            let _ = child.kill().await;
            return Err(StepError::transient("the run was cancelled while the program ran"));
        }
    };
    let (out, err) = output
        .await
        .map_err(|e| StepError::permanent(format!("cannot read the output of `{program}`: {e}")))?;
    if !err.is_empty() {
        tracing::info!(run_id = %run_id, program, stderr = %String::from_utf8_lossy(&err), "program stderr");
    }
    let tail = || {
        let text = String::from_utf8_lossy(&err);
        let start = text.len().saturating_sub(512);
        text[text.floor_char_boundary(start)..].to_string()
    };
    match status.code() {
        Some(0) => {
            let trimmed = out.trim_ascii();
            if trimmed.is_empty() {
                return Ok(Outcome::Succeeded(Value::Null));
            }
            serde_json::from_slice(trimmed)
                .map(Outcome::Succeeded)
                .map_err(|e| {
                    StepError::permanent(format!("stdout of `{program}` is not JSON: {e}"))
                })
        }
        Some(EX_TEMPFAIL) => Err(StepError::transient(format!(
            "`{program}` exited with {EX_TEMPFAIL}: {}",
            tail()
        ))),
        Some(code) => Err(StepError::permanent(format!(
            "`{program}` exited with {code}: {}",
            tail()
        ))),
        None => Err(StepError::permanent(format!(
            "`{program}` was terminated by a signal: {}",
            tail()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::Partition;
    use crate::task::TaskIdentity;
    use taquba_workflow::{Step, StepErrorKind};

    fn identity() -> TaskIdentity {
        TaskIdentity {
            graph: "g".into(),
            partition: Partition::new("20260915").unwrap(),
            node: "n".into(),
            asset: None,
            definition: "d".into(),
            rerun: 0,
        }
    }

    async fn run(script: &str) -> Result<Outcome, StepError> {
        let step = Step::detached(Vec::new());
        let identity = identity();
        let params = serde_json::json!({"argv": ["sh", "-c", script], "extra": 1});
        let inputs = BTreeMap::from([("up".to_string(), serde_json::json!({"rows": 3}))]);
        let task = Task {
            step: &step,
            identity: &identity,
            params: &params,
            inputs: &inputs,
        };
        let argv = SubprocessParams {
            argv: vec!["sh".into(), "-c".into(), script.into()],
        };
        Subprocess::default().run(&task, argv).await
    }

    #[tokio::test]
    async fn stdin_document_is_available_and_stdout_is_the_output() {
        // The script copies the stdin document into its output.
        let outcome = run("printf '{\"seen\": '; cat; printf '}'").await.unwrap();
        let Outcome::Succeeded(value) = outcome else {
            panic!("{outcome:?}");
        };
        let seen = &value["seen"];
        assert_eq!(seen["run_id"], "g-20260915-n-r0");
        assert_eq!(seen["graph"], "g");
        assert_eq!(seen["partition"], "20260915");
        assert_eq!(seen["node"], "n");
        assert_eq!(seen["params"]["extra"], 1);
        assert_eq!(seen["inputs"]["up"]["rows"], 3);
    }

    #[tokio::test]
    async fn empty_stdout_is_null_and_non_json_stdout_is_permanent() {
        assert_eq!(
            run("cat >/dev/null").await.unwrap(),
            Outcome::Succeeded(Value::Null)
        );
        let err = run("cat >/dev/null; echo not-json").await.unwrap_err();
        assert_eq!(err.kind, StepErrorKind::Permanent);
    }

    #[tokio::test]
    async fn exit_codes_map_to_transient_and_permanent_errors_with_the_stderr_tail() {
        let err = run("cat >/dev/null; echo busy >&2; exit 75")
            .await
            .unwrap_err();
        assert_eq!(err.kind, StepErrorKind::Transient);
        assert!(err.message.contains("busy"), "{}", err.message);
        let err = run("cat >/dev/null; echo broken >&2; exit 3")
            .await
            .unwrap_err();
        assert_eq!(err.kind, StepErrorKind::Permanent);
        assert!(
            err.message.contains("exited with 3: broken"),
            "{}",
            err.message
        );
    }

    #[tokio::test]
    async fn a_program_that_cannot_start_is_permanent() {
        let step = Step::detached(Vec::new());
        let identity = identity();
        let params = Value::Null;
        let inputs = BTreeMap::new();
        let task = Task {
            step: &step,
            identity: &identity,
            params: &params,
            inputs: &inputs,
        };
        let err = Subprocess::default()
            .run(
                &task,
                SubprocessParams {
                    argv: vec!["/nonexistent/program".into()],
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind, StepErrorKind::Permanent);
    }
}
