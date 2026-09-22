//! The operators a definition can name: the check of their parameters at load
//! time and their execution at run time.
//!
//! An [`OperatorSet`] maps an operator name to a parameter check and, for an
//! operator the process runs, to an [`Operator`]. [`OperatorSet::builtin`]
//! contains the operators of this crate, which the submodules [`subprocess`],
//! [`shell`], [`http`] and [`object_exists`] implement, and a consumer adds
//! its own with [`OperatorSet::add`] or, for a check alone,
//! [`OperatorSet::register`].
//!
//! An operator that waits on an external condition ends each poll with
//! [`Outcome::Continue`]: the run continues after a delay with the state of
//! the poll, and no worker is held during the wait.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::Value;
use taquba_workflow::{Step, StepError};

use crate::task::TaskIdentity;

pub mod http;
pub mod object_exists;
pub mod shell;
pub mod subprocess;

use http::Http;
use object_exists::ObjectExists;
use shell::Shell;
use subprocess::Subprocess;

/// One task instance as an operator sees it.
#[derive(Debug, Clone, Copy)]
pub struct Task<'a> {
    /// The step of the run, with the lease, the cancellation token and the
    /// attempt count.
    pub step: &'a Step,
    /// The identity of the task instance.
    pub identity: &'a TaskIdentity,
    /// The node's parameters with every template rendered.
    pub params: &'a Value,
    /// The output of each upstream node with a succeeded record.
    pub inputs: &'a BTreeMap<String, Value>,
    /// The state of the previous step of the run, from its
    /// [`Outcome::Continue`]. `None` on the first step.
    pub state: Option<&'a Value>,
    /// The time the step started, in milliseconds from the Unix epoch, by
    /// the clock of the store.
    pub now_ms: u64,
}

/// The outcome of an operator. An infrastructure failure is a [`StepError`]:
/// transient for a retry, permanent for a dead-letter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The task instance succeeded with this output.
    Succeeded(Value),
    /// The task instance failed with this reason.
    Failed(String),
    /// The step ended, and the run continues with a next step after `after`,
    /// with `state` as [`Task::state`] of that step.
    Continue {
        /// The state of the next step.
        state: Value,
        /// The delay before the next step.
        after: Duration,
    },
}

/// An operator the process runs.
pub trait Operator: Send + Sync + 'static {
    /// The parameter type. A node's `params` table must deserialize into it
    /// at load time, and the rendered parameters do so at run time.
    type Params: DeserializeOwned + Send;

    /// Runs one task instance. The implementation must be idempotent per
    /// attempt, because delivery is at least once.
    fn run(
        &self,
        task: &Task<'_>,
        params: Self::Params,
    ) -> impl Future<Output = Result<Outcome, StepError>> + Send;
}

type Check = Box<dyn Fn(&toml::Table) -> Result<(), String> + Send + Sync>;
type BoxFuture<'a> = Pin<Box<dyn Future<Output = Result<Outcome, StepError>> + Send + 'a>>;
type Run = Box<dyn for<'a> Fn(&'a Task<'a>, Value) -> BoxFuture<'a> + Send + Sync>;

struct Entry {
    check: Check,
    run: Option<Run>,
}

/// The registered operators.
#[derive(Default)]
pub struct OperatorSet {
    entries: BTreeMap<String, Entry>,
}

/// The reason a node's operator or parameters are rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorError {
    /// The operator is not registered.
    Unknown,
    /// The parameters do not deserialize into the operator's type. The
    /// string is the deserializer's message.
    InvalidParams(String),
}

impl OperatorSet {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// The operators of this crate: `subprocess`, `shell`, `http` and
    /// `object_exists`.
    pub fn builtin() -> Self {
        let mut set = Self::new();
        set.add("subprocess", Subprocess::default());
        set.add("shell", Shell::default());
        set.add("http", Http::default());
        set.add("object_exists", ObjectExists::default());
        set
    }

    /// Registers `name` with `P` as its parameter type and without an
    /// operator to run. A second registration of `name` replaces the first.
    pub fn register<P: DeserializeOwned + 'static>(&mut self, name: &str) {
        self.entries.insert(
            name.to_string(),
            Entry {
                check: check_fn::<P>(),
                run: None,
            },
        );
    }

    /// Adds `operator` as `name`, with its parameter type as the check. A
    /// second entry for `name` replaces the first.
    pub fn add<O: Operator>(&mut self, name: &str, operator: O) {
        let operator = Arc::new(operator);
        let run: Run = Box::new(move |task, params| {
            let operator = operator.clone();
            Box::pin(async move {
                let params: O::Params = serde_json::from_value(params).map_err(|e| {
                    StepError::permanent(format!("the rendered parameters are invalid: {e}"))
                })?;
                operator.run(task, params).await
            })
        });
        self.entries.insert(
            name.to_string(),
            Entry {
                check: check_fn::<O::Params>(),
                run: Some(run),
            },
        );
    }

    /// Whether `name` is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    /// Whether `name` has an operator to run.
    pub fn runs(&self, name: &str) -> bool {
        self.entries.get(name).is_some_and(|e| e.run.is_some())
    }

    /// The registered names in lexical order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    /// Checks a node's parameters against the operator `name`.
    pub fn check(&self, name: &str, params: &toml::Table) -> Result<(), OperatorError> {
        let entry = self.entries.get(name).ok_or(OperatorError::Unknown)?;
        (entry.check)(params).map_err(OperatorError::InvalidParams)
    }

    /// Runs `task` with the operator `name` and the rendered `params`. An
    /// operator that is unknown or checked only is a permanent error.
    pub async fn run(
        &self,
        name: &str,
        task: &Task<'_>,
        params: Value,
    ) -> Result<Outcome, StepError> {
        let run = self
            .entries
            .get(name)
            .and_then(|e| e.run.as_ref())
            .ok_or_else(|| {
                StepError::permanent(format!("operator `{name}` cannot run in this process"))
            })?;
        run(task, params).await
    }
}

/// The lease extension of an operator that waits on an external process
/// or call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    /// The time the lease is extended to at each extension.
    pub extension: Duration,
    /// The time between extensions.
    pub interval: Duration,
}

impl Default for Lease {
    /// An extension to 60 seconds every 20 seconds.
    fn default() -> Self {
        Lease {
            extension: Duration::from_secs(60),
            interval: Duration::from_secs(20),
        }
    }
}

/// Extends the lease of `step` by `lease` while the operator waits, and
/// returns the error of an extension that failed.
pub(crate) async fn keep_lease(step: &Step, lease: Lease) -> StepError {
    let mut ticks = tokio::time::interval(lease.interval);
    ticks.tick().await;
    loop {
        ticks.tick().await;
        if let Err(e) = step.lease.ensure_at_least(lease.extension) {
            return StepError::transient(format!("lease extension failed: {e}"));
        }
    }
}

/// The last 512 bytes of `bytes` as text, for an error message.
pub(crate) fn tail(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let start = text.len().saturating_sub(512);
    text[text.floor_char_boundary(start)..].to_string()
}

fn check_fn<P: DeserializeOwned + 'static>() -> Check {
    Box::new(|params: &toml::Table| {
        P::deserialize(params.clone())
            .map(|_| ())
            .map_err(|e| e.to_string())
    })
}

impl fmt::Debug for OperatorSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.entries.keys()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::Partition;

    fn table(text: &str) -> toml::Table {
        text.parse().unwrap()
    }

    #[test]
    fn builtin_set_runs_the_four_operators() {
        let set = OperatorSet::builtin();
        assert_eq!(
            set.names().collect::<Vec<_>>(),
            ["http", "object_exists", "shell", "subprocess"]
        );
        for name in ["http", "object_exists", "shell", "subprocess"] {
            assert!(set.runs(name), "{name}");
        }
        assert_eq!(
            set.check("object_exists", &table(r#"url = "s3://landing/k""#)),
            Ok(())
        );
        assert_eq!(
            set.check("subprocess", &table(r#"argv = ["python", "x.py"]"#)),
            Ok(())
        );
        assert_eq!(set.check("shell", &table(r#"command = "true""#)), Ok(()));
        assert_eq!(
            set.check("http", &table(r#"url = "https://example.test/""#)),
            Ok(())
        );
    }

    #[test]
    fn unknown_operator_is_reported_as_unknown() {
        assert_eq!(
            OperatorSet::builtin().check("sql", &table("")),
            Err(OperatorError::Unknown)
        );
    }

    #[test]
    fn params_that_do_not_deserialize_are_invalid() {
        let set = OperatorSet::builtin();
        for params in ["argv = []", "argv = [\"a\"]\nextra = 1", ""] {
            assert!(
                matches!(
                    set.check("subprocess", &table(params)),
                    Err(OperatorError::InvalidParams(_))
                ),
                "{params}"
            );
        }
    }

    #[test]
    fn register_adds_a_check_only_operator() {
        #[derive(serde::Deserialize)]
        struct Params {
            #[allow(dead_code)]
            query: String,
        }
        let mut set = OperatorSet::new();
        assert!(!set.contains("sql"));
        set.register::<Params>("sql");
        assert!(set.contains("sql"));
        assert!(!set.runs("sql"));
        assert_eq!(set.check("sql", &table(r#"query = "select 1""#)), Ok(()));
    }

    struct Echo;

    #[derive(serde::Deserialize)]
    struct EchoParams {
        text: String,
    }

    impl Operator for Echo {
        type Params = EchoParams;

        async fn run(&self, task: &Task<'_>, params: EchoParams) -> Result<Outcome, StepError> {
            Ok(Outcome::Succeeded(serde_json::json!({
                "text": params.text,
                "node": task.identity.node,
            })))
        }
    }

    fn task<'a>(
        step: &'a Step,
        identity: &'a TaskIdentity,
        params: &'a Value,
        inputs: &'a BTreeMap<String, Value>,
    ) -> Task<'a> {
        Task {
            step,
            identity,
            params,
            inputs,
            state: None,
            now_ms: 0,
        }
    }

    #[tokio::test]
    async fn add_registers_a_check_and_runs_the_operator_with_typed_params() {
        let mut set = OperatorSet::new();
        set.add("echo", Echo);
        assert_eq!(set.check("echo", &table(r#"text = "hi""#)), Ok(()));
        assert!(matches!(
            set.check("echo", &table("")),
            Err(OperatorError::InvalidParams(_))
        ));

        let step = Step::detached(Vec::new());
        let identity = TaskIdentity {
            graph: "g".into(),
            partition: Partition::none(),
            node: "n".into(),
            asset: None,
            definition: "d".into(),
            rerun: 0,
        };
        let params = serde_json::json!({"text": "hi"});
        let inputs = BTreeMap::new();
        let task = task(&step, &identity, &params, &inputs);
        assert_eq!(
            set.run("echo", &task, params.clone()).await.unwrap(),
            Outcome::Succeeded(serde_json::json!({"text": "hi", "node": "n"}))
        );
        let err = set
            .run("echo", &task, serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(err.kind, taquba_workflow::StepErrorKind::Permanent);
        let err = set.run("missing", &task, params.clone()).await.unwrap_err();
        assert_eq!(err.kind, taquba_workflow::StepErrorKind::Permanent);
    }
}
