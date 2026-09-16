//! The operators a definition can name: the check of their parameters at load
//! time and their execution at run time.
//!
//! An [`OperatorSet`] maps an operator name to a parameter check and, for an
//! operator the process runs, to an [`Operator`]. [`OperatorSet::builtin`]
//! contains the operators of this crate, and a consumer adds its own with
//! [`OperatorSet::add`] or, for a check alone, [`OperatorSet::register`].

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use taquba_workflow::{Step, StepError};

use crate::subprocess::Subprocess;
use crate::task::TaskIdentity;

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
}

/// The outcome of an operator. An infrastructure failure is a [`StepError`]
/// instead: transient for a retry, permanent for a dead-letter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The task instance succeeded with this output.
    Succeeded(Value),
    /// The task instance failed with this reason.
    Failed(String),
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

    /// The operators of this crate: `subprocess`.
    pub fn builtin() -> Self {
        let mut set = Self::new();
        set.add("subprocess", Subprocess::default());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::Partition;

    fn table(text: &str) -> toml::Table {
        text.parse().unwrap()
    }

    #[test]
    fn builtin_set_runs_the_subprocess_operator() {
        let set = OperatorSet::builtin();
        assert_eq!(set.names().collect::<Vec<_>>(), ["subprocess"]);
        assert!(set.runs("subprocess"));
        assert_eq!(
            set.check("subprocess", &table(r#"argv = ["python", "x.py"]"#)),
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
        #[derive(Deserialize)]
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

    #[derive(Deserialize)]
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
