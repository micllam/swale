//! The step runner of every pool: it reads the task input from the payload
//! and the identity from the headers, renders the parameters and runs the
//! operator. An `{{ env.<NAME> }}` reference renders from the environment
//! of the process, read once when the runner is built. An
//! [`Outcome::Continue`] enqueues the next step of the run after its delay,
//! with the input and the state as the payload.
//!
//! # Mapped nodes
//!
//! The task instance of a mapped node does not run the operator. Its first step
//! renders the items and submits one member per item as a run group of the
//! pool's runtime, with the id of [`crate::task::group_id`] and the position of
//! the item as the member's key. Each later step reads the status of the group
//! after a delay that doubles from the member poll interval up to the maximum
//! interval, so no worker is held while the members run. A member runs the
//! operator with its item, in the pool and with the attempts of its node.
//!
//! When every member has succeeded, the task instance removes the group and
//! succeeds. Its output is the array of the member outputs in item order. When
//! a member has failed or has been cancelled, the task instance fails once
//! every member is terminated, and the group remains. The task instance of a
//! rerun submits the same group, and the runtime then runs the members that did
//! not succeed. When the items or the input of a member differ from those of
//! the group, the task instance cancels the active members, removes the group
//! and submits every member. It does the same when the memo retention of the
//! pool has removed the run result record of a member that succeeded.
//!
//! The input of a member includes the upstream outputs that the parameters of
//! the node refer to, and the operator of a member receives those alone.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Weak};
use std::time::Duration;

use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use taquba::Clock;
use taquba_workflow::{
    GroupMember, MemberResult, RunGroup, RunOptions, Step, StepError, StepOutcome, StepRunner,
    TerminalStatus,
};

use crate::hook::output_value;
use crate::input::TaskInput;
use crate::operator::{OperatorSet, Outcome, Task};
use crate::pools::PoolRuntime;
use crate::records::JsonBytes;
use crate::task::{HEADER_GROUP, TaskIdentity};

/// The default of [`Dispatch::member_poll_interval`].
pub const DEFAULT_MEMBER_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// The default of [`Dispatch::member_poll_max_interval`].
pub const DEFAULT_MEMBER_POLL_MAX_INTERVAL: Duration = Duration::from_secs(30);

/// The reason of the cancel of the members of a group that the task instance
/// replaces.
const GROUP_REPLACED: &str = "the members of the node changed";

/// The reason of the cancel of the members of a cancelled task instance.
const TASK_INSTANCE_CANCELLED: &str = "the task instance is cancelled";

/// The step runner over an [`OperatorSet`].
#[derive(Clone)]
pub struct Dispatch {
    operators: Arc<OperatorSet>,
    clock: Arc<dyn Clock>,
    env: Arc<BTreeMap<String, String>>,
    member_poll_interval: Duration,
    member_poll_max_interval: Duration,
    pool: Option<Pool>,
}

/// The pool a runner runs in: the name of the pool and the runtimes of the
/// pools by name. Each runtime owns its runner.
#[derive(Clone)]
struct Pool {
    runtimes: Weak<HashMap<String, PoolRuntime>>,
    name: String,
}

/// The state of the task instance of a mapped node between two steps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
enum Phase {
    /// The step submits the members.
    Submit {
        /// The waits of the task instance for the cancelled members of an
        /// earlier group.
        polls: u32,
    },
    /// The step reads the status of the group.
    Poll {
        /// The reads of the status before the step.
        polls: u32,
    },
    /// The step removes the group, and the task instance succeeds.
    Finish {
        /// The output of the node: the member outputs in item order.
        output: Value,
    },
}

impl Dispatch {
    /// A runner over `operators` with `clock` as the clock of
    /// [`Task::now_ms`] and the environment of the process as the variables
    /// of `{{ env.<NAME> }}`. A variable whose name or value is not UTF-8 is
    /// omitted.
    pub fn new(operators: Arc<OperatorSet>, clock: Arc<dyn Clock>) -> Self {
        let env = std::env::vars_os()
            .filter_map(|(name, value)| Some((name.into_string().ok()?, value.into_string().ok()?)))
            .collect();
        Dispatch::with_env(operators, clock, env)
    }

    /// A runner over `operators` with `clock` as the clock of
    /// [`Task::now_ms`] and `env` as the variables of `{{ env.<NAME> }}`.
    pub fn with_env(
        operators: Arc<OperatorSet>,
        clock: Arc<dyn Clock>,
        env: BTreeMap<String, String>,
    ) -> Self {
        Dispatch {
            operators,
            clock,
            env: Arc::new(env),
            member_poll_interval: DEFAULT_MEMBER_POLL_INTERVAL,
            member_poll_max_interval: DEFAULT_MEMBER_POLL_MAX_INTERVAL,
            pool: None,
        }
    }

    /// The time before the first read of the status of the members of a mapped
    /// node, [`DEFAULT_MEMBER_POLL_INTERVAL`] by default. The time doubles at
    /// each read, up to [`Self::member_poll_max_interval`].
    pub fn member_poll_interval(mut self, interval: Duration) -> Self {
        self.member_poll_interval = interval;
        self
    }

    /// The longest time between two reads of the status of the members of a
    /// mapped node, [`DEFAULT_MEMBER_POLL_MAX_INTERVAL`] by default. When
    /// [`Self::member_poll_interval`] is longer, the runner waits that interval
    /// between every two reads.
    pub fn member_poll_max_interval(mut self, interval: Duration) -> Self {
        self.member_poll_max_interval = interval;
        self
    }

    /// The time before the read of the status that follows `polls` reads.
    fn member_poll_delay(&self, polls: u32) -> Duration {
        self.member_poll_interval
            .saturating_mul(2u32.saturating_pow(polls))
            .min(self.member_poll_max_interval.max(self.member_poll_interval))
    }

    /// The runner of the pool `name` among `runtimes`. A runner without a pool
    /// fails the task instance of a mapped node.
    pub(crate) fn in_pool(
        mut self,
        runtimes: Weak<HashMap<String, PoolRuntime>>,
        name: impl Into<String>,
    ) -> Self {
        self.pool = Some(Pool {
            runtimes,
            name: name.into(),
        });
        self
    }

    /// The run group of the members of the mapped node of `identity`, on the
    /// runtime of the runner's pool.
    fn group(&self, identity: &TaskIdentity) -> Result<RunGroup, StepError> {
        let runtime = self
            .pool
            .as_ref()
            .and_then(|pool| Some(pool.runtimes.upgrade()?.get(&pool.name)?.clone()))
            .ok_or_else(|| {
                StepError::permanent("a mapped node runs on the runtime of a pool".to_string())
            })?;
        Ok(runtime.group(identity.group_id()))
    }

    /// One step of the task instance of a mapped node.
    async fn run_mapped(
        &self,
        step: &Step,
        input: TaskInput,
        identity: &TaskIdentity,
    ) -> Result<StepOutcome, StepError> {
        let group = self.group(identity)?;
        let phase = match &input.state {
            None => Phase::Submit { polls: 0 },
            Some(state) => serde_json::from_value(state.clone()).map_err(|e| {
                StepError::permanent(format!("the state is not a phase of a mapped node: {e}"))
            })?,
        };
        let next = |phase: Phase, after: Duration| {
            let next = TaskInput {
                state: Some(serde_json::to_value(phase).expect("a phase serializes to JSON")),
                ..input.clone()
            };
            StepOutcome::continue_after(next.to_bytes(), after)
        };
        match phase {
            Phase::Submit { polls } => {
                let items = match input.rendered_items(identity, &self.env) {
                    Ok(items) => items,
                    Err(e) => {
                        return Ok(StepOutcome::Fail {
                            reason: e.to_string(),
                        });
                    }
                };
                let members: Vec<GroupMember> = items
                    .into_iter()
                    .enumerate()
                    .map(|(position, item)| GroupMember {
                        key: position.to_string(),
                        input: input.member(item).to_bytes(),
                    })
                    .collect();
                let delay = if members.is_empty() {
                    Duration::ZERO
                } else {
                    self.member_poll_delay(0)
                };
                let mut headers = identity.headers();
                headers.insert(HEADER_GROUP.to_string(), group.id().to_string());
                let options = RunOptions {
                    headers,
                    max_attempts_per_step: Some(step.max_attempts),
                    ..RunOptions::default()
                };
                match group.submit(members.clone(), &options).await {
                    Ok(()) => {}
                    Err(taquba_workflow::Error::GroupMismatch(_)) => {
                        // The group has the members of an earlier task instance
                        // of the node. A member of it that is active must
                        // terminate before the group is removed.
                        if group.status().await.map_err(step_error)?.pending > 0 {
                            let reason = Some(GROUP_REPLACED.to_string());
                            group.cancel(reason).await.map_err(step_error)?;
                            let polls = polls + 1;
                            return Ok(next(
                                Phase::Submit { polls },
                                self.member_poll_delay(polls),
                            ));
                        }
                        group.forget().await.map_err(step_error)?;
                        group.submit(members, &options).await.map_err(step_error)?;
                    }
                    Err(e) => return Err(step_error(e)),
                }
                if step.cancel_token.is_cancelled() {
                    let reason = Some(TASK_INSTANCE_CANCELLED.to_string());
                    group.cancel(reason).await.map_err(step_error)?;
                }
                Ok(next(Phase::Poll { polls: 0 }, delay))
            }
            Phase::Poll { polls } => {
                if step.cancel_token.is_cancelled() {
                    let reason = Some(TASK_INSTANCE_CANCELLED.to_string());
                    group.cancel(reason).await.map_err(step_error)?;
                }
                let status = match group.status().await {
                    Ok(status) => status,
                    Err(taquba_workflow::Error::GroupNotFound(_)) => {
                        return Ok(next(Phase::Submit { polls: 0 }, Duration::ZERO));
                    }
                    Err(e) => return Err(step_error(e)),
                };
                if status.succeeded + status.failed + status.cancelled < status.total {
                    let polls = polls + 1;
                    return Ok(next(Phase::Poll { polls }, self.member_poll_delay(polls)));
                }
                let results: Vec<MemberResult> = group
                    .results()
                    .await
                    .map_err(step_error)?
                    .try_collect()
                    .await
                    .map_err(step_error)?;
                Ok(match member_outputs(results) {
                    Ok(output) => next(Phase::Finish { output }, Duration::ZERO),
                    Err(NoOutputs::Failed(reason)) => StepOutcome::Fail { reason },
                    Err(NoOutputs::ResultRemoved) => {
                        // Every member is terminated, so the group does not
                        // have an active member.
                        group.forget().await.map_err(step_error)?;
                        next(Phase::Submit { polls: 0 }, Duration::ZERO)
                    }
                })
            }
            Phase::Finish { output } => {
                group.forget().await.map_err(step_error)?;
                Ok(StepOutcome::Succeed {
                    result: serde_json::to_vec(&output).expect("an output serializes to JSON"),
                })
            }
        }
    }
}

/// The reason that the members of a group do not give the output of the node.
#[derive(Debug, Clone, PartialEq, Eq)]
enum NoOutputs {
    /// A member did not succeed: the count of such members and the error of the
    /// first in item order.
    Failed(String),
    /// Every member succeeded, and the memo retention of the pool removed the
    /// run result record of a member.
    ResultRemoved,
}

/// The array of the outputs of `results` in item order.
fn member_outputs(results: Vec<MemberResult>) -> Result<Value, NoOutputs> {
    let total = results.len();
    let mut members: Vec<(usize, MemberResult)> = results
        .into_iter()
        .map(|result| (result.key.parse().unwrap_or(usize::MAX), result))
        .collect();
    members.sort_by_key(|(position, _)| *position);
    let failed: Vec<&MemberResult> = members
        .iter()
        .map(|(_, result)| result)
        .filter(|result| result.termination.status != TerminalStatus::Succeeded)
        .collect();
    if let Some(first) = failed.first() {
        let error = match (&first.termination.error, first.termination.status) {
            (Some(error), _) => error.as_str(),
            (None, TerminalStatus::Cancelled) => "cancelled",
            (None, _) => "failed",
        };
        return Err(NoOutputs::Failed(format!(
            "{} of {total} members did not succeed, and member {} is the first: {error}",
            failed.len(),
            first.key,
        )));
    }
    members
        .into_iter()
        .map(|(_, result)| {
            let outcome = result.outcome.ok_or(NoOutputs::ResultRemoved)?;
            Ok(outcome
                .result
                .map_or(Value::Null, |bytes| output_value(&bytes)))
        })
        .collect::<Result<_, _>>()
        .map(Value::Array)
}

/// The step error of a runtime error: permanent when the error is, and
/// transient otherwise.
fn step_error(error: taquba_workflow::Error) -> StepError {
    if error.is_permanent() {
        StepError::permanent(error.to_string())
    } else {
        StepError::transient(error.to_string())
    }
}

impl std::fmt::Debug for Dispatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dispatch")
            .field("operators", &self.operators)
            .finish_non_exhaustive()
    }
}

impl StepRunner for Dispatch {
    async fn run_step(&self, step: &Step) -> Result<StepOutcome, StepError> {
        let input = TaskInput::from_bytes(&step.payload)
            .map_err(|e| StepError::permanent(format!("the payload is not a task input: {e}")))?;
        let identity = TaskIdentity::from_headers(&step.headers).map_err(|e| {
            StepError::permanent(format!("the headers do not identify a task: {e}"))
        })?;
        if input.each.is_some() {
            return self.run_mapped(step, input, &identity).await;
        }
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
            state: input.state.as_ref(),
            now_ms: self.clock.now_ms(),
        };
        let outcome = self
            .operators
            .run(&input.operator, &task, params.clone())
            .await?;
        Ok(match outcome {
            Outcome::Succeeded(value) => StepOutcome::Succeed {
                result: serde_json::to_vec(&value).expect("an output serializes to JSON"),
            },
            Outcome::Failed(reason) => StepOutcome::Fail { reason },
            Outcome::Continue { state, after } => {
                let next = TaskInput {
                    state: Some(state),
                    ..input
                };
                StepOutcome::continue_after(next.to_bytes(), after)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde::Deserialize;
    use taquba::MockClock;
    use taquba_workflow::{StepErrorKind, Trigger};

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

        async fn run(&self, task: &Task<'_>, params: EchoParams) -> Result<Outcome, StepError> {
            if params.text == "fail" {
                return Ok(Outcome::Failed("asked to".into()));
            }
            if params.text == "wait" {
                return Ok(match task.state {
                    None => Outcome::Continue {
                        state: serde_json::json!({"polled_at_ms": task.now_ms}),
                        after: Duration::from_secs(5),
                    },
                    Some(state) => Outcome::Succeeded(state.clone()),
                });
            }
            Ok(Outcome::Succeeded(serde_json::json!({"text": params.text})))
        }
    }

    fn dispatch() -> Dispatch {
        let mut set = OperatorSet::new();
        set.add("echo", Echo);
        Dispatch::with_env(
            Arc::new(set),
            Arc::new(MockClock::new(1_000)),
            BTreeMap::from([("TOKEN".to_string(), "t0k".to_string())]),
        )
    }

    fn step(operator: &str, text: &str, inputs: BTreeMap<String, serde_json::Value>) -> Step {
        let input = TaskInput {
            operator: operator.into(),
            params: serde_json::json!({"text": text}),
            inputs,
            upstreams: BTreeMap::new(),
            state: None,
            each: None,
            item: None,
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
    async fn a_continue_outcome_enqueues_the_next_step_with_the_state_after_the_delay() {
        let outcome = dispatch()
            .run_step(&step("echo", "wait", BTreeMap::new()))
            .await
            .unwrap();
        let StepOutcome::Continue { payload, when } = outcome else {
            panic!("{outcome:?}");
        };
        assert!(matches!(when, Trigger::After(delay) if delay == Duration::from_secs(5)));
        let next = TaskInput::from_bytes(&payload).unwrap();
        assert_eq!(next.state, Some(serde_json::json!({"polled_at_ms": 1_000})));
        // The next step sees the state.
        let mut second = step("echo", "wait", BTreeMap::new());
        second.step_number = 1;
        second.payload = payload;
        let outcome = dispatch().run_step(&second).await.unwrap();
        assert!(
            matches!(&outcome, StepOutcome::Succeed { result } if result == br#"{"polled_at_ms":1000}"#),
            "{outcome:?}"
        );
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

    #[test]
    fn the_member_poll_delay_doubles_up_to_the_maximum_interval() {
        let delays: Vec<u64> = (0..7)
            .map(|polls| dispatch().member_poll_delay(polls).as_secs())
            .collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 30, 30]);
        assert_eq!(
            dispatch().member_poll_delay(u32::MAX),
            Duration::from_secs(30)
        );
        let slow = dispatch().member_poll_interval(Duration::from_secs(60));
        assert_eq!(slow.member_poll_delay(0), Duration::from_secs(60));
        assert_eq!(slow.member_poll_delay(3), Duration::from_secs(60));
    }
}
