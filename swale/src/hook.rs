//! The terminal hook of every pool: it writes the node's record and enqueues
//! the event for the scheduler, and both commit with the notification's
//! acknowledgement.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use taquba::{Clock, EnqueueOptions, EnqueueRequest, MAX_KV_VALUE_SIZE};
use taquba_workflow::{RunOutcome, StepError, TerminalEffects, TerminalHook, TerminalStatus};

use crate::partition::Partition;
use crate::records::{JsonBytes, NodeRecord, RecordStatus};
use crate::task::TaskIdentity;

/// The queue of the scheduler's events.
pub const EVENTS_QUEUE: &str = "swale-events";

/// The event the hook enqueues for a terminated task instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// The graph name.
    pub graph: String,
    /// The partition.
    pub partition: Partition,
    /// The node name.
    pub node: String,
    /// The terminal status.
    pub status: RecordStatus,
}

impl JsonBytes for Event {}

/// The terminal hook.
#[derive(Clone)]
pub struct RecordHook {
    clock: Arc<dyn Clock>,
}

impl RecordHook {
    /// A hook that dates records with `clock` and enqueues events on
    /// [`EVENTS_QUEUE`].
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        RecordHook { clock }
    }

    /// The record for `outcome`. The output is included when the record stays
    /// within the KV value cap, and left out with `output_omitted` set
    /// otherwise.
    pub fn record(&self, identity: &TaskIdentity, outcome: &RunOutcome) -> NodeRecord {
        let output = match (outcome.status, &outcome.result) {
            (TerminalStatus::Succeeded, Some(bytes)) => {
                Some(serde_json::from_slice(bytes).unwrap_or_else(|_| {
                    serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned())
                }))
            }
            _ => None,
        };
        let mut record = NodeRecord {
            status: outcome.status.into(),
            run_id: outcome.run_id.to_string(),
            definition: identity.definition.clone(),
            rerun: identity.rerun,
            terminated_at_ms: self.clock.now_ms(),
            output,
            output_omitted: false,
            error: outcome.error.clone(),
        };
        if record.output.is_some() && record.to_bytes().len() > MAX_KV_VALUE_SIZE {
            record.output = None;
            record.output_omitted = true;
        }
        record
    }
}

impl TerminalHook for RecordHook {
    async fn on_termination(
        &self,
        outcome: &RunOutcome,
        effects: &TerminalEffects,
    ) -> Result<(), StepError> {
        let identity = TaskIdentity::from_headers(&outcome.headers).map_err(|e| {
            StepError::permanent(format!("the headers do not identify a task: {e}"))
        })?;
        let record = self.record(&identity, outcome);
        effects
            .put(identity.record_key(), record.to_bytes())
            .map_err(|e| StepError::permanent(e.to_string()))?;
        let event = Event {
            graph: identity.graph,
            partition: identity.partition,
            node: identity.node,
            status: record.status,
        };
        effects
            .enqueue(EnqueueRequest {
                queue: EVENTS_QUEUE.to_string(),
                payload: event.to_bytes(),
                options: EnqueueOptions::default().dedup_key(format!("evt:{}", outcome.run_id)),
            })
            .map_err(|e| StepError::permanent(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use taquba::MockClock;
    use taquba_workflow::RunId;

    use super::*;

    fn identity() -> TaskIdentity {
        TaskIdentity {
            graph: "g".into(),
            partition: Partition::new("20260915").unwrap(),
            node: "extract".into(),
            asset: Some("raw".into()),
            definition: "d".into(),
            rerun: 1,
        }
    }

    fn hook() -> RecordHook {
        RecordHook::new(Arc::new(MockClock::new(42)))
    }

    fn outcome(status: TerminalStatus, result: Option<&[u8]>, error: Option<&str>) -> RunOutcome {
        RunOutcome {
            run_id: RunId::new("g-20260915-extract-r1").unwrap(),
            status,
            result: result.map(<[u8]>::to_vec),
            error: error.map(str::to_string),
            headers: identity().headers(),
            final_step: 0,
        }
    }

    #[test]
    fn record_copies_the_json_output_and_dates_with_the_clock() {
        let record = hook().record(
            &identity(),
            &outcome(TerminalStatus::Succeeded, Some(br#"{"rows":3}"#), None),
        );
        assert_eq!(record.status, RecordStatus::Succeeded);
        assert_eq!(record.run_id, "g-20260915-extract-r1");
        assert_eq!(record.rerun, 1);
        assert_eq!(record.terminated_at_ms, 42);
        assert_eq!(record.output, Some(serde_json::json!({"rows": 3})));
        assert!(!record.output_omitted);
    }

    #[test]
    fn a_non_json_output_is_kept_as_a_string_and_a_failure_keeps_the_error() {
        let record = hook().record(
            &identity(),
            &outcome(TerminalStatus::Succeeded, Some(b"plain"), None),
        );
        assert_eq!(
            record.output,
            Some(serde_json::Value::String("plain".into()))
        );
        let record = hook().record(
            &identity(),
            &outcome(TerminalStatus::Failed, None, Some("boom")),
        );
        assert_eq!(record.status, RecordStatus::Failed);
        assert_eq!(record.output, None);
        assert_eq!(record.error.as_deref(), Some("boom"));
    }

    #[test]
    fn an_output_beyond_the_kv_cap_is_left_out_of_the_record() {
        let big = format!("\"{}\"", "x".repeat(MAX_KV_VALUE_SIZE));
        let record = hook().record(
            &identity(),
            &outcome(TerminalStatus::Succeeded, Some(big.as_bytes()), None),
        );
        assert_eq!(record.output, None);
        assert!(record.output_omitted);
        assert!(record.to_bytes().len() < MAX_KV_VALUE_SIZE);
    }

    #[test]
    fn event_round_trips() {
        let event = Event {
            graph: "g".into(),
            partition: Partition::none(),
            node: "n".into(),
            status: RecordStatus::Cancelled,
        };
        assert_eq!(Event::from_bytes(&event.to_bytes()).unwrap(), event);
    }
}
