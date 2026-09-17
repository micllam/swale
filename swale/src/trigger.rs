//! The trigger: the request to start graph runs of a graph, as the payload of
//! a job on the triggers queue.
//!
//! The cron schedule of a graph enqueues a trigger without a partition, and
//! the job's `cron.previous_fire_ms` header, the start of the schedule
//! interval that the firing ends, determines the partition. A target request
//! lists its partitions.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use taquba::{JobRecord, LeaseHandle, PermanentFailure, Worker, WorkerError};
use taquba_cron::PREVIOUS_FIRE_MS_HEADER;

use crate::partition::Partition;
use crate::scheduler::{Error, Scheduler};

/// The queue of the triggers.
pub const TRIGGERS_QUEUE: &str = "swale-triggers";

/// The request to start graph runs of `graph`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trigger {
    /// The graph name.
    pub graph: String,
    /// The partitions to run. Empty for a cron firing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<Partition>,
}

impl Trigger {
    /// The trigger of a cron firing of `graph`.
    pub fn firing(graph: impl Into<String>) -> Self {
        Trigger {
            graph: graph.into(),
            partitions: Vec::new(),
        }
    }

    /// The JSON form of the trigger.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a trigger serializes to JSON")
    }

    /// Parses the JSON form of the trigger.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// The [`Worker`] of the triggers queue.
pub struct TriggerWorker {
    scheduler: Arc<Scheduler>,
}

impl TriggerWorker {
    /// A worker that starts graph runs on `scheduler`.
    pub fn new(scheduler: Arc<Scheduler>) -> Self {
        TriggerWorker { scheduler }
    }
}

impl Worker for TriggerWorker {
    async fn process(&self, job: &JobRecord, _lease: &LeaseHandle) -> Result<(), WorkerError> {
        let trigger = Trigger::from_bytes(&job.payload)
            .map_err(|e| PermanentFailure::new(format!("the payload is not a trigger: {e}")))?;
        let interval_start_ms = job
            .headers
            .get(PREVIOUS_FIRE_MS_HEADER)
            .and_then(|value| value.parse().ok());
        match self
            .scheduler
            .handle_trigger(&trigger, interval_start_ms)
            .await
        {
            Ok(_) => Ok(()),
            Err(e @ (Error::UnknownGraph(_) | Error::NoPartition(_))) => {
                Err(PermanentFailure::new(e.to_string()).into())
            }
            Err(e) => Err(Box::new(e) as WorkerError),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_json_form_leaves_out_an_empty_partition_list() {
        let firing = Trigger::firing("orders");
        assert_eq!(firing.to_bytes(), br#"{"graph":"orders"}"#);
        assert_eq!(Trigger::from_bytes(&firing.to_bytes()).unwrap(), firing);

        let request = Trigger {
            graph: "orders".into(),
            partitions: vec![Partition::new("20260915").unwrap()],
        };
        assert_eq!(
            request.to_bytes(),
            br#"{"graph":"orders","partitions":["20260915"]}"#
        );
        assert_eq!(Trigger::from_bytes(&request.to_bytes()).unwrap(), request);
    }
}
