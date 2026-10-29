//! The request: an operation on a running daemon, sent by a command through the
//! inbox of the queue and applied by the scheduler.
//!
//! A command sends a [`Request`] with [`Request::send`] and never opens the
//! queue. The writer of the queue enqueues it as a job on [`REQUESTS_QUEUE`]
//! with the request id as the job id. The [`RequestWorker`] of the scheduler
//! applies the jobs one at a time in send order, and records the outcome as the
//! [`RequestRecord`] at the KV key `swale/requests/{id}`.
//!
//! The worker applies a request through [`Scheduler::handle_request`], which
//! returns the existing record of an applied request. The record of a rerun
//! commits with the submit of the task instance. A request job delivered a
//! second time after a crash therefore does not submit a second task instance.
//! The expiry index entry of a record commits with the record.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use taquba::{
    EnqueueEntry, EnqueueOptions, InboxSender, JobRecord, LeaseHandle, PermanentFailure,
    SettlementEffects, Worker, WorkerError,
};
use taquba_workflow::RunId;

use crate::partition::Partition;
use crate::records::JsonBytes;
use crate::records::{self, Expiring, RequestOutcome, RequestRecord};
use crate::scheduler::{Error, RerunOutcome, Scheduler, worker_error};

/// The queue of the request jobs. The writer of the queue must open it with an
/// inbox that accepts this queue, such as
/// `OpenOptions::inbox(InboxOptions::new([REQUESTS_QUEUE]))`.
pub const REQUESTS_QUEUE: &str = "swale-requests";

/// Maximum length of a request id in bytes.
pub const MAX_REQUEST_ID_LEN: usize = 64;

/// The id of a request: `[A-Za-z0-9_-]` of one to [`MAX_REQUEST_ID_LEN`] bytes.
/// A generated id is a ULID, so the ids of a listing sort by the time of the
/// request.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(String);

/// The text is not a request id.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("`{0}` is not a request id: `[A-Za-z0-9_-]` of one to {MAX_REQUEST_ID_LEN} bytes")]
pub struct InvalidRequestId(pub String);

impl RequestId {
    /// Validates `text` as a request id.
    pub fn new(text: impl Into<String>) -> Result<Self, InvalidRequestId> {
        let text = text.into();
        let valid = !text.is_empty()
            && text.len() <= MAX_REQUEST_ID_LEN
            && text
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
        if valid {
            Ok(RequestId(text))
        } else {
            Err(InvalidRequestId(text))
        }
    }

    /// A new id for a request at `now_ms`, in milliseconds from the Unix epoch.
    pub fn generate(now_ms: u64) -> Self {
        let time = SystemTime::UNIX_EPOCH + Duration::from_millis(now_ms);
        RequestId(ulid::Ulid::from_datetime(time).to_string())
    }

    /// The id text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for RequestId {
    type Err = InvalidRequestId;

    fn from_str(text: &str) -> Result<Self, InvalidRequestId> {
        RequestId::new(text)
    }
}

/// An operation on a running daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Request {
    /// Start the graph run of every listed partition of the graph's adopted
    /// definition. A partition with a graph run is unchanged.
    Start {
        /// The graph name.
        graph: String,
        /// The partitions.
        partitions: Vec<Partition>,
    },
    /// Run a node with a failed or cancelled record again, at the next rerun
    /// count.
    Rerun {
        /// The graph name.
        graph: String,
        /// The partition.
        partition: Partition,
        /// The node name.
        node: String,
    },
    /// Cancel an active graph run.
    Cancel {
        /// The graph name.
        graph: String,
        /// The partition.
        partition: Partition,
    },
}

impl JsonBytes for Request {}

impl Request {
    /// Sends the request with `id` through `sender`, the sender of the inbox of
    /// the queue. The request job takes `id` as its job id.
    pub async fn send(&self, sender: &InboxSender, id: &RequestId) -> Result<(), taquba::Error> {
        let entry = EnqueueEntry {
            payload: self.to_bytes(),
            options: EnqueueOptions {
                id_override: Some(id.to_string()),
                ..EnqueueOptions::default()
            },
        };
        sender
            .send_with_id(REQUESTS_QUEUE, id.as_str(), entry)
            .await
    }
}

/// The [`Worker`] of the requests queue. A job whose id is not a request id or
/// whose payload is not a request is dead-lettered.
pub struct RequestWorker {
    scheduler: Arc<Scheduler>,
}

impl RequestWorker {
    /// A worker that applies requests on `scheduler`.
    pub fn new(scheduler: Arc<Scheduler>) -> Self {
        RequestWorker { scheduler }
    }
}

impl Worker for RequestWorker {
    async fn process(&self, job: &JobRecord, _lease: &LeaseHandle) -> Result<(), WorkerError> {
        let id =
            RequestId::new(job.id.clone()).map_err(|e| PermanentFailure::new(e.to_string()))?;
        let request = Request::from_bytes(&job.payload)
            .map_err(|e| PermanentFailure::new(format!("the payload is not a request: {e}")))?;
        let record = self
            .scheduler
            .handle_request(&id, &request)
            .await
            .map_err(worker_error)?;
        log_outcome(&id, &record);
        Ok(())
    }
}

fn log_outcome(id: &RequestId, record: &RequestRecord) {
    match &record.outcome {
        RequestOutcome::Refused { reason } => {
            tracing::warn!(request = %id, %reason, "request refused");
        }
        outcome => {
            tracing::info!(request = %id, ?outcome, "request applied");
        }
    }
}

impl Scheduler {
    /// Applies the request `id` and writes its record at the request's key, or
    /// returns the existing record of an applied request. A request that refers
    /// to an absent graph, graph run or node is refused in the record, as is a
    /// request that the state of the graph run does not admit. A failure of the
    /// store is returned, and the record is not written.
    pub async fn handle_request(
        &self,
        id: &RequestId,
        request: &Request,
    ) -> Result<RequestRecord, Error> {
        let key = records::request_key(id);
        if let Some(record) = records::read::<RequestRecord>(self.queue.view(), &key).await? {
            return Ok(record);
        }
        let handled_at_ms = self.clock.now_ms();
        let record = |outcome| RequestRecord {
            request: request.clone(),
            handled_at_ms,
            outcome,
        };
        let outcome = match request {
            Request::Start { graph, partitions } => {
                match self.start_runs(graph, partitions).await {
                    Ok(partitions) => RequestOutcome::Started { partitions },
                    Err(e) => RequestOutcome::Refused {
                        reason: refusal(e)?,
                    },
                }
            }
            Request::Rerun {
                graph,
                partition,
                node,
            } => {
                let rerun_record = |run_id: &RunId| {
                    record(RequestOutcome::Rerun {
                        run_id: run_id.to_string(),
                    })
                };
                let outcome = self
                    .rerun_with(graph, partition, node, |run_id| {
                        self.request_effects(id, &rerun_record(run_id))
                    })
                    .await;
                let reason = match outcome {
                    Ok(RerunOutcome::Submitted(run_id)) => return Ok(rerun_record(&run_id)),
                    Ok(RerunOutcome::Active(run_id)) => format!("the rerun `{run_id}` is active"),
                    Ok(RerunOutcome::NoRecord) => format!("node `{node}` does not have a record"),
                    Ok(RerunOutcome::NotReady) => format!(
                        "node `{node}` is not ready: its upstreams do not satisfy its trigger rule"
                    ),
                    Err(e) => refusal(e)?,
                };
                RequestOutcome::Refused { reason }
            }
            Request::Cancel { graph, partition } => {
                if self.cancel_run(graph, partition).await? {
                    RequestOutcome::Cancelled
                } else {
                    RequestOutcome::Refused {
                        reason: format!(
                            "graph `{graph}` does not have an active run for partition `{partition}`"
                        ),
                    }
                }
            }
        };
        let record = record(outcome);
        self.queue
            .commit_effects(self.request_effects(id, &record))
            .await?;
        Ok(record)
    }

    /// The write of `record` at the key of the request `id` with the expiry
    /// index entry of the record.
    fn request_effects(&self, id: &RequestId, record: &RequestRecord) -> SettlementEffects {
        SettlementEffects::default()
            .kv_put(records::request_key(id), record.to_bytes())
            .expiry_entry(
                &self.expiry,
                record.handled_at_ms,
                &Expiring::Request(id.clone()).suffix(),
            )
    }
}

/// The reason a request is refused for `error`, or the error when it is a
/// failure of the store, which the caller retries.
fn refusal(error: Error) -> Result<String, Error> {
    if error.is_permanent() {
        Ok(error.to_string())
    } else {
        Err(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_generated_id_sorts_by_time_and_the_charset_is_checked() {
        let earlier = RequestId::generate(1_700_000_000_000);
        let later = RequestId::generate(1_700_000_000_001);
        assert!(earlier < later);
        assert_eq!(earlier.as_str().len(), 26);
        assert_eq!(RequestId::new(earlier.as_str()), Ok(earlier));
        for text in ["", "a/b", "a b", &"x".repeat(MAX_REQUEST_ID_LEN + 1)] {
            assert!(RequestId::new(text).is_err(), "{text}");
        }
        assert_eq!("req-1".parse::<RequestId>().unwrap().to_string(), "req-1");
    }

    #[test]
    fn the_json_form_has_a_kind_tag() {
        let start = Request::Start {
            graph: "orders".into(),
            partitions: vec![Partition::new("20260915").unwrap()],
        };
        assert_eq!(
            start.to_bytes(),
            br#"{"kind":"start","graph":"orders","partitions":["20260915"]}"#
        );
        let rerun = Request::Rerun {
            graph: "orders".into(),
            partition: Partition::new("20260915").unwrap(),
            node: "transform".into(),
        };
        assert_eq!(
            rerun.to_bytes(),
            br#"{"kind":"rerun","graph":"orders","partition":"20260915","node":"transform"}"#
        );
        let cancel = Request::Cancel {
            graph: "orders".into(),
            partition: Partition::new("20260915").unwrap(),
        };
        assert_eq!(
            cancel.to_bytes(),
            br#"{"kind":"cancel","graph":"orders","partition":"20260915"}"#
        );
        for request in [start, rerun, cancel] {
            assert_eq!(Request::from_bytes(&request.to_bytes()).unwrap(), request);
        }
        assert!(Request::from_bytes(br#"{"kind":"forget","graph":"orders"}"#).is_err());
    }
}
