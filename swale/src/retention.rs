//! The retention of the records: the pass that removes the records of a settled
//! graph run and the request records past the retention window.
//!
//! The expiry index ([`taquba::ExpiryIndex`]) at [`records::EXPIRY_PREFIX`] has
//! one entry per settled graph run and per request record, at the settle time
//! or the handling time, with an [`Expiring`] suffix. The entry commits with
//! the record it refers to. [`Scheduler::expire`] runs one pass of the index.
//! For a due entry of a graph run the pass removes the node records, the graph
//! run record and the entry in one transaction, and for a due entry of a
//! request record it removes the record and the entry. The pass discards an
//! entry whose record is absent or whose time differs from the entry's, which
//! is the entry of a settle that a rerun reactivated. A graph run whose
//! definition does not load is kept, and the next pass reads its entry again. A
//! start of a partition whose records were removed runs the graph again.
//!
//! The daemon runs the pass after a request pass that succeeded, so a request
//! object with a record is removed before the record expires, and a request is
//! not applied twice.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use taquba::{Expired, SettlementEffects};

use crate::partition::Partition;
use crate::records::{self, Expiring, GraphRunRecord, RequestRecord};
use crate::request::RequestId;
use crate::scheduler::{Error, Scheduler};

/// Counts of one retention pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExpireReport {
    /// Graph runs whose records were removed.
    pub runs: usize,
    /// Request records removed.
    pub requests: usize,
}

impl Scheduler {
    /// One retention pass: removes the records of every graph run settled
    /// `retention` or more before the clock, and every request record of the
    /// same age. A graph run whose definition does not load is logged and kept,
    /// and its entry is read again at the next pass. A failure of the store or
    /// a record that does not parse ends the removals of the pass, and the pass
    /// returns the failure once the index is read: the entries after the
    /// failure are kept for the next pass.
    pub async fn expire(&self, retention: Duration) -> Result<ExpireReport, Error> {
        let runs = AtomicUsize::new(0);
        let requests = AtomicUsize::new(0);
        let failure: Mutex<Option<Error>> = Mutex::new(None);
        let (runs, requests, failure) = (&runs, &requests, &failure);
        let now = self.clock.now_ms();
        self.expiry
            .pass(&self.queue, now, retention, |time_ms, suffix| async move {
                if failure.lock().expect("the failure is not poisoned").is_some() {
                    return Expired::Keep;
                }
                let Some(expiring) = Expiring::parse(&suffix) else {
                    tracing::warn!(suffix = %String::from_utf8_lossy(&suffix), "expiry index entry removed: not an entry");
                    return Expired::Delete(SettlementEffects::default());
                };
                let (expired, count) = match &expiring {
                    Expiring::Run { graph, partition } => {
                        (self.expire_run(time_ms, graph, partition).await, runs)
                    }
                    Expiring::Request(id) => (self.expire_request(time_ms, id).await, requests),
                };
                match expired {
                    Ok(expired) => {
                        // The record is removed under the compare.
                        if matches!(expired, Expired::DeleteIf { .. }) {
                            count.fetch_add(1, Ordering::Relaxed);
                        }
                        expired
                    }
                    Err(e) => {
                        *failure.lock().expect("the failure is not poisoned") = Some(e);
                        Expired::Keep
                    }
                }
            })
            .await?;
        if let Some(e) = failure.lock().expect("the failure is not poisoned").take() {
            return Err(e);
        }
        Ok(ExpireReport {
            runs: runs.load(Ordering::Relaxed),
            requests: requests.load(Ordering::Relaxed),
        })
    }

    /// The removal of the records of the graph run settled at `time_ms`: the
    /// node records and the graph run record, against the record's stored
    /// bytes. A run whose record is absent or was settled at another time is
    /// stale, and a run whose definition does not load is kept.
    async fn expire_run(
        &self,
        time_ms: u64,
        graph: &str,
        partition: &Partition,
    ) -> Result<Expired, Error> {
        let run_key = records::graph_run_key(graph, partition);
        let Some(bytes) = self.queue.view().kv_get(&run_key).await? else {
            return Ok(stale());
        };
        let run = records::parse::<GraphRunRecord>(&run_key, &bytes)?;
        if run.settled_at_ms != Some(time_ms) {
            return Ok(stale());
        }
        let definition = match self.definitions().get(&run.definition).await {
            Ok(Some(definition)) => definition,
            Ok(None) => {
                tracing::warn!(%graph, %partition, definition = %run.definition, "graph run records an unknown definition");
                return Ok(Expired::Keep);
            }
            Err(e) => {
                tracing::warn!(%graph, %partition, definition = %run.definition, error = %e, "the definition of a graph run does not load");
                return Ok(Expired::Keep);
            }
        };
        let mut effects = SettlementEffects::default().kv_delete(run_key.clone());
        for node in definition.nodes() {
            effects = effects.kv_delete(records::node_record_key(graph, partition, node));
        }
        tracing::info!(%graph, %partition, "graph run expired");
        Ok(Expired::DeleteIf {
            key: run_key,
            expected: Some(bytes.to_vec()),
            effects,
        })
    }

    /// The removal of the request record handled at `time_ms`, against its
    /// stored bytes. A record that is absent or was handled at another time is
    /// stale.
    async fn expire_request(&self, time_ms: u64, id: &RequestId) -> Result<Expired, Error> {
        let key = records::request_key(id);
        let Some(bytes) = self.queue.view().kv_get(&key).await? else {
            return Ok(stale());
        };
        if records::parse::<RequestRecord>(&key, &bytes)?.handled_at_ms != time_ms {
            return Ok(stale());
        }
        Ok(Expired::DeleteIf {
            key: key.clone(),
            expected: Some(bytes.to_vec()),
            effects: SettlementEffects::default().kv_delete(key),
        })
    }
}

/// The outcome of an entry without a matching record: the entry is removed
/// alone.
fn stale() -> Expired {
    Expired::Delete(SettlementEffects::default())
}
