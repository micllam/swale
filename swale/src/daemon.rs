//! The daemon: the long-running process of a deployment. It runs the pools
//! and the [`Scheduler`], adopts the published definitions and schedules the
//! cron entry of every adopted graph with a schedule.
//!
//! # Adoption
//!
//! A sync pass ([`Daemon::sync`]) reads the pointer objects of the
//! [`DefinitionStore`](crate::DefinitionStore) and writes the [`GraphRecord`]
//! of every graph whose pointer changed. A trigger starts the definition of
//! the graph record, so an edit applies from the next graph run. The pass
//! refuses a definition that does not load, that uses a pool without a
//! runtime or that produces an asset of another adopted graph, and the graph
//! keeps its adopted definition.
//!
//! # Catch-up and backfill
//!
//! The cron entry of a graph with a `catchup` window has a backfill of that
//! window, which starts at the lookback ([`BackfillStart::Lookback`]). The
//! cron scheduler replays the firings within the window for a new graph, and
//! the firings missed during downtime for a graph that fired before.
//!
//! The daemon keeps one cron scheduler for its lifetime. Every sync pass
//! gives the [`Schedule`] of each adopted graph to
//! [`ScheduleHandle::replace_all`], which leaves an equal schedule untouched.
//!
//! # Requests
//!
//! After the sync pass, a request pass ([`Daemon::apply_requests`]) reads
//! the request objects of the [`RequestStore`] in id order and applies each
//! request through [`Scheduler::handle_request`], which records the outcome
//! at the request's key. The pass then removes the object. A request whose
//! record exists is not applied again, so a crash between the record and
//! the removal does not apply the request twice.

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use taquba::{Clock, Queue};
use taquba_cron::{Backfill, BackfillStart, CronScheduler, Schedule, ScheduleHandle};
use tokio_util::sync::CancellationToken;

use crate::graph::Graph;
use crate::pools::Pools;
use crate::records::JsonBytes;
use crate::records::{self, GraphRecord, RequestOutcome, RequestRecord};
use crate::request::{Request, RequestId, RequestStore};
use crate::scheduler::{Error, Scheduler, SchedulerOptions, TRIGGERS_QUEUE, firing_headers};

/// The settings of [`Daemon::run`].
#[derive(Debug, Clone)]
pub struct DaemonOptions {
    /// The settings of the scheduler.
    pub scheduler: SchedulerOptions,
    /// The time between sync passes.
    pub sync_interval: Duration,
}

impl Default for DaemonOptions {
    fn default() -> Self {
        DaemonOptions {
            scheduler: SchedulerOptions::default(),
            sync_interval: Duration::from_secs(30),
        }
    }
}

/// The result of one sync pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncReport {
    /// The graphs whose graph record was written, with the adopted hash.
    pub adopted: Vec<(String, String)>,
    /// The graphs whose published definition was refused, with the reason.
    pub refused: Vec<(String, String)>,
    /// The cron schedules of the adopted graphs, in graph name order. The
    /// name of a schedule is the graph name.
    pub schedules: Vec<Schedule>,
}

/// The result of one request pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestReport {
    /// The requests the pass applied, in id order, with the outcome of each.
    pub applied: Vec<(RequestId, RequestOutcome)>,
}

/// The daemon.
pub struct Daemon {
    queue: Arc<Queue>,
    scheduler: Arc<Scheduler>,
    pools: Arc<Pools>,
    requests: RequestStore,
    clock: Arc<dyn Clock>,
    /// The keys of the refusals that were logged.
    logged: Mutex<HashSet<String>>,
}

impl Daemon {
    /// A daemon over `queue` that runs `scheduler` and `pools` and applies
    /// the requests of `requests`.
    pub fn new(
        queue: Arc<Queue>,
        scheduler: Arc<Scheduler>,
        pools: Arc<Pools>,
        requests: RequestStore,
    ) -> Self {
        let clock = queue.clock();
        Daemon {
            queue,
            scheduler,
            pools,
            requests,
            clock,
            logged: Mutex::new(HashSet::new()),
        }
    }

    /// One sync pass: adopts the published definitions that changed.
    pub async fn sync(&self) -> Result<SyncReport, Error> {
        let definitions = self.scheduler.definitions();
        let current = definitions.current().await?;
        let mut report = SyncReport::default();
        let mut graphs: BTreeMap<String, Arc<Graph>> = BTreeMap::new();
        let mut pending = Vec::new();

        // Every adopted graph loads first, and the asset check of a new
        // definition reads all of them.
        for (name, hash) in &current {
            let record = self.scheduler.graph_record(name).await?;
            if let Some(record) = &record
                && let Some(graph) = definitions.get(&record.definition).await?
            {
                graphs.insert(name.clone(), graph);
            }
            if record.as_ref().map(|r| &r.definition) != Some(hash) {
                pending.push((name, hash));
            }
        }

        for (name, hash) in pending {
            let graph = match definitions.get(hash).await {
                Ok(Some(graph)) => graph,
                Ok(None) => {
                    self.refuse(
                        &mut report,
                        name,
                        hash,
                        "the definition object is absent".into(),
                    );
                    continue;
                }
                Err(e) => {
                    self.refuse(&mut report, name, hash, e.to_string());
                    continue;
                }
            };
            if let Err(reason) = self.check(name, &graph, &graphs) {
                self.refuse(&mut report, name, hash, reason);
                continue;
            }
            self.adopt(hash, name).await?;
            tracing::info!(graph = %name, definition = %hash, "definition adopted");
            report.adopted.push((name.clone(), hash.clone()));
            graphs.insert(name.clone(), graph);
        }

        report.schedules = graphs.values().filter_map(|g| schedule_of(g)).collect();
        Ok(report)
    }

    /// One request pass: applies every request object without a record and
    /// removes every request object. An object that is not a request is
    /// removed and logged. A request that fails on the store is logged and
    /// stays for the next pass, and the pass continues with the next
    /// request.
    pub async fn apply_requests(&self) -> Result<RequestReport, Error> {
        let mut report = RequestReport::default();
        for (id, bytes) in self.requests.list().await? {
            let key = records::request_key(&id);
            if self.queue.kv_get(&key).await?.is_none() {
                match Request::from_bytes(&bytes) {
                    Ok(request) => match self.scheduler.handle_request(&id, &request).await {
                        Ok(record) => {
                            log_outcome(&id, &record);
                            report.applied.push((id.clone(), record.outcome));
                        }
                        Err(e) => {
                            tracing::warn!(request = %id, error = %e, "request failed");
                            continue;
                        }
                    },
                    Err(e) => {
                        tracing::warn!(request = %id, error = %e, "the object is not a request");
                    }
                }
            }
            self.requests.remove(&id).await?;
        }
        Ok(report)
    }

    /// Runs the pools, the scheduler, the sync pass at its interval and the
    /// cron entries until `shutdown` resolves.
    pub async fn run<F: Future<Output = ()>>(
        &self,
        options: DaemonOptions,
        shutdown: F,
    ) -> Result<(), Error> {
        let stop = CancellationToken::new();
        let pool_handles = self.pools.spawn(&stop);
        let scheduler_handle = self
            .scheduler
            .clone()
            .spawn(options.scheduler.clone(), stop.clone().cancelled_owned());

        let cron = CronScheduler::new(self.queue.clone());
        let handle = cron.handle();
        let cron = cron.spawn(std::future::pending());
        let mut shutdown = std::pin::pin!(shutdown);
        loop {
            match self.sync().await {
                Ok(report) => self.register(&handle, report.schedules),
                Err(e) => tracing::warn!(error = %e, "sync pass failed"),
            }
            if let Err(e) = self.apply_requests().await {
                tracing::warn!(error = %e, "request pass failed");
            }
            tokio::select! {
                () = tokio::time::sleep(options.sync_interval) => {}
                () = &mut shutdown => break,
            }
        }

        let _ = cron.shutdown().await;
        stop.cancel();
        for handle in pool_handles {
            let _ = handle.wait().await;
        }
        scheduler_handle.wait().await
    }

    /// Replaces the schedules of the cron scheduler with `schedules`. An
    /// equal schedule keeps its next firing. The call applies the whole set
    /// or no part of it, so a schedule that the scheduler rejects for its
    /// catch-up window is removed from the set and reported.
    fn register(&self, handle: &ScheduleHandle, mut schedules: Vec<Schedule>) {
        loop {
            match handle.replace_all(schedules.clone()) {
                Ok(()) => return,
                Err(taquba_cron::Error::UnboundedStart(graph)) => {
                    self.log_once(&format!("cron/{graph}"), || {
                        tracing::error!(%graph, "the catch-up window is unbounded, and the schedule does not fire");
                    });
                    schedules.retain(|schedule| schedule.name != graph);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "cron schedules refused");
                    return;
                }
            }
        }
    }

    /// The reason to refuse `graph` as the definition of the pointer `name`.
    fn check(
        &self,
        name: &str,
        graph: &Graph,
        adopted: &BTreeMap<String, Arc<Graph>>,
    ) -> Result<(), String> {
        if graph.name() != name {
            return Err(format!("the definition is of graph `{}`", graph.name()));
        }
        for node in graph.nodes() {
            if self.pools.runtime(node.pool()).is_none() {
                return Err(format!(
                    "node `{}`: pool `{}` does not have a runtime",
                    node.name(),
                    node.pool()
                ));
            }
        }
        for (other, other_graph) in adopted {
            if other == name {
                continue;
            }
            if let Some(node) = graph.conflicting_asset(other_graph) {
                return Err(format!(
                    "node `{}`: asset `{}` is produced by graph `{other}`",
                    node.name(),
                    node.asset().unwrap_or_default()
                ));
            }
        }
        Ok(())
    }

    fn refuse(&self, report: &mut SyncReport, name: &str, hash: &str, reason: String) {
        self.log_once(hash, || {
            tracing::error!(graph = %name, definition = %hash, %reason, "definition refused");
        });
        report.refused.push((name.to_string(), reason));
    }

    /// Runs `log` at the first call with `key`.
    fn log_once(&self, key: &str, log: impl FnOnce()) {
        let first = self
            .logged
            .lock()
            .expect("the log set is not poisoned")
            .insert(key.to_string());
        if first {
            log();
        }
    }

    /// Writes the graph record.
    async fn adopt(&self, hash: &str, name: &str) -> Result<(), Error> {
        let record = GraphRecord {
            definition: hash.to_string(),
            adopted_at_ms: self.clock.now_ms(),
        };
        self.queue
            .kv_put(&records::graph_key(name), &record.to_bytes())
            .await?;
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

/// The cron schedule of a graph with a `schedule`: a firing on the triggers
/// queue with the graph in its headers, with the catch-up window as the
/// backfill.
fn schedule_of(graph: &Graph) -> Option<Schedule> {
    let backfill = graph.catchup().map(|lookback| Backfill {
        lookback,
        start: BackfillStart::Lookback,
    });
    Some(
        Schedule::new(
            graph.name(),
            graph.schedule()?.clone(),
            TRIGGERS_QUEUE,
            Vec::new(),
        )
        .headers(firing_headers(graph.name()))
        .backfill(backfill),
    )
}
