//! The pools: one [`WorkflowRuntime`] per pool, all over one queue and one
//! store.
//!
//! Every task instance runs on the runtime of its node's pool. The queue of
//! the pool `name` is `swale-pool-{name}`, and its memos are at `memos/{name}`
//! within the store prefix. The terminal hook of each runtime is the
//! [`RecordHook`], which writes the node's record and enqueues the event for
//! the scheduler.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use taquba::Queue;
use taquba::object_store::ObjectStore;
use taquba_workflow::{RunnerHandle, WorkflowRuntime};
use tokio_util::sync::CancellationToken;

use crate::dispatch::Dispatch;
use crate::hook::RecordHook;
use crate::operator::OperatorSet;
use crate::store::store_path;

/// The runtime of a pool.
pub type PoolRuntime = WorkflowRuntime<Dispatch, RecordHook>;

/// The path of the memos of the pool `pool`: `memos/{pool}` within
/// `store_prefix`.
pub(crate) fn memo_prefix(store_prefix: &str, pool: &str) -> String {
    store_path(store_prefix, &format!("memos/{pool}"))
}

/// One [`WorkflowRuntime`] per pool, all over one queue and one store.
pub struct Pools {
    runtimes: HashMap<String, PoolRuntime>,
}

/// Builds a [`Pools`].
pub struct PoolsBuilder {
    queue: Arc<Queue>,
    store: Arc<dyn ObjectStore>,
    dispatch: Dispatch,
    hook: RecordHook,
    poll_interval: Duration,
    memo_retention: Duration,
    store_prefix: String,
    pools: Vec<(String, usize)>,
}

impl PoolsBuilder {
    /// Adds the pool `name` with `max_concurrent_steps` steps at a time. Its
    /// queue is `swale-pool-{name}`, and its memos are at `memos/{name}`
    /// within the store prefix.
    pub fn pool(mut self, name: impl Into<String>, max_concurrent_steps: usize) -> Self {
        self.pools.push((name.into(), max_concurrent_steps));
        self
    }

    /// The poll interval of every pool's step worker.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// The time the memos and the run result record of a terminated task
    /// instance are kept, seven days by default. The scheduler does not read
    /// them, so the retention determines how long an operator can inspect
    /// them.
    pub fn memo_retention(mut self, retention: Duration) -> Self {
        self.memo_retention = retention;
        self
    }

    /// The path within the store that every pool writes its memos under,
    /// for a store whose queue is opened at a prefix. Empty by default.
    pub fn store_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.store_prefix = prefix.into();
        self
    }

    /// Builds the runtimes.
    pub fn build(self) -> Pools {
        let runtimes = self
            .pools
            .into_iter()
            .map(|(name, concurrency)| {
                let runtime = WorkflowRuntime::builder(
                    self.queue.clone(),
                    self.store.clone(),
                    self.dispatch.clone(),
                    self.hook.clone(),
                )
                .queue_name(format!("swale-pool-{name}"))
                .memo_prefix(memo_prefix(&self.store_prefix, &name))
                .max_concurrent_steps(concurrency)
                .poll_interval(self.poll_interval)
                .memo_retention(self.memo_retention)
                .build();
                (name, runtime)
            })
            .collect();
        Pools { runtimes }
    }
}

impl Pools {
    /// Starts building pools over `queue` and `store`, with `operators` as
    /// the dispatch and `hook` as the terminal hook of every pool.
    pub fn builder(
        queue: Arc<Queue>,
        store: Arc<dyn ObjectStore>,
        operators: Arc<OperatorSet>,
        hook: RecordHook,
    ) -> PoolsBuilder {
        let dispatch = Dispatch::new(operators, queue.clock());
        PoolsBuilder {
            queue,
            store,
            dispatch,
            hook,
            poll_interval: Duration::from_millis(250),
            memo_retention: Duration::from_secs(7 * 86_400),
            store_prefix: String::new(),
            pools: Vec::new(),
        }
    }

    /// The runtime of the pool `name`.
    pub fn runtime(&self, name: &str) -> Option<&PoolRuntime> {
        self.runtimes.get(name)
    }

    /// Spawns the step worker of every pool. Each stops when `shutdown` is
    /// cancelled.
    pub fn spawn(&self, shutdown: &CancellationToken) -> Vec<RunnerHandle> {
        self.runtimes
            .values()
            .map(|runtime| runtime.spawn(shutdown.clone().cancelled_owned()))
            .collect()
    }
}
