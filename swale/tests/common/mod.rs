//! The setup shared by the tests that run the scheduler in the process.

use std::sync::Arc;
use std::time::Duration;

use taquba::object_store::ObjectStore;
use taquba::object_store::memory::InMemory;
use taquba::{MockClock, OpenOptions, Queue, QueueConfig};

/// The path of the queue within the store.
pub const QUEUE_PATH: &str = "test";

/// The time a wait allows before it fails.
const WAIT_DEADLINE: Duration = Duration::from_secs(30);

/// The time between two probes of a wait.
const WAIT_INTERVAL: Duration = Duration::from_millis(20);

/// An in-memory store and a queue over it at [`QUEUE_PATH`], with `clock`,
/// without a retry backoff and with a reaper and a scheduler interval of
/// ten milliseconds.
pub async fn open_queue(clock: MockClock) -> (Arc<dyn ObjectStore>, Arc<Queue>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let opts = OpenOptions::default()
        .clock(Arc::new(clock))
        .default_queue_config(QueueConfig::default().retry_backoff_base(Duration::ZERO))
        .reaper_interval(Duration::from_millis(10))
        .scheduler_interval(Duration::from_millis(10));
    let queue = Queue::open_with_options(store.clone(), QUEUE_PATH, opts)
        .await
        .unwrap();
    (store, Arc::new(queue))
}

/// The first `Some` of `probe`, polled every twenty milliseconds. The wait
/// fails after thirty seconds with `what`.
pub async fn wait_until<T>(what: &str, mut probe: impl AsyncFnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + WAIT_DEADLINE;
    loop {
        if let Some(value) = probe().await {
            return value;
        }
        assert!(tokio::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(WAIT_INTERVAL).await;
    }
}
