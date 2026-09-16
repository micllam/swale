//! A scheduled, dependency-ordered orchestrator of asset graphs.
//!
//! swale runs on the taquba durable execution crates. Every task instance
//! runs as a single workflow run, and the record of its outcome and the event
//! for the scheduler commit in one transaction.
//!
//! # Definition file
//!
//! One TOML file per graph. An asset node declares the asset it produces and
//! the assets it consumes, and a task node declares its upstream nodes with
//! `after`.
//!
//! ```toml
//! [graph]
//! name = "orders_daily"
//! schedule = "0 2 * * *"
//! partition = "daily"
//!
//! [[node]]
//! name = "extract"
//! produces = "orders_raw"
//! operator = "subprocess"
//! [node.params]
//! argv = ["python", "tasks/extract.py"]
//!
//! [[node]]
//! name = "load"
//! produces = "orders_warehouse"
//! consumes = ["orders_raw"]
//! operator = "subprocess"
//! [node.params]
//! argv = ["python", "tasks/load.py", "{{ upstream.extract.rows_key }}"]
//! ```
//!
//! The [`definition`] module documents every field, and the loader reports
//! every fault of a definition at once. From Rust, [`load_path`] returns the
//! checked graph:
//!
//! ```
//! use std::path::Path;
//! use swale::OperatorSet;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let graph = swale::load_path(
//!     Path::new("examples/orders_daily.toml"),
//!     &OperatorSet::builtin(),
//! )?;
//! println!("{}: {} nodes", graph.name(), graph.nodes().len());
//! # Ok(()) }
//! ```
//!
//! # Command
//!
//! The `swale` binary is enabled by the default `cli` feature. A program that
//! embeds the library can set `default-features = false`.
//!
//! `swale validate` loads a definition and prints each fault, one per line.
//!
//! `swale run` runs a graph for one partition on a store, prints each node's
//! record as it is written and waits for the run to settle. The store is a
//! directory or an object store URL (`s3://bucket/prefix`,
//! `gs://bucket/prefix`, `az://container/prefix`). A cloud scheme needs the
//! matching cargo feature (`aws`, `gcp` or `azure`) and reads the provider's
//! environment variables for its credentials. A second `run` for the same
//! partition resumes the existing graph run, and a process interrupted
//! mid-run resumes without repeating a completed node.
//!
//! ```console
//! $ swale validate examples/orders_daily.toml
//! orders_daily: 5 nodes, 3 edges
//! $ swale run examples/local.toml --store ./swale-store
//! local/none: started, 1 root node(s) submitted
//!   first: succeeded (local-none-first-r0)
//!   second: succeeded (local-none-second-r0)
//! local/none: complete
//! ```
//!
//! The exit status is 0 for a valid definition or a complete run, 1 for a
//! fault, a failed run or an error, and 2 for a usage error.

pub mod definition;
pub mod dispatch;
pub mod duration;
mod error;
pub mod graph;
pub mod hook;
pub mod input;
pub mod operator;
pub mod partition;
pub mod records;
pub mod scheduler;
pub mod subprocess;
pub mod task;
pub mod template;

pub use definition::{load_path, load_str};
pub use error::Error;
pub use graph::{Graph, GraphSpec, Node, NodeKind, NodeSpec, Partitioning, Problem, TriggerRule};
pub use hook::{EVENTS_QUEUE, Event, RecordHook};
pub use operator::{Operator, OperatorSet, Outcome, Task};
pub use partition::Partition;
pub use records::{GraphRunRecord, GraphRunState, NodeRecord, RecordStatus};
pub use scheduler::{DefinitionStore, Pools, Scheduler, SchedulerOptions, StartOutcome};
pub use task::TaskIdentity;
pub use template::Template;
