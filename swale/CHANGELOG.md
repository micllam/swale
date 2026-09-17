# Changelog

All notable changes to the `swale` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- The `swale daemon` command and the `Daemon` type: the process adopts the
  published definitions, fires the schedule of each graph through
  `taquba-cron` and runs the task instances. The first adoption of a graph
  runs the partitions within its `catchup` window, and the daemon replays the
  firings missed during downtime within the same window.
- The `swale publish` command and `DefinitionStore::publish`, which write a
  definition and the pointer of its graph to the object store. The publish
  refuses an asset that the current definition of another graph produces.
- `Partition::of_time`, which gives the partition that contains a time. The
  partition of a firing contains the start of the schedule interval that ends
  at the firing time.
- The `Trigger` payload, the `swale-triggers` queue and
  `Scheduler::handle_trigger`, which start the graph runs of a firing or of a
  list of partitions.
- The graph record at `swale/graphs/{graph}`, which records the adopted
  definition of a graph.
- The default store of every command, `~/.swale/store`.
- The `swale status` and `swale queues` commands and the `StatusReader` type,
  which read the graphs, the graph runs, the node states and the queues of a
  store through a `QueueReader`. The commands only read from the store, so
  they can run alongside a daemon.

### Changed

- **Breaking:** `DefinitionStore` stores every definition in the object store
  at `definitions/{hash}.toml` within the store prefix, so a graph run resumes
  with its definition after the file changes. Construct it with
  `DefinitionStore::new(store, store_prefix, operators)` and replace `insert`
  with `put`, and `get` is async. The type moved from `scheduler` to
  `definition_store`.
- **Breaking:** a definition with a `schedule` and without a `daily` or
  `hourly` partition fails the load with `Problem::ScheduleWithoutPartition`.
  Declare `partition` in a scheduled graph.
- **Breaking:** the `schedule` of a definition is parsed by `taquba-cron`
  0.10, and the direct `croner` dependency is removed. A step without a range,
  as in `5/5 * * * *`, fails the load, and a `+` prefix on the day-of-week
  field requires both day fields to match. Write the range, as in
  `5-59/5 * * * *`, and remove the prefix to keep the earlier firing times.
- **Breaking:** `Graph::schedule` returns the parsed `taquba_cron::Expression`.
  Call `to_string` on the value for the text, which the parser normalises.
- **Breaking:** `scheduler::Error` has the variants `Definition`,
  `UnknownGraph` and `NoPartition`. Add a wildcard arm to an exhaustive match.

### Fixed

- A store URL with a cloud scheme reads the provider's environment variables
  (`AWS_*`, `GOOGLE_*` and `AZURE_*`) for its credentials, its region and its
  endpoint. 0.1.0 did not read them, so an `s3://` store did not open with
  the credentials of the environment.

## [0.1.0] - 2026-09-16

### Added

- The definition model: a graph of asset nodes and task nodes with the
  trigger rules `all_succeeded`, `all_done` and `one_failed`, loaded from a
  TOML file by `load_str` and `load_path`. The loader reports every fault of a
  definition at once as a `Problem`.
- The template language of parameter strings, rendered when the step runs:
  `{{ partition }}`, `{{ run.id }}`, `{{ run.summary }}` and
  `{{ upstream.<node>.<path> }}`. A reference to an absent output fails the
  task.
- The scheduler over the taquba runtime: every task instance runs as one
  workflow run with the id `{graph}-{partition}-{node}-r{n}`, the terminal
  hook writes the node's record and enqueues the scheduler's event in one
  transaction, and the reconciler resubmits ready nodes and settles graph
  runs. `Scheduler::start_run`, `Scheduler::rerun` and `Scheduler::cancel_run`
  are the operations.
- The KV records at the `swale/` prefix: the graph run record and the asset
  and task records, as JSON.
- The `Operator` trait and the `OperatorSet` of a process, with the
  `subprocess` operator.
- The `swale` command, enabled by the default `cli` feature: `validate` checks a
  definition, and `run` runs a graph for one partition on a directory or an
  object store URL and resumes an existing run.
- The `aws`, `gcp` and `azure` features, which enable the object store
  backends of the same names in `taquba`.
