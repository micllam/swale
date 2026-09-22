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
- `Partition::of_time`, which gives the partition that contains a time, or
  `None` beyond the year 9999. The partition of a firing contains the start
  of the schedule interval that ends at the firing time.
- The `swale-triggers` queue and `Scheduler::handle_trigger`: a cron firing
  is a job with the graph in its `swale.graph` header and without a payload,
  and the handler starts the graph run of the partition of the firing.
- The graph record at `swale/graphs/{graph}`, which records the adopted
  definition of a graph.
- The default store of every command, `~/.swale/store`.
- The `swale status` and `swale queues` commands and the `StatusReader` type,
  which read the graphs, the graph runs, the node states and the queues of a
  store through a `QueueReader`. The commands only read from the store, so
  they can run alongside a daemon.
- The `shell` operator, which runs a command line with `sh -c` with the
  identity of the task instance and the upstream outputs in the environment,
  and the `http` operator, which sends one request and outputs the status and
  the body. Both are in `OperatorSet::builtin`. The crate depends on
  `reqwest` with `rustls`, as the object store does for a cloud backend.
- `JsonBytes`, the trait of the JSON byte form of every record and payload,
  and `records::read` and `records::scan`, which read a record or a listing
  through `KvRead`, implemented by `Queue` and `QueueReader`. A malformed
  record in a listing is logged and skipped.
- `store::store_path` and `store::ObjectPrefix`, the one join of a path with
  the store prefix and the objects within such a path.
- `Graph::conflicting_asset` and `Scheduler::graph_record`.
- The `swale start`, `swale rerun` and `swale cancel` commands and the
  `Request` type, which write a request object to the store for the daemon,
  and the request record at `swale/requests/{id}`, which the daemon writes
  with the outcome and `StatusReader::request` reads. The commands do not
  open the queue, and `--wait` prints the outcome. `Scheduler::start_runs`
  applies a start request. `Daemon::new` takes a `RequestStore`, and the
  crate depends on `ulid` for the request id.
- A rerun of a succeeded node. `swale rerun` and `Scheduler::rerun` run the
  node again and, after it, every node downstream of it through an
  all-succeeded edge, and a task node with the all-done or one-failed rule
  keeps its record. `GraphRunRecord::expected_reruns` lists the rerun count
  each such node must reach, `GraphRunRecord::is_current` states whether a
  record reached it, and the readiness rule reads the current records alone
  (`readiness::current_records`, `readiness::rerun_scope`).
- The `{{ env.<NAME> }}` template reference, an environment variable of the
  daemon rendered at dispatch. A reference to an unset variable fails the
  task instance, and `Dispatch::with_env` builds a runner with given
  variables.
- The `object_exists` operator, which polls a store URL every `interval`
  until the object exists or `timeout` passes after the first poll. Each
  poll is a step of the run, so no worker is held between polls, and the
  crate depends on `url`.
- `Outcome::Continue`, with which an operator ends a step and continues the
  run after a delay with a state, which the next step reads as `Task::state`.
  `Task::now_ms` is the time of the step, and `store::provider_options` reads
  the object store options of a URL from the environment.

### Changed

- **Breaking:** `Dispatch::new` and `Dispatch::with_env` take the clock of
  the store, `Task` has the `state` and `now_ms` fields and `Outcome` has
  the `Continue` variant. Pass the queue's clock, and add a wildcard arm to
  an exhaustive match.
- **Breaking:** `RenderContext` has the `run` and `env` fields in place of
  `run_id` and `run_summary`, `TaskInput::rendered_params` takes the
  environment variables and `RenderError` has the `MissingEnv` variant. Pass
  the variables where the parameters are rendered, and add a wildcard arm to
  an exhaustive match.
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
- **Breaking:** the operators are submodules of `operator`. Import
  `Subprocess` and `SubprocessParams` from `swale::operator::subprocess`.
- **Breaking:** `Template::segments` and `template::Segment` are private.
  `Template::upstream_nodes` and `Template::render` are the public
  operations.
- **Breaking:** `Scheduler::rerun` returns a `RerunOutcome`: the run id
  submitted (`Submitted`), the run id of an active rerun (`Active`), or
  `NoRecord` or `NotReady` for a node that was not rerun. Match
  `RerunOutcome::Submitted` where `Some` was matched.
- **Breaking:** `Pools`, `PoolsBuilder` and `PoolRuntime` moved from
  `scheduler` to the `pools` module. The root re-export `swale::Pools` is
  unchanged.
- **Breaking:** `is_ready` moved to the `readiness` module, which has the
  rule when a node runs (`is_ready`, `node_states`) and when a graph run is
  finished (`settled_state`). Import it from `swale::readiness`.
- **Breaking:** `scheduler::Error` has the variants `Definition`,
  `UnknownGraph`, `NoPartition`, `ObjectStore` and `Contended`, and its
  `Record` variant and that of `status::Error` contain a `RecordError`. Add
  a wildcard arm to an exhaustive match. `Error::is_permanent` states
  whether a retry can change the outcome, and a worker dead-letters a job
  with a permanent error at once, a malformed record included.
- **Breaking:** `to_bytes` and `from_bytes` of every record and payload are
  the methods of the `JsonBytes` trait. Import `swale::JsonBytes` where they
  are called.
- **Breaking:** `RecordHook::new` takes the clock alone, and the hook
  enqueues on `EVENTS_QUEUE`. `Pools::names` is removed.
- **Breaking:** the memos of a pool are at `memos/{pool}` within the store
  prefix, as the definitions and the requests are at `definitions/` and
  `requests/`. 0.1.0 wrote them at `swale-memo-{pool}` next to the queue.
  Remove the objects at that path of an existing store: the daemon does not
  read them.
- Every pool keeps the memos and the run result record of a terminated task
  instance for `PoolsBuilder::memo_retention`, seven days by default. 0.1.0
  kept them without a bound.
- `swale queues <queue>` exits with status 1 for a queue the store does not
  have, and `StatusReader::dead_jobs` returns `None` for it. 0.1.0 printed
  an empty table.
- `swale publish` and `swale run` print an invalid definition as one
  `error:` line per problem, as `swale validate` does.
- An interrupted `swale run` waits for its workers and closes the store
  before it exits with status 130.
- **Breaking:** an operator has one `lease` field, an `operator::Lease` with
  `extension` and `interval`, which replaces the two fields `lease_extension`
  and `lease_interval`.
- The loader reports the faults of the file and the faults of the graph in
  one `Error::Invalid`. 0.1.0 reported the faults of the file alone when it
  had any.
- An invalid partition key argument of a command exits with status 2 and the
  parser's message, as an invalid argument does. 0.1.0 exited with status 1.

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
