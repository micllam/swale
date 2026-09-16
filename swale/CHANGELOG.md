# Changelog

All notable changes to the `swale` crate will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
