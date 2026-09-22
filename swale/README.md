# swale

[![crates.io](https://img.shields.io/crates/v/swale.svg)](https://crates.io/crates/swale)
[![docs.rs](https://img.shields.io/docsrs/swale)](https://docs.rs/swale)
[![license](https://img.shields.io/crates/l/swale.svg)](#license)

A scheduled, dependency-ordered orchestrator of asset graphs.

swale runs on the [taquba](https://github.com/micllam/taquba) durable
execution crates. Every task instance runs as a single workflow run, and the
record of its outcome and the event for the scheduler commit in one transaction.

## Definition file

One TOML file per graph. An asset node declares the asset it produces and the
assets it consumes, and a task node declares its upstream nodes with `after`.

```toml
[graph]
name = "orders_daily"
schedule = "0 2 * * *"
partition = "daily"

[[node]]
name = "extract"
produces = "orders_raw"
operator = "shell"
[node.params]
command = "dbt run --select orders_raw >&2 && printf '{\"table\": \"orders_raw\"}'"

[[node]]
name = "load"
produces = "orders_warehouse"
consumes = ["orders_raw"]
operator = "http"
[node.params]
method = "POST"
url = "https://warehouse.example/load"
body = '{"table": "{{ upstream.extract.table }}"}'
```

The [`definition` module](https://docs.rs/swale/latest/swale/definition/)
documents every field, and the loader reports every fault of a definition at
once. From Rust, `load_path` returns the checked graph:

```rust
use std::path::Path;
use swale::OperatorSet;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let graph = swale::load_path(
        Path::new("examples/orders_daily.toml"),
        &OperatorSet::builtin(),
    )?;
    println!("{}: {} nodes", graph.name(), graph.nodes().len());
    Ok(())
}
```

## Operators

An operator runs a node, and its parameters are the `params` table of the
node. A parameter string refers to `{{ partition }}`, `{{ run.<field> }}`
(`id` or `summary`), `{{ upstream.<node>.<path> }}` and `{{ env.<NAME> }}`.
The upstream reference is a path into the JSON output of an upstream node,
and the env reference is an environment variable of the daemon.

- **`subprocess`** runs `argv` with a JSON document on stdin (the identity,
  the parameters and the upstream outputs) and reads the output from stdout.
- **`shell`** runs `command` with `sh -c`, with the identity in `SWALE_*`
  variables, the upstream outputs in `SWALE_INPUTS` and the `env` table in
  the environment, and reads the output from stdout.
- **`http`** sends `method`, `url`, `headers` and `body` within `timeout`
  and outputs `{"status": <code>, "body": <value>}`, with a JSON body
  parsed.

Exit code 0 and a 2xx status are success. Exit code 75, a 5xx status, a 429
status, a connection failure and a timeout are transient errors, retried up
to `retries` times. Any other exit code or status is a permanent error,
which dead-letters the task instance. A program or a request must be
idempotent per attempt.

## Schedule

The daemon fires the `schedule` of a graph, a cron expression with five fields
in UTC. The partition of a firing is the start of the schedule interval that
ends at the firing time: a daily schedule at 02:00 that fires on 2026-09-16
runs the partition `20260915`. A graph with a schedule declares `partition` as
`daily` or `hourly`.

The first adoption of a graph runs the partitions of the firings within its
`catchup` window. After downtime the daemon replays the missed firings within
the same window.

## Command

The `swale` binary is enabled by the default `cli` feature. A program that
embeds the library can set `default-features = false`.

`swale validate` loads a definition and prints each fault, one per line.

`swale run` runs a graph for one partition on a store, prints each node's
record as it is written and waits for the run to settle. A second `run` for
the same partition resumes the existing graph run, and a process interrupted
mid-run resumes without repeating a completed node.

`swale publish` writes a definition to the store as the current definition of
its graph. `swale daemon` runs the published graphs until it is interrupted.
It adopts each published definition within one sync interval, fires each
schedule and runs the task instances. A graph run keeps the definition it
started from, so an edit applies from the next graph run. The `default` pool
always exists, and `--pool name=steps` adds a pool.

Every command that opens a store takes `--store`: a directory or an object
store URL (`s3://bucket/prefix`, `gs://bucket/prefix`,
`az://container/prefix`). A cloud scheme needs the matching cargo feature
(`aws`, `gcp` or `azure`) and reads the provider's environment variables for
its credentials. The default store is `~/.swale/store`.

`swale run` and `swale daemon` open the store as its only writer. A `swale run`
on the store of a running daemon opens a second writer, and the store then
refuses the writes of the daemon.

`swale status` lists the graphs of a store with the count of their graph runs
in each state. `swale status <graph>` lists the latest graph runs of the graph,
and `swale status <graph> <partition>` lists the nodes of one graph run. A node
without a record is `ready`, `waiting` or `blocked`. A blocked node does not
run until a rerun changes the record of an upstream.

`swale queues` lists the job counts of every queue, and `swale queues <queue>`
lists the dead jobs of one queue. Both commands only read from the store, so
they can run alongside a daemon.

`swale start <graph> <partition>...` starts the graph run of each partition,
`swale rerun <graph> <partition> <node>` runs a node again, and
`swale cancel <graph> <partition>` cancels an active graph run. Each command
writes a request to the store and does not open the queue, and the daemon
applies the request at its next sync pass. With `--wait` the command waits
for the outcome and prints it.

A rerun of a failed or cancelled node runs the node again, and its downstream
nodes follow as their trigger rules allow. A rerun of a succeeded node runs
the node again and, after it, every node downstream of it through an asset
edge or an all-succeeded edge, each with the new outputs. A task node with
the all-done or one-failed rule keeps its record. Until such a node runs
again, the status view shows it as ready, waiting or blocked with its last
record.

```console
$ swale validate examples/orders_daily.toml
orders_daily: 5 nodes, 3 edges
$ swale run examples/local.toml --store ./swale-store
local/none: started, 1 root node(s) submitted
  first: succeeded (local-none-first-r0)
  second: succeeded (local-none-second-r0)
local/none: complete
$ swale status local none --store ./swale-store
local/none: complete, requested 2026-09-17T13:48:19Z, definition 9450f82715de
NODE    POOL     STATE      RUN                   TERMINATED
first   default  succeeded  local-none-first-r0   2026-09-17T13:48:19Z
second  default  succeeded  local-none-second-r0  2026-09-17T13:48:20Z
$ swale publish examples/orders_daily.toml --store s3://bucket/swale
orders_daily: published 36e831ff15e026ad45115374cb98edec6b617dffb0d4ef40f9fd351ea36bca9a
$ swale daemon --store s3://bucket/swale --pool warehouse=2
$ swale rerun orders_daily 20260915 transform --store s3://bucket/swale --wait
request 01K5ARRE8ZW9K8XTJTQ6PVYJK7: submitted orders_daily-20260915-transform-r1
```

A command exits with status 0, 1 or 2:

- **0.** The command succeeded. An interrupt ends `swale daemon` with this
  status.
- **1.** A definition has a fault, a graph run failed, a request was refused
  or another error occurred.
- **2.** The arguments are not valid.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
