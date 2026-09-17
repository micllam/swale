# swale

[![crates.io](https://img.shields.io/crates/v/swale.svg)](https://crates.io/crates/swale)
[![docs.rs](https://img.shields.io/docsrs/swale)](https://docs.rs/swale)
[![CI](https://github.com/micllam/swale/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/micllam/swale/actions/workflows/ci.yml)
[![license](https://img.shields.io/crates/l/swale.svg)](#license)

A scheduled, dependency-ordered orchestrator of asset graphs. The deployment
consists of a single binary and a single bucket.

swale runs on the [taquba](https://github.com/micllam/taquba) durable
execution crates. Every task instance runs as a single workflow run, and the
record of its outcome and the event for the scheduler commit in one transaction.
The [`swale`](swale/) crate is the library and the binary, and its README
documents the API and the command in full.

## Definition file

One TOML file per graph. An asset node declares the asset it produces and the
assets it consumes, and a task node declares its upstream nodes with `after`.
Every node runs an operator with parameters, and a parameter string can refer
to the partition or to the output of an upstream node.

```toml
[graph]
name = "orders_daily"
schedule = "0 2 * * *"
partition = "daily"

[[node]]
name = "extract"
produces = "orders_raw"
operator = "subprocess"
[node.params]
argv = ["python", "tasks/extract.py"]

[[node]]
name = "load"
produces = "orders_warehouse"
consumes = ["orders_raw"]
operator = "subprocess"
[node.params]
argv = ["python", "tasks/load.py", "{{ upstream.extract.rows_key }}"]
```

## Command

`swale validate` checks a definition and reports every fault at once.
`swale run` runs a graph for one partition on a store, a directory or an object
store URL (`s3://`, `gs://`, `az://`), and resumes an existing graph run.

`swale publish` writes a definition to the store, and `swale daemon` runs the
published graphs: it fires each schedule, catches up a new graph and replays
the firings missed during downtime.

```console
$ cargo install swale
$ swale validate swale/examples/orders_daily.toml
orders_daily: 5 nodes, 3 edges
$ swale run swale/examples/local.toml --store ./swale-store
local/none: started, 1 root node(s) submitted
  first: succeeded (local-none-first-r0)
  second: succeeded (local-none-second-r0)
local/none: complete
$ swale publish swale/examples/orders_daily.toml --store s3://bucket/swale
orders_daily: published 36e831ff15e026ad45115374cb98edec6b617dffb0d4ef40f9fd351ea36bca9a
$ swale daemon --store s3://bucket/swale --pool warehouse=2
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
