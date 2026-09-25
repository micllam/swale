//! `swale daemon`.

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use swale::{Daemon, DaemonOptions, OperatorSet};

use super::CommandResult;
use super::store::{Runtime, StoreArg, create_store};

pub(crate) async fn daemon(
    store: StoreArg,
    concurrency: usize,
    pools: Vec<(String, usize)>,
    sync_interval: Duration,
    retention: Duration,
) -> CommandResult {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("SWALE_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,swale=info")),
        )
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .with_writer(std::io::stderr)
        .init();

    let store = create_store(store)?;
    let operators = Arc::new(OperatorSet::builtin());
    let definitions = store.definitions(operators.clone());
    let mut pool_sizes = BTreeMap::from([("default".to_string(), concurrency)]);
    pool_sizes.extend(pools);
    let requests = store.requests();
    let runtime = Runtime::open(store, operators, definitions, &pool_sizes).await?;
    let daemon = Daemon::new(
        runtime.queue.clone(),
        runtime.scheduler.clone(),
        runtime.pools.clone(),
        requests,
    );
    let result = daemon
        .run(
            DaemonOptions {
                sync_interval,
                retention,
                ..DaemonOptions::default()
            },
            async {
                let _ = tokio::signal::ctrl_c().await;
            },
        )
        .await;
    drop(daemon);
    runtime.close().await?;
    result?;
    Ok(ExitCode::SUCCESS)
}

/// Parses a `--pool` argument of the form `name=steps`.
pub(crate) fn parse_pool_arg(raw: &str) -> Result<(String, usize), String> {
    let invalid = || format!("`{raw}` is not `name=steps`, such as `warehouse=2`");
    let (name, steps) = raw.split_once('=').ok_or_else(invalid)?;
    let steps: usize = steps.parse().map_err(|_| invalid())?;
    if !swale::graph::is_name(name) || steps == 0 {
        return Err(invalid());
    }
    Ok((name.to_string(), steps))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pool_argument_is_a_name_and_a_positive_step_count() {
        assert_eq!(parse_pool_arg("warehouse=2"), Ok(("warehouse".into(), 2)));
        for raw in ["warehouse", "warehouse=0", "warehouse=x", "Bad=1", "=1"] {
            assert!(parse_pool_arg(raw).is_err(), "{raw}");
        }
    }
}
