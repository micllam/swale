//! `swale status`.

use swale::{Partition, StatusReader};

use super::CommandResult;
use super::store::{StoreArg, with_reader};
use super::table::{format_time, print_table, short_hash};

pub(crate) async fn status(
    store: StoreArg,
    graph: Option<String>,
    partition: Option<Partition>,
    from: Option<Partition>,
    limit: usize,
) -> CommandResult {
    with_reader(store, async |reader| {
        print_status(
            reader,
            graph.as_deref(),
            partition.as_ref(),
            from.as_ref(),
            limit,
        )
        .await
    })
    .await
}

async fn print_status(
    reader: &StatusReader,
    graph: Option<&str>,
    partition: Option<&Partition>,
    from: Option<&Partition>,
    limit: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(graph) = graph else {
        let header = [
            "GRAPH",
            "DEFINITION",
            "ACTIVE",
            "COMPLETE",
            "FAILED",
            "CANCELLED",
            "LATEST",
        ];
        let mut rows = Vec::new();
        for graph in reader.graphs().await? {
            rows.push([
                graph.name,
                graph
                    .adopted
                    .map_or("-".to_string(), |record| short_hash(&record.definition)),
                graph.runs.active.to_string(),
                graph.runs.complete.to_string(),
                graph.runs.failed.to_string(),
                graph.runs.cancelled.to_string(),
                graph.latest.map_or("-".to_string(), |run| {
                    format!("{} {}", run.partition, run.record.state)
                }),
            ]);
        }
        print_table(header, &rows);
        return Ok(());
    };
    let Some(partition) = partition else {
        let runs = reader.runs(graph, from).await?;
        if runs.is_empty() {
            return Err(match from {
                Some(from) => {
                    format!("graph `{graph}` does not have a graph run at or after `{from}`")
                }
                None => format!("graph `{graph}` does not have a graph run"),
            }
            .into());
        }
        let header = ["PARTITION", "STATE", "REQUESTED", "DEFINITION"];
        let mut rows = Vec::new();
        for run in &runs[runs.len().saturating_sub(limit)..] {
            rows.push([
                run.partition.to_string(),
                run.record.state.to_string(),
                format_time(run.record.requested_at_ms),
                short_hash(&run.record.definition),
            ]);
        }
        print_table(header, &rows);
        return Ok(());
    };
    let Some(run) = reader.run(graph, partition).await? else {
        return Err(
            format!("graph `{graph}` does not have a run for partition `{partition}`").into(),
        );
    };
    println!(
        "{graph}/{partition}: {}, requested {}, definition {}",
        run.record.state,
        format_time(run.record.requested_at_ms),
        short_hash(&run.record.definition)
    );
    let header = ["NODE", "POOL", "STATE", "INSTANCE", "RUN", "TERMINATED"];
    let mut rows = Vec::new();
    for node in &run.nodes {
        let record = node.record.as_ref();
        let instance = node.instance.as_ref();
        rows.push([
            node.name.clone(),
            node.pool.clone(),
            node.state.to_string(),
            instance.map_or("-".to_string(), |i| i.state.to_string()),
            instance
                .map(|i| i.run_id.clone())
                .unwrap_or_else(|| record.map_or("-".to_string(), |r| r.run_id.clone())),
            record.map_or("-".to_string(), |r| format_time(r.terminated_at_ms)),
        ]);
    }
    print_table(header, &rows);
    for node in &run.nodes {
        if let Some(error) = node.record.as_ref().and_then(|r| r.error.as_ref()) {
            println!("{}: {error}", node.name);
        }
    }
    Ok(())
}
