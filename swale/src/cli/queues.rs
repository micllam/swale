//! `swale queues`.

use swale::StatusReader;

use super::CommandResult;
use super::store::{StoreArg, with_reader};
use super::table::{format_time, print_table};

pub(crate) async fn queues(store: StoreArg, queue: Option<String>, limit: usize) -> CommandResult {
    with_reader(store, async |reader| {
        print_queues(reader, queue.as_deref(), limit).await
    })
    .await
}

async fn print_queues(
    reader: &StatusReader,
    queue: Option<&str>,
    limit: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(queue) = queue else {
        let header = ["QUEUE", "PENDING", "SCHEDULED", "CLAIMED", "DONE", "DEAD"];
        let mut rows = Vec::new();
        for stats in reader.queues().await? {
            rows.push([
                stats.queue,
                stats.pending.to_string(),
                stats.scheduled.to_string(),
                stats.claimed.to_string(),
                stats.done.to_string(),
                stats.dead.to_string(),
            ]);
        }
        print_table(header, &rows);
        return Ok(());
    };
    let header = ["JOB", "RUN", "ATTEMPTS", "ENQUEUED"];
    let mut rows = Vec::new();
    let Some(jobs) = reader.dead_jobs(queue, limit).await? else {
        return Err(format!("queue `{queue}` does not exist").into());
    };
    for job in &jobs {
        rows.push([
            job.id.clone(),
            job.headers
                .get(taquba_workflow::HEADER_RUN_ID)
                .cloned()
                .unwrap_or_else(|| "-".to_string()),
            format!("{}/{}", job.attempts, job.max_attempts),
            format_time(job.enqueued_at),
        ]);
    }
    print_table(header, &rows);
    for job in &jobs {
        if let Some(error) = &job.last_error {
            println!("{}: {error}", job.id);
        }
    }
    Ok(())
}
