//! The commands on an object store URL, as CI runs them against an S3 API.
//!
//! Every test is ignored by default. `SWALE_TEST_STORE_URL` is a store URL
//! that the tests write to, such as `s3://swale/ci-1`, and the provider's
//! environment variables configure the store as they do for the command:
//!
//!     cargo test -p swale --features aws --test s3 -- --ignored
//!
//! The tests run the binary, so the daemon uses the wall clock. An assertion
//! depends on a count of graph runs and never on a partition key.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use swale::records::{GRAPH_RUNS_PREFIX, GraphRunRecord, GraphRunState};
use taquba::object_store::path::Path as ObjectPath;
use taquba::object_store::{ObjectStore, parse_url_opts};
use taquba::{QueueReader, ReaderOptions};

const LOCAL_EXAMPLE: &str = "examples/local.toml";

/// A graph whose catch-up window of two hours contains the firings of at
/// least two hourly partitions at every time of the day.
const CATCH_UP_GRAPH: &str = r#"
[graph]
name = "catch_up"
schedule = "*/10 * * * *"
catchup = "2h"
partition = "hourly"

[[node]]
name = "first"
produces = "catch_up_first"
operator = "subprocess"
[node.params]
argv = ["sh", "-c", "cat >/dev/null; printf '{}'"]
"#;

fn base_url() -> String {
    let url = std::env::var("SWALE_TEST_STORE_URL").expect("SWALE_TEST_STORE_URL is set");
    url.trim_end_matches('/').to_string()
}

/// The object store of `url` and the path of the URL within it, opened as the
/// command opens them.
fn open(url: &str) -> (Arc<dyn ObjectStore>, ObjectPath) {
    let options = std::env::vars().filter_map(|(name, value)| {
        let key = name.to_ascii_lowercase();
        key.starts_with("aws_").then_some((key, value))
    });
    let (store, path) = parse_url_opts(&url::Url::parse(url).unwrap(), options).unwrap();
    (Arc::from(store), path)
}

/// The names of the entries directly within `path`. An object directly
/// within `path` fails the test.
async fn children(store: &Arc<dyn ObjectStore>, path: Option<&ObjectPath>) -> Vec<String> {
    let listing = store.list_with_delimiter(path).await.unwrap();
    assert!(listing.objects.is_empty(), "{:?}", listing.objects);
    let mut names: Vec<String> = listing
        .common_prefixes
        .iter()
        .map(|p| p.filename().unwrap().to_string())
        .collect();
    names.sort();
    names
}

fn swale(args: &[&str], store_url: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_swale"));
    command.args(args).arg("--store").arg(store_url);
    command
}

/// A child process that is killed when the test ends early.
struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
#[ignore = "needs SWALE_TEST_STORE_URL"]
async fn run_command_completes_and_resumes_on_the_url_and_every_object_is_within_its_prefix() {
    let base = base_url();
    let url = format!("{base}/run");

    let output = swale(&["run", LOCAL_EXAMPLE], &url).output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "{stdout}\n{stderr}");
    assert!(stdout.contains("local/none: started"), "{stdout}");
    assert!(stdout.contains("local/none: complete"), "{stdout}");

    let output = swale(&["run", LOCAL_EXAMPLE], &url).output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(output.status.code(), Some(0), "{stdout}");
    assert!(stdout.contains("local/none: run exists"), "{stdout}");

    // The definitions, the queue and the memos of the pool are within the
    // prefix of the URL.
    let (store, prefix) = open(&url);
    assert_eq!(
        children(&store, Some(&prefix)).await,
        ["definitions", "swale", "swale-memo-default"]
    );
    // Every object that the tests of this file write is within the base
    // URL. The root of a `file` URL is the root of the file system, which
    // the test does not list.
    let (_, base_path) = open(&base);
    for name in children(&store, Some(&base_path)).await {
        assert!(["run", "daemon"].contains(&name.as_str()), "{name}");
    }
    if !base.starts_with("file:") {
        let first_segment = base_path.parts().next().unwrap();
        assert_eq!(children(&store, None).await, [first_segment.as_ref()]);
    }
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "needs SWALE_TEST_STORE_URL"]
async fn daemon_adopts_a_published_graph_and_catches_up_on_the_url() {
    let url = format!("{}/daemon", base_url());
    let file = Path::new(env!("CARGO_TARGET_TMPDIR")).join("swale-s3-catch-up.toml");
    std::fs::write(&file, CATCH_UP_GRAPH).unwrap();

    let output = swale(&["publish", file.to_str().unwrap()], &url)
        .output()
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(output.status.code(), Some(0), "{stdout}");
    assert!(stdout.starts_with("catch_up: published "), "{stdout}");

    let daemon = Process(
        swale(&["daemon", "--sync-interval", "1"], &url)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );

    // The reader does not fence the daemon. The queue exists once the daemon
    // opened the store.
    let (store, prefix) = open(&url);
    let queue_path = format!("{prefix}/swale");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let mut complete = 0;
    while complete < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{complete} graph run(s) completed"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
        let options = ReaderOptions {
            manifest_poll_interval: Duration::from_secs(1),
            ..ReaderOptions::default()
        };
        let Ok(reader) = QueueReader::open_with_options(store.clone(), &queue_path, options).await
        else {
            continue;
        };
        let prefix = format!("{GRAPH_RUNS_PREFIX}catch_up/");
        let page = reader.kv_scan(prefix.as_bytes(), None, 100).await.unwrap();
        complete = page
            .entries
            .iter()
            .filter(|(_, value)| {
                GraphRunRecord::from_bytes(value).unwrap().state == GraphRunState::Complete
            })
            .count();
        reader.close().await.unwrap();
    }

    // An interrupt stops the daemon with status 0.
    let mut daemon = daemon;
    let pid = daemon.0.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-INT", &pid])
            .status()
            .unwrap()
            .success()
    );
    let status = daemon.0.wait().unwrap();
    assert_eq!(status.code(), Some(0));
}
