use std::path::Path;
use std::process::Command;

use swale::{OperatorSet, Partitioning};

const EXAMPLE: &str = "examples/orders_daily.toml";

/// An empty directory under cargo's target tmp dir, left in place after the
/// test for inspection.
fn scratch(name: &str) -> std::path::PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn example_graph_loads_with_the_documented_structure() {
    let graph = swale::load_path(Path::new(EXAMPLE), &OperatorSet::builtin()).unwrap();
    assert_eq!(graph.name(), "orders_daily");
    assert_eq!(graph.schedule().unwrap().to_string(), "0 2 * * *");
    assert_eq!(
        graph.catchup(),
        Some(std::time::Duration::from_secs(7 * 86_400))
    );
    assert_eq!(graph.partitioning(), Partitioning::Daily);
    let order: Vec<&str> = graph.nodes().iter().map(|n| n.name()).collect();
    assert_eq!(
        order,
        ["extract", "transform", "load", "partition_ready", "notify"]
    );
    let roots: Vec<&str> = graph.roots().map(|n| n.name()).collect();
    assert_eq!(roots, ["extract", "partition_ready"]);
    let leaves: Vec<&str> = graph.leaves().map(|n| n.name()).collect();
    assert_eq!(leaves, ["partition_ready", "notify"]);
    assert_eq!(graph.edge_count(), 3);
    assert_eq!(graph.node("load").unwrap().pool(), "warehouse");
    assert_eq!(graph.node("load").unwrap().retries(), 5);
}

#[test]
fn validate_command_reports_a_valid_definition_with_status_0() {
    let output = Command::new(env!("CARGO_BIN_EXE_swale"))
        .args(["validate", EXAMPLE])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "orders_daily: 5 nodes, 3 edges\n"
    );
}

#[test]
fn validate_command_prints_each_fault_with_status_1() {
    let dir = scratch("swale-validate");
    let path = dir.join("bad.toml");
    std::fs::write(
        &path,
        r#"
[graph]
name = "g"
[[node]]
name = "a"
produces = "a"
consumes = ["missing"]
operator = "sql"
"#,
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_swale"))
        .args(["validate", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "error: node `a`: consumes `missing`, which no node produces\n\
         error: node `a`: unknown operator `sql`\n"
    );
}

#[test]
fn validate_command_reports_a_missing_file_with_status_1() {
    let output = Command::new(env!("CARGO_BIN_EXE_swale"))
        .args(["validate", "/nonexistent/graph.toml"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .starts_with("error: cannot read the definition: ")
    );
}

#[test]
fn usage_error_has_status_2() {
    let output = Command::new(env!("CARGO_BIN_EXE_swale")).output().unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("Usage: swale")
    );
}

const LOCAL_EXAMPLE: &str = "examples/local.toml";

fn runnable() -> String {
    std::fs::read_to_string(LOCAL_EXAMPLE).unwrap()
}

fn run_command(
    dir: &std::path::Path,
    file: &std::path::Path,
    extra: &[&str],
) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_swale"))
        .arg("run")
        .arg(file)
        .arg("--store")
        .arg(dir.join("store"))
        .args(extra)
        .output()
        .unwrap()
}

#[test]
fn run_command_completes_a_graph_on_a_directory_store_and_resumes_an_existing_run() {
    let dir = scratch("swale-run");
    let file = dir.join("local.toml");
    std::fs::write(&file, runnable()).unwrap();

    let output = run_command(&dir, &file, &[]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.starts_with("local/none: started, 1 root node(s) submitted\n"),
        "{stdout}"
    );
    assert!(
        stdout.contains("  first: succeeded (local-none-first-r0)\n"),
        "{stdout}"
    );
    assert!(
        stdout.contains("  second: succeeded (local-none-second-r0)\n"),
        "{stdout}"
    );
    assert!(stdout.ends_with("local/none: complete\n"), "{stdout}");

    let output = run_command(&dir, &file, &[]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(output.status.code(), Some(0), "{stdout}");
    assert!(
        stdout.starts_with("local/none: run exists, resuming\n"),
        "{stdout}"
    );
    assert!(stdout.ends_with("local/none: complete\n"), "{stdout}");
}

#[test]
fn run_command_accepts_a_file_url_and_refuses_an_unknown_or_unbuilt_scheme() {
    let dir = scratch("swale-run-url");
    let file = dir.join("local.toml");
    std::fs::write(&file, runnable()).unwrap();
    let store = dir.join("store");
    std::fs::create_dir_all(&store).unwrap();
    let url = format!("file://{}", store.display());
    let output = Command::new(env!("CARGO_BIN_EXE_swale"))
        .args(["run", file.to_str().unwrap(), "--store", &url])
        .output()
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.ends_with("local/none: complete\n"), "{stdout}");

    for (store, message) in [
        ("ftp://bucket/prefix", "scheme `ftp` is not one of"),
        ("s3://bucket/prefix", "needs a build with the `aws` feature"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_swale"))
            .args(["run", file.to_str().unwrap(), "--store", store])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{store}");
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains(message), "{store}: {stderr}");
    }
}

#[test]
fn run_command_reports_a_failed_run_with_status_1() {
    let dir = scratch("swale-run-fail");
    let file = dir.join("local.toml");
    std::fs::write(
        &file,
        runnable().replace("printf '{\\\"n\\\": 1}'", "echo no >&2; exit 4"),
    )
    .unwrap();

    let output = run_command(&dir, &file, &[]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(output.status.code(), Some(1), "{stdout}");
    assert!(
        stdout.contains("  first: failed (local-none-first-r0)\n    `sh` exited with 4: no\n"),
        "{stdout}"
    );
    assert!(stdout.ends_with("local/none: failed\n"), "{stdout}");
}

#[test]
fn run_command_requires_a_partition_for_a_partitioned_graph() {
    let dir = scratch("swale-run-part");
    let file = dir.join("daily.toml");
    std::fs::write(
        &file,
        runnable().replace(
            "name = \"local\"",
            "name = \"local\"\npartition = \"daily\"",
        ),
    )
    .unwrap();

    let output = run_command(&dir, &file, &[]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "error: the graph is partitioned: pass --partition <key>\n"
    );
    let output = run_command(&dir, &file, &["--partition", "20260915"]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(output.status.code(), Some(0), "{stdout}");
    assert!(stdout.contains("(local-20260915-first-r0)"), "{stdout}");
}

#[test]
fn publish_command_writes_the_definition_once_and_reports_a_fault_with_status_1() {
    let dir = scratch("swale-publish");
    let publish = |file: &Path| {
        Command::new(env!("CARGO_BIN_EXE_swale"))
            .arg("publish")
            .arg(file)
            .arg("--store")
            .arg(dir.join("store"))
            .output()
            .unwrap()
    };
    let hash = swale::definition::hash(&std::fs::read_to_string(EXAMPLE).unwrap());

    let output = publish(Path::new(EXAMPLE));
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("orders_daily: published {hash}\n")
    );
    let definitions = dir.join("store/definitions");
    assert!(definitions.join(format!("{hash}.toml")).is_file());
    assert_eq!(
        std::fs::read_to_string(definitions.join("current/orders_daily")).unwrap(),
        hash
    );

    let output = publish(Path::new(EXAMPLE));
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("orders_daily: unchanged {hash}\n")
    );

    let bad = dir.join("bad.toml");
    std::fs::write(&bad, "[graph]\nname = \"BAD\"\n").unwrap();
    let output = publish(&bad);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("graph name `BAD`")
    );
}
