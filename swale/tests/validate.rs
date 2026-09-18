use std::path::Path;
use std::process::Command;

use swale::{OperatorSet, Partitioning};

const EXAMPLE: &str = "examples/orders_daily.toml";

/// The output of the binary run with `args`, and `--store` at `store` when
/// given.
fn swale(args: &[&str], store: Option<&Path>) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_swale"));
    command.args(args);
    if let Some(store) = store {
        command.arg("--store").arg(store);
    }
    command.output().unwrap()
}

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
    let output = swale(&["validate", EXAMPLE], None);
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
    let output = swale(&["validate", path.to_str().unwrap()], None);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "error: node `a`: consumes `missing`, which no node produces\n\
         error: node `a`: unknown operator `sql`\n"
    );
}

#[test]
fn validate_command_reports_a_missing_file_with_status_1() {
    let output = swale(&["validate", "/nonexistent/graph.toml"], None);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .starts_with("error: cannot read the definition: ")
    );
}

#[test]
fn usage_error_has_status_2() {
    let output = swale(&[], None);
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

fn run_command(dir: &Path, file: &Path, extra: &[&str]) -> std::process::Output {
    let mut args = vec!["run", file.to_str().unwrap()];
    args.extend(extra);
    swale(&args, Some(&dir.join("store")))
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
    let output = swale(&["run", file.to_str().unwrap(), "--store", &url], None);
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
        let output = swale(&["run", file.to_str().unwrap(), "--store", store], None);
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
        swale(
            &["publish", file.to_str().unwrap()],
            Some(&dir.join("store")),
        )
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
    // The command prints one `error:` line per problem, as `validate` does.
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(
        stderr,
        "error: graph name `BAD` is not `[a-z0-9_]+`\nerror: the graph is empty\n"
    );
}

fn read_command(dir: &Path, store: &str, args: &[&str]) -> (Option<i32>, String) {
    let output = swale(args, Some(&dir.join(store)));
    let stdout = String::from_utf8(output.stdout).unwrap();
    (output.status.code(), stdout)
}

#[test]
fn status_and_queues_commands_print_a_failed_run_and_never_create_a_store() {
    let dir = scratch("swale-status");
    let file = dir.join("local.toml");
    std::fs::write(
        &file,
        runnable().replace("printf '{\\\"n\\\": 1}'", "exit 4"),
    )
    .unwrap();
    assert_eq!(run_command(&dir, &file, &[]).status.code(), Some(1));

    let (code, stdout) = read_command(&dir, "store", &["status"]);
    assert_eq!(code, Some(0), "{stdout}");
    assert_eq!(
        stdout,
        "GRAPH  DEFINITION  ACTIVE  COMPLETE  FAILED  CANCELLED  LATEST\n\
         local  -           0       0         1       0          none failed\n"
    );

    let (code, stdout) = read_command(&dir, "store", &["status", "local"]);
    assert_eq!(code, Some(0), "{stdout}");
    assert!(
        stdout.starts_with("PARTITION  STATE   REQUESTED"),
        "{stdout}"
    );
    assert!(stdout.contains("\nnone       failed  "), "{stdout}");

    let (code, stdout) = read_command(&dir, "store", &["status", "local", "none"]);
    assert_eq!(code, Some(0), "{stdout}");
    assert!(
        stdout.starts_with("local/none: failed, requested "),
        "{stdout}"
    );
    assert!(
        stdout.contains("\nfirst   default  failed   local-none-first-r0  "),
        "{stdout}"
    );
    assert!(stdout.contains("\nsecond  default  blocked  -"), "{stdout}");
    assert!(
        stdout.ends_with("\nfirst: `sh` exited with 4: \n"),
        "{stdout}"
    );

    let (code, stdout) = read_command(&dir, "store", &["queues"]);
    assert_eq!(code, Some(0), "{stdout}");
    assert!(
        stdout.starts_with("QUEUE               PENDING  "),
        "{stdout}"
    );
    let pool = stdout
        .lines()
        .find(|line| line.starts_with("swale-pool-default "))
        .unwrap();
    assert!(pool.ends_with("  1"), "{stdout}");

    let (code, stdout) = read_command(&dir, "store", &["queues", "swale-pool-default"]);
    assert_eq!(code, Some(0), "{stdout}");
    assert!(stdout.contains("  local-none-first-r0  1/1  "), "{stdout}");

    for args in [
        &["status", "absent"][..],
        &["status", "local", "20260915"],
        &["queues", "absent"],
    ] {
        let (code, stdout) = read_command(&dir, "store", args);
        assert_eq!(code, Some(1), "{stdout}");
    }
    let (code, _) = read_command(&dir, "absent", &["status"]);
    assert_eq!(code, Some(1));
    assert!(!dir.join("absent").exists());
}
