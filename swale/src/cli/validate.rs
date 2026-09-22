//! `swale validate`.

use std::path::Path;
use std::process::ExitCode;

use swale::OperatorSet;

use super::CommandResult;

pub(crate) fn validate(path: &Path) -> CommandResult {
    let graph = swale::load_path(path, &OperatorSet::builtin())?;
    println!(
        "{}: {} nodes, {} edges",
        graph.name(),
        graph.nodes().len(),
        graph.edge_count()
    );
    Ok(ExitCode::SUCCESS)
}
