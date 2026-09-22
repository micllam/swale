//! `swale publish`.

use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use swale::OperatorSet;

use super::CommandResult;
use super::store::{StoreArg, create_store};

pub(crate) async fn publish(file: &Path, store: StoreArg) -> CommandResult {
    let text = std::fs::read_to_string(file)?;
    let store = create_store(store)?;
    let definitions = store.definitions(Arc::new(OperatorSet::builtin()));
    let published = definitions.publish(&text).await?;
    let state = if published.changed {
        "published"
    } else {
        "unchanged"
    };
    println!("{}: {state} {}", published.graph.name(), published.hash);
    Ok(ExitCode::SUCCESS)
}
