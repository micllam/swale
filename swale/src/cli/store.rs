//! The store of a command: the `--store` argument, the open object store
//! and the queue, the pools and the scheduler of a process over it.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use swale::store::store_path;
use swale::{
    DefinitionStore, OperatorSet, Pools, RecordHook, RequestStore, Scheduler, StatusReader,
};
use taquba::Queue;
use taquba::object_store::local::LocalFileSystem;
use taquba::object_store::path::Path as ObjectPath;
use taquba::object_store::{ObjectStore, parse_url_opts};

use super::CommandResult;

/// The SlateDB path of the queue within the store.
const QUEUE_PATH: &str = "swale";

#[derive(clap::Args)]
pub(crate) struct StoreArg {
    /// The store: a directory, created when absent, or an object store
    /// URL (`s3://bucket/prefix`, `gs://bucket/prefix`,
    /// `az://container/prefix`, `file:///path`). A cloud scheme needs
    /// the matching cargo feature and reads the provider's environment
    /// variables for its credentials. The default is `~/.swale/store`.
    #[arg(long, value_parser = parse_store_arg)]
    pub(crate) store: Option<String>,
}

/// Checks the scheme of a `--store` URL at argument parsing. A bare path
/// passes through.
fn parse_store_arg(raw: &str) -> Result<String, String> {
    let is_url = raw.contains("://") && raw.chars().next().is_some_and(|c| c.is_ascii_alphabetic());
    if !is_url {
        return Ok(raw.to_string());
    }
    let url = url::Url::parse(raw).map_err(|e| format!("`{raw}` is not a URL: {e}"))?;
    let feature = match url.scheme() {
        "file" => None,
        "s3" => Some(("aws", cfg!(feature = "aws"))),
        "gs" => Some(("gcp", cfg!(feature = "gcp"))),
        "az" | "abfs" | "abfss" => Some(("azure", cfg!(feature = "azure"))),
        other => {
            return Err(format!(
                "scheme `{other}` is not one of s3, gs, az, abfs, abfss or file"
            ));
        }
    };
    if let Some((feature, enabled)) = feature
        && !enabled
    {
        return Err(format!(
            "scheme `{}` needs a build with the `{feature}` feature",
            url.scheme()
        ));
    }
    Ok(raw.to_string())
}

/// The default store directory: `.swale/store` in `home`, the result of
/// [`std::env::home_dir`]. On Unix that result is empty for an empty `HOME`.
fn default_store(home: Option<PathBuf>) -> Option<PathBuf> {
    let home = home.filter(|dir| !dir.as_os_str().is_empty())?;
    Some(home.join(".swale").join("store"))
}

/// The object store options among the environment variables `vars`: the
/// variables of the three providers, with the name in lower case as the
/// option key. The prefixes exclude a variable of another program whose lower
/// case name is an option key, such as `TOKEN` or `ENDPOINT`.
fn store_options(
    vars: impl Iterator<Item = (String, String)>,
) -> impl Iterator<Item = (String, String)> {
    vars.filter_map(|(name, value)| {
        let key = name.to_ascii_lowercase();
        ["aws_", "google_", "azure_"]
            .iter()
            .any(|prefix| key.starts_with(prefix))
            .then_some((key, value))
    })
}

/// An open store: the object store, the store prefix and the path of the
/// queue within the store.
pub(crate) struct Store {
    pub(crate) objects: Arc<dyn ObjectStore>,
    pub(crate) prefix: String,
    pub(crate) queue_path: String,
}

impl Store {
    /// The definition store, checking definitions against `operators`.
    pub(crate) fn definitions(&self, operators: Arc<OperatorSet>) -> Arc<DefinitionStore> {
        Arc::new(DefinitionStore::new(
            self.objects.clone(),
            &self.prefix,
            operators,
        ))
    }

    /// The request store.
    pub(crate) fn requests(&self) -> RequestStore {
        RequestStore::new(self.objects.clone(), &self.prefix)
    }
}

/// The value of `--store`, or the default store directory.
fn store_location(arg: StoreArg) -> Result<String, Box<dyn std::error::Error>> {
    match arg.store {
        Some(raw) => Ok(raw),
        None => Ok(default_store(std::env::home_dir())
            .ok_or("the default store needs a home directory: pass --store <dir or URL>")?
            .to_string_lossy()
            .into_owned()),
    }
}

/// Opens the store of a command that writes to it, and creates the
/// directory of a directory store first.
pub(crate) fn create_store(arg: StoreArg) -> Result<Store, Box<dyn std::error::Error>> {
    let location = store_location(arg)?;
    if !location.contains("://") {
        std::fs::create_dir_all(&location)?;
    }
    open_location(&location)
}

/// Opens the store of `--store`, or the default store.
pub(crate) fn open_store(arg: StoreArg) -> Result<Store, Box<dyn std::error::Error>> {
    open_location(&store_location(arg)?)
}

/// Opens the store at `location`: a directory, or an object store URL with
/// the path in the URL as the store prefix.
fn open_location(location: &str) -> Result<Store, Box<dyn std::error::Error>> {
    let (objects, prefix): (Arc<dyn ObjectStore>, ObjectPath) = if location.contains("://") {
        let url = url::Url::parse(location)?;
        let (store, prefix) = parse_url_opts(&url, store_options(std::env::vars()))?;
        (Arc::from(store), prefix)
    } else {
        (
            Arc::new(LocalFileSystem::new_with_prefix(location)?),
            ObjectPath::default(),
        )
    };
    let prefix = prefix.as_ref().to_string();
    let queue_path = store_path(&prefix, QUEUE_PATH);
    Ok(Store {
        objects,
        prefix,
        queue_path,
    })
}

/// The queue, the pools and the scheduler of a process over `store`.
pub(crate) struct Runtime {
    pub(crate) queue: Arc<Queue>,
    pub(crate) pools: Arc<Pools>,
    pub(crate) scheduler: Arc<Scheduler>,
}

impl Runtime {
    pub(crate) async fn open(
        store: Store,
        operators: Arc<OperatorSet>,
        definitions: Arc<DefinitionStore>,
        pool_sizes: &BTreeMap<String, usize>,
    ) -> Result<Runtime, Box<dyn std::error::Error>> {
        let queue = Arc::new(Queue::open(store.objects.clone(), &store.queue_path).await?);
        let hook = RecordHook::new(queue.clock());
        let mut pools = Pools::builder(queue.clone(), store.objects, operators, hook)
            .poll_interval(Duration::from_millis(100))
            .store_prefix(store.prefix);
        for (name, steps) in pool_sizes {
            pools = pools.pool(name, *steps);
        }
        let pools = Arc::new(pools.build());
        let scheduler = Arc::new(Scheduler::new(queue.clone(), definitions, pools.clone()));
        Ok(Runtime {
            queue,
            pools,
            scheduler,
        })
    }

    pub(crate) async fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        drop(self.scheduler);
        drop(self.pools);
        if let Ok(queue) = Arc::try_unwrap(self.queue) {
            queue.close().await?;
        }
        Ok(())
    }
}

/// Opens the status reader of `--store` with the built-in operators.
async fn open_status(store: StoreArg) -> Result<StatusReader, Box<dyn std::error::Error>> {
    let store = open_store(store)?;
    Ok(StatusReader::open(
        store.objects.clone(),
        &store.queue_path,
        store.definitions(Arc::new(OperatorSet::builtin())),
    )
    .await?)
}

/// Opens the status reader of `store`, runs `print` on it and closes it.
pub(crate) async fn with_reader(
    store: StoreArg,
    print: impl AsyncFnOnce(&StatusReader) -> Result<(), Box<dyn std::error::Error>>,
) -> CommandResult {
    let reader = open_status(store).await?;
    let result = print(&reader).await;
    reader.close().await?;
    result?;
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn the_default_store_is_swale_store_in_the_home_directory() {
        assert_eq!(
            default_store(Some(PathBuf::from("/home/u"))),
            Some(Path::new("/home/u").join(".swale").join("store"))
        );
        assert_eq!(default_store(Some(PathBuf::new())), None);
        assert_eq!(default_store(None), None);
    }

    #[test]
    fn the_store_options_are_the_provider_variables_in_lower_case() {
        let vars = [
            ("AWS_ENDPOINT", "http://127.0.0.1:9000"),
            ("ENDPOINT", "other"),
            ("GOOGLE_SERVICE_ACCOUNT", "sa.json"),
            ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
            ("HOME", "/home/u"),
        ]
        .map(|(name, value)| (name.to_string(), value.to_string()));
        let options: Vec<(String, String)> = store_options(vars.into_iter()).collect();
        assert_eq!(
            options,
            [
                ("aws_endpoint", "http://127.0.0.1:9000"),
                ("google_service_account", "sa.json"),
                ("azure_storage_account_name", "account"),
            ]
            .map(|(name, value)| (name.to_string(), value.to_string()))
        );
    }
}
