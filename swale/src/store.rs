//! The layout of the object store: every path the process writes is within
//! the store prefix of the `--store` URL. [`store_path`] joins the prefix the
//! one way, and an [`ObjectPrefix`] is the objects within one such path.

use std::ffi::OsString;
use std::sync::Arc;

use taquba::object_store::path::Path as ObjectPath;
use taquba::object_store::{self, ObjectMeta, ObjectStore, ObjectStoreExt};

/// The path `name` within `store_prefix`. An empty prefix gives `name` alone.
pub fn store_path(store_prefix: &str, name: &str) -> String {
    if store_prefix.is_empty() {
        name.to_string()
    } else {
        format!("{store_prefix}/{name}")
    }
}

/// The object store options among the environment variables `vars`: the
/// variables of the three providers, with the name in lower case as the
/// option key. The prefixes exclude a variable of another program whose lower
/// case name is an option key, such as `TOKEN` or `ENDPOINT`. A variable
/// whose name or value is not UTF-8 is omitted.
pub fn provider_options(
    vars: impl Iterator<Item = (OsString, OsString)>,
) -> impl Iterator<Item = (String, String)> {
    vars.filter_map(|(name, value)| {
        let key = name.into_string().ok()?.to_ascii_lowercase();
        let value = value.into_string().ok()?;
        ["aws_", "google_", "azure_"]
            .iter()
            .any(|prefix| key.starts_with(prefix))
            .then_some((key, value))
    })
}

/// An object store and the prefix its objects are within. An absent object
/// reads as `None` and deletes as already removed.
#[derive(Clone)]
pub struct ObjectPrefix {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ObjectPrefix {
    /// The objects within `name` within `store_prefix` of `store`.
    pub fn new(store: Arc<dyn ObjectStore>, store_prefix: &str, name: &str) -> Self {
        ObjectPrefix {
            store,
            prefix: store_path(store_prefix, name),
        }
    }

    /// The prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The path `{prefix}/{suffix}`.
    pub fn path(&self, suffix: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/{suffix}", self.prefix))
    }

    /// The object at `path`, or `None` when the object does not exist.
    pub async fn get(&self, path: &ObjectPath) -> Result<Option<Vec<u8>>, object_store::Error> {
        match self.store.get(path).await {
            Ok(result) => Ok(Some(result.bytes().await?.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Writes `value` at `path`, over an existing object.
    pub async fn put(&self, path: &ObjectPath, value: Vec<u8>) -> Result<(), object_store::Error> {
        self.store.put(path, value.into()).await?;
        Ok(())
    }

    /// Removes the object at `path`. An absent object is not an error.
    pub async fn delete(&self, path: &ObjectPath) -> Result<(), object_store::Error> {
        match self.store.delete(path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// The objects directly within `path`, without the objects of a deeper
    /// path.
    pub async fn list(&self, path: &ObjectPath) -> Result<Vec<ObjectMeta>, object_store::Error> {
        Ok(self.store.list_with_delimiter(Some(path)).await?.objects)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use taquba::object_store::memory::InMemory;

    #[test]
    fn the_provider_options_are_the_provider_variables_in_lower_case() {
        let vars = [
            ("AWS_ENDPOINT", "http://127.0.0.1:9000"),
            ("ENDPOINT", "other"),
            ("GOOGLE_SERVICE_ACCOUNT", "sa.json"),
            ("AZURE_STORAGE_ACCOUNT_NAME", "account"),
            ("HOME", "/home/u"),
        ]
        .map(|(name, value)| (OsString::from(name), OsString::from(value)));
        let options: Vec<(String, String)> = provider_options(vars.into_iter()).collect();
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

    #[test]
    fn an_empty_prefix_gives_the_bare_name() {
        assert_eq!(store_path("", "definitions"), "definitions");
        assert_eq!(
            store_path("deploy/a", "definitions"),
            "deploy/a/definitions"
        );
    }

    #[tokio::test]
    async fn an_absent_object_reads_as_none_and_deletes_as_removed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let objects = ObjectPrefix::new(store.clone(), "deploy", "things");
        assert_eq!(objects.prefix(), "deploy/things");
        let path = objects.path("a");
        assert_eq!(path.as_ref(), "deploy/things/a");
        assert_eq!(objects.get(&path).await.unwrap(), None);
        objects.put(&path, b"1".to_vec()).await.unwrap();
        objects
            .put(&objects.path("sub/b"), b"2".to_vec())
            .await
            .unwrap();
        assert_eq!(objects.get(&path).await.unwrap(), Some(b"1".to_vec()));
        let listed = objects
            .list(&ObjectPath::from("deploy/things"))
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].location, path);
        objects.delete(&path).await.unwrap();
        objects.delete(&path).await.unwrap();
        assert_eq!(objects.get(&path).await.unwrap(), None);
    }
}
