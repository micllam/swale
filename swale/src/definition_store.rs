//! The definitions of a deployment, stored in the object store by content
//! hash.
//!
//! - `{store prefix}/definitions/{hash}.toml`: the text of a definition.
//! - `{store prefix}/definitions/current/{graph}`: the pointer object of a
//!   graph. Its content is the hash of the graph's current definition.
//!
//! [`DefinitionStore::publish`] writes both objects and does not open the
//! queue. The daemon reads the pointer objects and adopts each definition
//! (see [`crate::daemon`]).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use taquba::object_store::path::Path as ObjectPath;
use taquba::object_store::{self, ObjectStore, ObjectStoreExt};

use crate::definition;
use crate::graph::Graph;
use crate::operator::OperatorSet;

/// A failure of the definition store.
#[derive(Debug, thiserror::Error)]
pub enum DefinitionError {
    /// The object store failed.
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    /// The text is not a valid definition.
    #[error(transparent)]
    Invalid(#[from] crate::Error),
    /// The object of a definition or a pointer is not UTF-8 text.
    #[error("object `{0}` is not UTF-8 text")]
    NotText(String),
    /// The text of a definition object does not have the hash of its path.
    #[error("the object of definition `{0}` has another hash")]
    HashMismatch(String),
    /// The current definition of another graph produces the asset.
    #[error("asset `{asset}` is produced by graph `{graph}`")]
    AssetConflict {
        /// The asset.
        asset: String,
        /// The other graph.
        graph: String,
    },
}

/// The result of [`DefinitionStore::publish`].
#[derive(Debug, Clone)]
pub struct Published {
    /// The checked graph.
    pub graph: Arc<Graph>,
    /// The hash of the definition.
    pub hash: String,
    /// Whether the pointer of the graph referred to another hash, or was
    /// absent, before the call.
    pub changed: bool,
}

/// The definitions of a deployment, by hash.
pub struct DefinitionStore {
    store: Arc<dyn ObjectStore>,
    root: String,
    operators: Arc<OperatorSet>,
    graphs: RwLock<HashMap<String, Arc<Graph>>>,
}

impl DefinitionStore {
    /// A definition store within `store_prefix` of `store`. A definition is
    /// checked against `operators` when it is loaded.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        store_prefix: &str,
        operators: Arc<OperatorSet>,
    ) -> Self {
        let root = if store_prefix.is_empty() {
            "definitions".to_string()
        } else {
            format!("{store_prefix}/definitions")
        };
        DefinitionStore {
            store,
            root,
            operators,
            graphs: RwLock::new(HashMap::new()),
        }
    }

    /// Checks `text` and writes its definition object. The pointer of the
    /// graph is unchanged.
    pub async fn put(&self, text: &str) -> Result<(String, Arc<Graph>), DefinitionError> {
        let graph = definition::load_str(text, &self.operators)?;
        let hash = definition::hash(text);
        self.store
            .put(
                &self.definition_path(&hash),
                text.as_bytes().to_vec().into(),
            )
            .await?;
        Ok((hash.clone(), self.cache(hash, graph)))
    }

    /// Publishes `text` as the current definition of its graph: checks that
    /// the current definition of another graph does not produce one of its
    /// assets, then writes the definition object and the pointer object.
    pub async fn publish(&self, text: &str) -> Result<Published, DefinitionError> {
        let graph = definition::load_str(text, &self.operators)?;
        let current = self.current().await?;
        for (name, hash) in &current {
            if name == graph.name() {
                continue;
            }
            let Some(other) = self.get(hash).await? else {
                continue;
            };
            let conflict = graph
                .nodes()
                .iter()
                .filter_map(|n| n.asset())
                .find(|asset| other.nodes().iter().any(|n| n.asset() == Some(asset)));
            if let Some(asset) = conflict {
                return Err(DefinitionError::AssetConflict {
                    asset: asset.to_string(),
                    graph: name.clone(),
                });
            }
        }
        let (hash, graph) = self.put(text).await?;
        let changed = current.get(graph.name()) != Some(&hash);
        if changed {
            self.store
                .put(
                    &self.pointer_path(graph.name()),
                    hash.as_bytes().to_vec().into(),
                )
                .await?;
        }
        Ok(Published {
            graph,
            hash,
            changed,
        })
    }

    /// The graph of the definition `hash`, or `None` when the store does not
    /// have the definition.
    pub async fn get(&self, hash: &str) -> Result<Option<Arc<Graph>>, DefinitionError> {
        let cached = self
            .graphs
            .read()
            .expect("the definition store is not poisoned")
            .get(hash)
            .cloned();
        if cached.is_some() {
            return Ok(cached);
        }
        let path = self.definition_path(hash);
        let Some(text) = self.read_text(&path).await? else {
            return Ok(None);
        };
        if definition::hash(&text) != hash {
            return Err(DefinitionError::HashMismatch(hash.to_string()));
        }
        let graph = definition::load_str(&text, &self.operators)?;
        Ok(Some(self.cache(hash.to_string(), graph)))
    }

    /// The hash of the current definition of every published graph, by graph
    /// name.
    pub async fn current(&self) -> Result<BTreeMap<String, String>, DefinitionError> {
        let prefix = ObjectPath::from(format!("{}/current", self.root));
        let listing = self.store.list_with_delimiter(Some(&prefix)).await?;
        let mut current = BTreeMap::new();
        for object in listing.objects {
            let Some(name) = object.location.filename() else {
                continue;
            };
            if let Some(hash) = self.read_text(&object.location).await? {
                current.insert(name.to_string(), hash.trim().to_string());
            }
        }
        Ok(current)
    }

    fn cache(&self, hash: String, graph: Graph) -> Arc<Graph> {
        self.graphs
            .write()
            .expect("the definition store is not poisoned")
            .entry(hash)
            .or_insert_with(|| Arc::new(graph))
            .clone()
    }

    async fn read_text(&self, path: &ObjectPath) -> Result<Option<String>, DefinitionError> {
        let bytes = match self.store.get(path).await {
            Ok(result) => result.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        String::from_utf8(bytes.to_vec())
            .map(Some)
            .map_err(|_| DefinitionError::NotText(path.to_string()))
    }

    fn definition_path(&self, hash: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/{hash}.toml", self.root))
    }

    fn pointer_path(&self, graph: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/current/{graph}", self.root))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use taquba::object_store::memory::InMemory;

    fn definition(graph: &str, asset: &str, arg: &str) -> String {
        format!(
            "[graph]\nname = \"{graph}\"\n[[node]]\nname = \"a\"\nproduces = \"{asset}\"\n\
             operator = \"subprocess\"\n[node.params]\nargv = [\"{arg}\"]\n"
        )
    }

    fn store_over(objects: &Arc<dyn ObjectStore>) -> DefinitionStore {
        DefinitionStore::new(objects.clone(), "deploy", Arc::new(OperatorSet::builtin()))
    }

    #[tokio::test]
    async fn publish_writes_the_definition_and_the_pointer_within_the_store_prefix() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = store_over(&objects);
        let text = definition("g", "asset_g", "true");
        let published = store.publish(&text).await.unwrap();
        assert!(published.changed);
        assert_eq!(published.hash, definition::hash(&text));

        let hash = &published.hash;
        let object = objects
            .get(&ObjectPath::from(format!("deploy/definitions/{hash}.toml")))
            .await
            .unwrap();
        assert_eq!(object.bytes().await.unwrap(), text.as_bytes());
        let pointer = objects
            .get(&ObjectPath::from("deploy/definitions/current/g"))
            .await
            .unwrap();
        assert_eq!(pointer.bytes().await.unwrap(), hash.as_bytes());

        // A second store over the same objects reads both.
        let reader = store_over(&objects);
        assert_eq!(
            reader.current().await.unwrap(),
            BTreeMap::from([("g".to_string(), hash.clone())])
        );
        assert_eq!(reader.get(hash).await.unwrap().unwrap().name(), "g");
        assert!(reader.get("absent").await.unwrap().is_none());

        // The same text again leaves the pointer, and an edit moves it.
        assert!(!store.publish(&text).await.unwrap().changed);
        let edited = store
            .publish(&definition("g", "asset_g", "false"))
            .await
            .unwrap();
        assert!(edited.changed);
        assert_eq!(reader.current().await.unwrap()["g"], edited.hash);
        // The replaced definition stays readable for the runs that record it.
        assert!(reader.get(hash).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn publish_refuses_an_asset_of_another_current_definition_and_an_invalid_text() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = store_over(&objects);
        store
            .publish(&definition("g", "shared", "true"))
            .await
            .unwrap();
        let conflict = store.publish(&definition("h", "shared", "true")).await;
        assert!(matches!(
            conflict,
            Err(DefinitionError::AssetConflict { asset, graph }) if asset == "shared" && graph == "g"
        ));
        let invalid = store.publish("[graph]\nname = \"BAD\"\n").await;
        assert!(matches!(invalid, Err(DefinitionError::Invalid(_))));
        assert_eq!(store.current().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_definition_object_with_another_hash_is_an_error() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = store_over(&objects);
        let hash = "0".repeat(64);
        objects
            .put(
                &ObjectPath::from(format!("deploy/definitions/{hash}.toml")),
                definition("g", "a", "true").into_bytes().into(),
            )
            .await
            .unwrap();
        assert!(matches!(
            store.get(&hash).await,
            Err(DefinitionError::HashMismatch(h)) if h == hash
        ));
    }
}
