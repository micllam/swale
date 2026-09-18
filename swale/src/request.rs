//! The request: an operation on a running daemon, written to the object
//! store by a command and applied by the daemon.
//!
//! A command writes a [`Request`] as the object `{store prefix}/requests/{id}`
//! and never opens the queue. The daemon reads the objects at every sync
//! pass, applies each request in id order, records the outcome as the
//! [`RequestRecord`](crate::records::RequestRecord) at the KV key
//! `swale/requests/{id}` and removes the object. A request is applied once:
//! a pass that finds the record of an object only removes the object.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use taquba::object_store::path::Path as ObjectPath;
use taquba::object_store::{self, ObjectStore};

use crate::partition::Partition;
use crate::records::JsonBytes;
use crate::store::ObjectPrefix;

/// Maximum length of a request id in bytes.
pub const MAX_REQUEST_ID_LEN: usize = 64;

/// The id of a request: `[A-Za-z0-9_-]` of one to [`MAX_REQUEST_ID_LEN`]
/// bytes. A generated id is a ULID, so the ids of a listing sort by the time
/// of the request.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(String);

/// The text is not a request id.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("`{0}` is not a request id: `[A-Za-z0-9_-]` of one to {MAX_REQUEST_ID_LEN} bytes")]
pub struct InvalidRequestId(pub String);

impl RequestId {
    /// Validates `text` as a request id.
    pub fn new(text: impl Into<String>) -> Result<Self, InvalidRequestId> {
        let text = text.into();
        let valid = !text.is_empty()
            && text.len() <= MAX_REQUEST_ID_LEN
            && text
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
        if valid {
            Ok(RequestId(text))
        } else {
            Err(InvalidRequestId(text))
        }
    }

    /// A new id for a request at `now_ms`, in milliseconds from the Unix
    /// epoch.
    pub fn generate(now_ms: u64) -> Self {
        let time = SystemTime::UNIX_EPOCH + Duration::from_millis(now_ms);
        RequestId(ulid::Ulid::from_datetime(time).to_string())
    }

    /// The id text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for RequestId {
    type Err = InvalidRequestId;

    fn from_str(text: &str) -> Result<Self, InvalidRequestId> {
        RequestId::new(text)
    }
}

/// An operation on a running daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Request {
    /// Start the graph run of every listed partition of the graph's adopted
    /// definition. A partition with a graph run is unchanged.
    Start {
        /// The graph name.
        graph: String,
        /// The partitions.
        partitions: Vec<Partition>,
    },
    /// Run a node with a failed or cancelled record again, at the next rerun
    /// count.
    Rerun {
        /// The graph name.
        graph: String,
        /// The partition.
        partition: Partition,
        /// The node name.
        node: String,
    },
    /// Cancel an active graph run.
    Cancel {
        /// The graph name.
        graph: String,
        /// The partition.
        partition: Partition,
    },
}

impl JsonBytes for Request {}

/// The request objects of a deployment.
pub struct RequestStore {
    objects: ObjectPrefix,
}

impl RequestStore {
    /// A request store within `store_prefix` of `store`.
    pub fn new(store: Arc<dyn ObjectStore>, store_prefix: &str) -> Self {
        RequestStore {
            objects: ObjectPrefix::new(store, store_prefix, "requests"),
        }
    }

    /// Writes the object of `request` with `id`.
    pub async fn submit(
        &self,
        id: &RequestId,
        request: &Request,
    ) -> Result<(), object_store::Error> {
        self.objects.put(&self.path(id), request.to_bytes()).await
    }

    /// The request objects, in id order, with the bytes of each. An object
    /// whose name is not a request id is skipped.
    pub async fn list(&self) -> Result<Vec<(RequestId, Vec<u8>)>, object_store::Error> {
        let prefix = ObjectPath::from(self.objects.prefix());
        let mut requests = Vec::new();
        for object in self.objects.list(&prefix).await? {
            let Some(id) = object
                .location
                .filename()
                .and_then(|name| RequestId::new(name).ok())
            else {
                tracing::warn!(object = %object.location, "the object is not a request");
                continue;
            };
            // An object removed between the listing and the read is skipped.
            if let Some(bytes) = self.objects.get(&object.location).await? {
                requests.push((id, bytes));
            }
        }
        requests.sort();
        Ok(requests)
    }

    /// Removes the object of the request `id`. An absent object is not an
    /// error.
    pub async fn remove(&self, id: &RequestId) -> Result<(), object_store::Error> {
        self.objects.delete(&self.path(id)).await
    }

    fn path(&self, id: &RequestId) -> ObjectPath {
        self.objects.path(id.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use taquba::object_store::ObjectStoreExt;
    use taquba::object_store::memory::InMemory;

    #[test]
    fn a_generated_id_sorts_by_time_and_the_charset_is_checked() {
        let earlier = RequestId::generate(1_700_000_000_000);
        let later = RequestId::generate(1_700_000_000_001);
        assert!(earlier < later);
        assert_eq!(earlier.as_str().len(), 26);
        assert_eq!(RequestId::new(earlier.as_str()), Ok(earlier));
        for text in ["", "a/b", "a b", &"x".repeat(MAX_REQUEST_ID_LEN + 1)] {
            assert!(RequestId::new(text).is_err(), "{text}");
        }
        assert_eq!("req-1".parse::<RequestId>().unwrap().to_string(), "req-1");
    }

    #[test]
    fn the_json_form_has_a_kind_tag() {
        let start = Request::Start {
            graph: "orders".into(),
            partitions: vec![Partition::new("20260915").unwrap()],
        };
        assert_eq!(
            start.to_bytes(),
            br#"{"kind":"start","graph":"orders","partitions":["20260915"]}"#
        );
        let rerun = Request::Rerun {
            graph: "orders".into(),
            partition: Partition::new("20260915").unwrap(),
            node: "transform".into(),
        };
        assert_eq!(
            rerun.to_bytes(),
            br#"{"kind":"rerun","graph":"orders","partition":"20260915","node":"transform"}"#
        );
        let cancel = Request::Cancel {
            graph: "orders".into(),
            partition: Partition::new("20260915").unwrap(),
        };
        assert_eq!(
            cancel.to_bytes(),
            br#"{"kind":"cancel","graph":"orders","partition":"20260915"}"#
        );
        for request in [start, rerun, cancel] {
            assert_eq!(Request::from_bytes(&request.to_bytes()).unwrap(), request);
        }
        assert!(Request::from_bytes(br#"{"kind":"forget","graph":"orders"}"#).is_err());
    }

    #[tokio::test]
    async fn the_store_lists_the_objects_within_the_prefix_in_id_order() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = RequestStore::new(objects.clone(), "deploy");
        let cancel = Request::Cancel {
            graph: "orders".into(),
            partition: Partition::new("20260915").unwrap(),
        };
        let later = RequestId::new("b").unwrap();
        let earlier = RequestId::new("a").unwrap();
        store.submit(&later, &cancel).await.unwrap();
        store.submit(&earlier, &cancel).await.unwrap();
        objects
            .put(
                &ObjectPath::from("deploy/requests/not an id"),
                b"{}".to_vec().into(),
            )
            .await
            .unwrap();
        objects
            .put(&ObjectPath::from("deploy/other/c"), b"{}".to_vec().into())
            .await
            .unwrap();
        let object = objects
            .get(&ObjectPath::from("deploy/requests/a"))
            .await
            .unwrap();
        assert_eq!(object.bytes().await.unwrap(), cancel.to_bytes());

        let listed = store.list().await.unwrap();
        assert_eq!(
            listed,
            [
                (earlier.clone(), cancel.to_bytes()),
                (later.clone(), cancel.to_bytes())
            ]
        );

        store.remove(&earlier).await.unwrap();
        store.remove(&earlier).await.unwrap();
        let listed = store.list().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].0, later);
    }
}
