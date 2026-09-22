//! The `object_exists` operator: waits until an object exists at a URL. Each
//! poll is a step of the run, so no worker is held between polls.
//!
//! The URL has the form of a `--store` URL, and a cloud scheme reads the
//! provider's environment variables for its credentials. The operator opens
//! the store of a URL once per scheme and host and keeps it for the polls
//! that follow, so a credential fetched on the first request is kept with
//! the store. The output is the URL, the size and the last-modified time of
//! the object. The deadline of the wait is set at the first poll and is the
//! state of the run between polls.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use taquba::object_store::path::Path as ObjectPath;
use taquba::object_store::{self, ObjectStore, ObjectStoreExt, ObjectStoreScheme, parse_url_opts};
use taquba_workflow::StepError;

use super::{Operator, Outcome, Task};
use crate::duration;
use crate::store::provider_options;

/// Parameters of the `object_exists` operator.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectExistsParams {
    /// The URL of the object, such as `s3://landing/orders/20260915.parquet`.
    pub url: String,
    /// The time between polls, one minute by default.
    #[serde(
        default = "default_interval",
        deserialize_with = "duration::deserialize"
    )]
    pub interval: Duration,
    /// The time after the first poll at which the task instance fails, one
    /// day by default.
    #[serde(
        default = "default_timeout",
        deserialize_with = "duration::deserialize"
    )]
    pub timeout: Duration,
}

fn default_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_timeout() -> Duration {
    Duration::from_secs(86_400)
}

/// The `object_exists` operator, with the store of every scheme and host it
/// opened.
#[derive(Debug, Default)]
pub struct ObjectExists {
    stores: Mutex<BTreeMap<String, Arc<dyn ObjectStore>>>,
}

impl ObjectExists {
    /// The store of `url`, opened at the first call for its scheme and host,
    /// and the path of the object within the store.
    fn open(&self, url: &url::Url) -> Result<(Arc<dyn ObjectStore>, ObjectPath), StepError> {
        let (_, path) = ObjectStoreScheme::parse(url)
            .map_err(|e| StepError::permanent(format!("`{url}` does not open: {e}")))?;
        let origin = url[..url::Position::BeforePath].to_string();
        let mut stores = self.stores.lock().expect("the lock is not poisoned");
        let store = match stores.get(&origin) {
            Some(store) => store.clone(),
            None => {
                let (store, _) = parse_url_opts(url, provider_options(std::env::vars_os()))
                    .map_err(|e| StepError::permanent(format!("`{url}` does not open: {e}")))?;
                let store: Arc<dyn ObjectStore> = Arc::from(store);
                stores.insert(origin, store.clone());
                store
            }
        };
        Ok((store, path))
    }
}

/// The state between polls.
#[derive(Debug, Serialize, Deserialize)]
struct State {
    /// The time at which the wait fails, in milliseconds from the Unix epoch.
    deadline_ms: u64,
}

impl Operator for ObjectExists {
    type Params = ObjectExistsParams;

    async fn run(&self, task: &Task<'_>, params: ObjectExistsParams) -> Result<Outcome, StepError> {
        let deadline_ms = match task.state {
            Some(state) => {
                serde_json::from_value::<State>(state.clone())
                    .map_err(|e| StepError::permanent(format!("the state is not a deadline: {e}")))?
                    .deadline_ms
            }
            None => task
                .now_ms
                .saturating_add(u64::try_from(params.timeout.as_millis()).unwrap_or(u64::MAX)),
        };
        let url = url::Url::parse(&params.url)
            .map_err(|e| StepError::permanent(format!("`{}` is not a URL: {e}", params.url)))?;
        let (store, path) = self.open(&url)?;
        match store.head(&path).await {
            Ok(meta) => Ok(Outcome::Succeeded(serde_json::json!({
                "url": params.url,
                "size": meta.size,
                "last_modified": meta
                    .last_modified
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            }))),
            Err(object_store::Error::NotFound { .. }) if task.now_ms >= deadline_ms => {
                Ok(Outcome::Failed(format!(
                    "no object at `{}` within {}s",
                    params.url,
                    params.timeout.as_secs()
                )))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Outcome::Continue {
                state: serde_json::to_value(State { deadline_ms }).expect("a state serializes"),
                after: params.interval,
            }),
            Err(e) => Err(StepError::transient(format!(
                "the head of `{}` failed: {e}",
                params.url
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use serde_json::Value;
    use taquba_workflow::{Step, StepErrorKind};

    use super::*;
    use crate::partition::Partition;
    use crate::task::TaskIdentity;

    fn dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("swale-object-exists-{}", std::process::id()))
            .join(name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn run(
        operator: &ObjectExists,
        url: &str,
        state: Option<Value>,
        now_ms: u64,
    ) -> Result<Outcome, StepError> {
        let step = Step::detached(Vec::new());
        let identity = TaskIdentity {
            graph: "g".into(),
            partition: Partition::new("20260915").unwrap(),
            node: "n".into(),
            asset: None,
            definition: "d".into(),
            rerun: 0,
        };
        let params = Value::Null;
        let inputs = BTreeMap::new();
        let task = Task {
            step: &step,
            identity: &identity,
            params: &params,
            inputs: &inputs,
            state: state.as_ref(),
            now_ms,
        };
        operator
            .run(
                &task,
                ObjectExistsParams {
                    url: url.into(),
                    interval: Duration::from_secs(30),
                    timeout: Duration::from_secs(3600),
                },
            )
            .await
    }

    #[test]
    fn params_have_defaults() {
        let p: ObjectExistsParams = serde_json::from_str(r#"{"url": "s3://b/k"}"#).unwrap();
        assert_eq!(p.interval, Duration::from_secs(60));
        assert_eq!(p.timeout, Duration::from_secs(86_400));
        let p: ObjectExistsParams =
            serde_json::from_str(r#"{"url": "s3://b/k", "interval": "5m", "timeout": "6h"}"#)
                .unwrap();
        assert_eq!(p.interval, Duration::from_secs(300));
        assert_eq!(p.timeout, Duration::from_secs(6 * 3600));
    }

    #[tokio::test]
    async fn an_absent_object_continues_until_the_deadline_and_then_fails() {
        let dir = dir("absent");
        let url = format!("file://{}/orders.parquet", dir.display());
        let operator = ObjectExists::default();
        let outcome = run(&operator, &url, None, 1_000).await.unwrap();
        let deadline = serde_json::json!({"deadline_ms": 1_000 + 3_600_000});
        assert_eq!(
            outcome,
            Outcome::Continue {
                state: deadline.clone(),
                after: Duration::from_secs(30),
            }
        );
        // A later poll keeps the deadline of the first.
        assert_eq!(
            run(&operator, &url, Some(deadline.clone()), 3_000_000)
                .await
                .unwrap(),
            Outcome::Continue {
                state: deadline.clone(),
                after: Duration::from_secs(30),
            }
        );
        assert_eq!(
            run(&operator, &url, Some(deadline), 3_601_000)
                .await
                .unwrap(),
            Outcome::Failed(format!("no object at `{url}` within 3600s"))
        );
        // The three polls opened the store of the scheme and host once.
        assert_eq!(
            operator.stores.lock().unwrap().keys().collect::<Vec<_>>(),
            ["file://"]
        );
    }

    #[tokio::test]
    async fn an_existing_object_succeeds_with_its_size() {
        let dir = dir("present");
        std::fs::write(dir.join("orders.parquet"), b"12345").unwrap();
        let url = format!("file://{}/orders.parquet", dir.display());
        let operator = ObjectExists::default();
        let Outcome::Succeeded(value) = run(&operator, &url, None, 1_000).await.unwrap() else {
            panic!("the object exists");
        };
        assert_eq!(value["url"], url);
        assert_eq!(value["size"], 5);
        assert!(value["last_modified"].as_str().unwrap().ends_with('Z'));
    }

    #[tokio::test]
    async fn an_invalid_url_is_a_permanent_error() {
        let operator = ObjectExists::default();
        let err = run(&operator, "not a url", None, 0).await.unwrap_err();
        assert_eq!(err.kind, StepErrorKind::Permanent);
        let err = run(&operator, "ftp://host/x", None, 0).await.unwrap_err();
        assert_eq!(err.kind, StepErrorKind::Permanent);
    }
}
