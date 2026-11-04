//! The `http` operator: sends one request and reads the response as the output.
//!
//! The output is `{"status": <code>, "body": <value>}`. The body is parsed as
//! JSON when it is JSON and kept as text otherwise, and an empty body is
//! `null`. A 2xx status is success. A 5xx status, a 429 status, a connection
//! failure and a timeout are transient errors, and any other status is a
//! permanent error. The tail of the response body is in the error message. The
//! request must be idempotent per attempt.
//!
//! With a [`Poll`], the operator sends the request again at each poll until the
//! value at a path of the JSON body is a `done` value. No worker is held
//! between polls, and the last response is the output. A `failed` value fails
//! the task instance, and so do a body without a value at the path and the
//! deadline that the first poll sets.

use std::collections::BTreeMap;
use std::time::Duration;

use reqwest::{Client, Method, StatusCode};
use serde::Deserialize;
use serde_json::Value;
use taquba_workflow::StepError;

use super::{Deadline, Lease, Operator, Outcome, Task, keep_lease, tail};
use crate::duration;
use crate::template::lookup;

/// Parameters of the `http` operator: the request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpParams {
    /// The method, `GET` by default.
    #[serde(default = "default_method", deserialize_with = "method")]
    pub method: String,
    /// The URL.
    pub url: String,
    /// The request headers.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// The request body.
    #[serde(default)]
    pub body: Option<String>,
    /// The time the whole request can take, `60s` by default.
    #[serde(
        default = "default_timeout",
        deserialize_with = "duration::deserialize"
    )]
    pub timeout: Duration,
    /// The poll of the request until the response reports a terminal value, or
    /// `None` for a single request.
    pub poll: Option<Poll>,
}

/// The poll of an `http` request: the path of the value in the JSON body and
/// the values that end the wait.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Poll {
    /// The dotted path of the value in the body, such as `status` or
    /// `job.state`.
    #[serde(deserialize_with = "path")]
    pub path: String,
    /// The values at which the task instance succeeds, one or more.
    #[serde(deserialize_with = "done")]
    pub done: Vec<Value>,
    /// The values at which the task instance fails.
    #[serde(default)]
    pub failed: Vec<Value>,
    /// The time between polls, one minute by default.
    #[serde(
        default = "default_interval",
        deserialize_with = "duration::deserialize"
    )]
    pub interval: Duration,
    /// The time after the first poll at which the task instance fails, one day
    /// by default.
    #[serde(
        default = "default_poll_timeout",
        deserialize_with = "duration::deserialize"
    )]
    pub timeout: Duration,
}

impl Poll {
    fn segments(&self) -> Vec<String> {
        self.path.split('.').map(str::to_string).collect()
    }
}

fn default_method() -> String {
    "GET".to_string()
}

fn default_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_poll_timeout() -> Duration {
    Duration::from_secs(86_400)
}

fn path<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let text = String::deserialize(deserializer)?;
    if text.split('.').any(str::is_empty) {
        return Err(serde::de::Error::custom(format!(
            "`{text}` is not a dotted path such as `status` or `job.state`"
        )));
    }
    Ok(text)
}

fn done<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Vec<Value>, D::Error> {
    let values = Vec::<Value>::deserialize(deserializer)?;
    if values.is_empty() {
        return Err(serde::de::Error::custom(
            "`done` requires at least one value",
        ));
    }
    Ok(values)
}

fn method<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let text = String::deserialize(deserializer)?;
    Method::from_bytes(text.as_bytes())
        .map_err(|_| serde::de::Error::custom(format!("`{text}` is not an HTTP method")))?;
    Ok(text)
}

/// The `http` operator.
#[derive(Debug, Clone)]
pub struct Http {
    /// The client of every request.
    pub client: Client,
    /// The lease extension while the request runs.
    pub lease: Lease,
}

impl Default for Http {
    fn default() -> Self {
        Http {
            client: Client::new(),
            lease: Lease::default(),
        }
    }
}

impl Operator for Http {
    type Params = HttpParams;

    async fn run(&self, task: &Task<'_>, params: HttpParams) -> Result<Outcome, StepError> {
        let method = Method::from_bytes(params.method.as_bytes()).map_err(|_| {
            StepError::permanent(format!("`{}` is not an HTTP method", params.method))
        })?;
        let request = format!("{method} {}", params.url);
        let mut builder = self
            .client
            .request(method, &params.url)
            .timeout(params.timeout);
        for (name, value) in &params.headers {
            builder = builder.header(name, value);
        }
        if let Some(body) = params.body {
            builder = builder.body(body);
        }
        let send = async {
            let response = builder.send().await?;
            let status = response.status();
            let body = response.bytes().await?;
            Ok::<_, reqwest::Error>((status, body))
        };
        let (status, body) = tokio::select! {
            sent = send => sent.map_err(|e| {
                if e.is_builder() {
                    StepError::permanent(format!("`{request}` is not a valid request: {e}"))
                } else {
                    StepError::transient(format!("`{request}` failed: {e}"))
                }
            })?,
            e = keep_lease(task.step, self.lease) => return Err(e),
            () = task.step.cancel_token.cancelled() => {
                return Err(StepError::transient("the run was cancelled while the request ran"));
            }
        };
        if !status.is_success() {
            let message = format!("`{request}` returned {}: {}", status.as_u16(), tail(&body));
            return if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
                Err(StepError::transient(message))
            } else {
                Err(StepError::permanent(message))
            };
        }
        let trimmed = body.trim_ascii();
        let body = if trimmed.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(trimmed)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned()))
        };
        let Some(poll) = params.poll else {
            return Ok(Outcome::Succeeded(serde_json::json!({
                "status": status.as_u16(),
                "body": body,
            })));
        };
        let deadline = Deadline::of(task, poll.timeout)?;
        let Some(value) = lookup(&body, &poll.segments()) else {
            return Ok(Outcome::Failed(format!(
                "the body of `{request}` has no value at `{}`",
                poll.path
            )));
        };
        if poll.done.contains(value) {
            Ok(Outcome::Succeeded(serde_json::json!({
                "status": status.as_u16(),
                "body": body,
            })))
        } else if poll.failed.contains(value) {
            Ok(Outcome::Failed(format!(
                "`{request}` reported `{}` = {value}",
                poll.path
            )))
        } else if deadline.passed(task) {
            Ok(Outcome::Failed(format!(
                "`{request}` reported `{}` = {value} after {:?}",
                poll.path, poll.timeout
            )))
        } else {
            Ok(deadline.poll_after(poll.interval))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::Partition;
    use crate::task::TaskIdentity;
    use taquba_workflow::{Step, StepErrorKind};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// A server of one response. It returns its URL and the request it
    /// received.
    async fn serve(status: u16, body: &'static str) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/path", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            loop {
                let n = socket.read(&mut buffer).await.unwrap();
                request.extend_from_slice(&buffer[..n]);
                let text = String::from_utf8_lossy(&request);
                let Some(end) = text.find("\r\n\r\n") else {
                    continue;
                };
                let length = text[..end]
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .map(|v| v.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if request.len() >= end + 4 + length {
                    break;
                }
            }
            let response = format!(
                "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
            String::from_utf8_lossy(&request).into_owned()
        });
        (url, handle)
    }

    async fn run(params: HttpParams) -> Result<Outcome, StepError> {
        poll_at(params, None, 0).await
    }

    async fn poll_at(
        params: HttpParams,
        state: Option<Value>,
        now_ms: u64,
    ) -> Result<Outcome, StepError> {
        let step = Step::detached(Vec::new());
        let identity = TaskIdentity {
            graph: "g".into(),
            partition: Partition::none(),
            node: "n".into(),
            asset: None,
            definition: "d".into(),
            rerun: 0,
        };
        let value = Value::Null;
        let inputs = BTreeMap::new();
        let task = Task {
            step: &step,
            identity: &identity,
            params: &value,
            inputs: &inputs,
            state: state.as_ref(),
            now_ms,
        };
        Http::default().run(&task, params).await
    }

    fn params(url: String) -> HttpParams {
        serde_json::from_value(serde_json::json!({"url": url})).unwrap()
    }

    fn polled(url: String) -> HttpParams {
        serde_json::from_value(serde_json::json!({
            "url": url,
            "poll": {
                "path": "job.state",
                "done": ["ended"],
                "failed": ["expired", "cancelled"],
                "interval": "30s",
                "timeout": "1h",
            },
        }))
        .unwrap()
    }

    #[test]
    fn params_have_defaults_and_check_the_method_and_the_timeout() {
        let p = params("https://example.test/".into());
        assert_eq!(p.method, "GET");
        assert_eq!(p.timeout, Duration::from_secs(60));
        assert!(p.headers.is_empty());
        assert_eq!(p.body, None);
        assert_eq!(p.poll, None);
        let p: HttpParams = serde_json::from_value(serde_json::json!({
            "url": "u", "poll": {"path": "status", "done": ["completed", true]}
        }))
        .unwrap();
        assert_eq!(
            p.poll,
            Some(Poll {
                path: "status".into(),
                done: vec![serde_json::json!("completed"), serde_json::json!(true)],
                failed: Vec::new(),
                interval: Duration::from_secs(60),
                timeout: Duration::from_secs(86_400),
            })
        );
        let p: HttpParams = serde_json::from_value(serde_json::json!({
            "method": "post", "url": "u", "timeout": "5m", "headers": {"a": "b"}, "body": "x"
        }))
        .unwrap();
        assert_eq!(p.method, "post");
        assert_eq!(p.timeout, Duration::from_secs(300));
        for json in [
            r#"{"url": "u", "method": "a b"}"#,
            r#"{"url": "u", "timeout": "5"}"#,
            r#"{"url": "u", "extra": 1}"#,
            r#"{"method": "GET"}"#,
            r#"{"url": "u", "poll": {"path": "status", "done": []}}"#,
            r#"{"url": "u", "poll": {"path": "job..state", "done": ["x"]}}"#,
            r#"{"url": "u", "poll": {"path": "status"}}"#,
            r#"{"url": "u", "poll": {"path": "status", "done": ["x"], "every": "1m"}}"#,
        ] {
            assert!(serde_json::from_str::<HttpParams>(json).is_err(), "{json}");
        }
    }

    #[tokio::test]
    async fn a_2xx_response_is_the_output_with_the_body_parsed_when_json() {
        let (url, server) = serve(201, r#"{"rows": 3}"#).await;
        let mut p = params(url);
        p.method = "POST".into();
        p.headers.insert("Content-Type".into(), "text/plain".into());
        p.body = Some("hello".into());
        assert_eq!(
            run(p).await.unwrap(),
            Outcome::Succeeded(serde_json::json!({"status": 201, "body": {"rows": 3}}))
        );
        let request = server.await.unwrap();
        assert!(request.starts_with("POST /path HTTP/1.1\r\n"), "{request}");
        assert!(
            request
                .to_ascii_lowercase()
                .contains("content-type: text/plain"),
            "{request}"
        );
        assert!(request.ends_with("\r\n\r\nhello"), "{request}");

        let (url, _) = serve(200, "plain text").await;
        assert_eq!(
            run(params(url)).await.unwrap(),
            Outcome::Succeeded(serde_json::json!({"status": 200, "body": "plain text"}))
        );
        let (url, _) = serve(204, "").await;
        assert_eq!(
            run(params(url)).await.unwrap(),
            Outcome::Succeeded(serde_json::json!({"status": 204, "body": null}))
        );
    }

    #[tokio::test]
    async fn statuses_and_failures_map_to_transient_and_permanent_errors() {
        for status in [500, 503, 429] {
            let (url, _) = serve(status, "later").await;
            let err = run(params(url)).await.unwrap_err();
            assert_eq!(err.kind, StepErrorKind::Transient, "{status}");
            assert!(
                err.message.contains(&format!("returned {status}: later")),
                "{}",
                err.message
            );
        }
        for status in [400, 404] {
            let (url, _) = serve(status, "no").await;
            let err = run(params(url)).await.unwrap_err();
            assert_eq!(err.kind, StepErrorKind::Permanent, "{status}");
            assert!(err.message.contains("GET http://"), "{}", err.message);
        }
        // A closed port.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        drop(listener);
        let err = run(params(url)).await.unwrap_err();
        assert_eq!(err.kind, StepErrorKind::Transient);
        let err = run(params("not a url".into())).await.unwrap_err();
        assert_eq!(err.kind, StepErrorKind::Permanent);
    }

    #[tokio::test]
    async fn a_poll_continues_with_its_first_deadline_until_a_done_value() {
        let (url, _) = serve(200, r#"{"job": {"state": "running"}}"#).await;
        let outcome = poll_at(polled(url), None, 1_000).await.unwrap();
        let deadline = serde_json::json!({"deadline_ms": 1_000 + 3_600_000});
        assert_eq!(
            outcome,
            Outcome::Continue {
                state: deadline.clone(),
                after: Duration::from_secs(30),
            }
        );
        let (url, _) = serve(200, r#"{"job": {"state": "running"}}"#).await;
        assert_eq!(
            poll_at(polled(url), Some(deadline.clone()), 2_000_000)
                .await
                .unwrap(),
            Outcome::Continue {
                state: deadline.clone(),
                after: Duration::from_secs(30),
            }
        );
        let (url, _) = serve(200, r#"{"job": {"state": "ended", "rows": 7}}"#).await;
        assert_eq!(
            poll_at(polled(url), Some(deadline), 3_000_000)
                .await
                .unwrap(),
            Outcome::Succeeded(serde_json::json!({
                "status": 200,
                "body": {"job": {"state": "ended", "rows": 7}},
            }))
        );
    }

    #[tokio::test]
    async fn a_poll_fails_at_a_failed_value_an_absent_value_and_the_deadline() {
        let deadline = serde_json::json!({"deadline_ms": 3_601_000});
        let (url, _) = serve(200, r#"{"job": {"state": "expired"}}"#).await;
        let Outcome::Failed(reason) = poll_at(polled(url), Some(deadline.clone()), 2_000)
            .await
            .unwrap()
        else {
            panic!("a failed value fails the task instance");
        };
        assert!(
            reason.ends_with(r#"reported `job.state` = "expired""#),
            "{reason}"
        );
        let (url, _) = serve(200, "not json").await;
        let Outcome::Failed(reason) = poll_at(polled(url), Some(deadline.clone()), 2_000)
            .await
            .unwrap()
        else {
            panic!("a body without the value fails the task instance");
        };
        assert!(reason.ends_with("has no value at `job.state`"), "{reason}");
        let (url, _) = serve(200, r#"{"job": {"state": "running"}}"#).await;
        let Outcome::Failed(reason) = poll_at(polled(url), Some(deadline), 3_601_000)
            .await
            .unwrap()
        else {
            panic!("the deadline fails the task instance");
        };
        assert!(
            reason.ends_with(r#"reported `job.state` = "running" after 3600s"#),
            "{reason}"
        );
    }
}
