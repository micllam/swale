//! The substitution language of a parameter string. A reference is written
//! between `{{` and `}}`, and the language is substitution only.
//!
//! The references:
//!
//! - `{{ partition }}`, the partition of the task instance.
//! - `{{ run.<field> }}`, a field of the run: `id` for the run id of the task
//!   instance or `summary` for the summary of the graph run as JSON text.
//! - `{{ upstream.<node>.<path> }}`, the output of an upstream node or the
//!   value at a dotted path into it, such as `rows.0.id`.
//! - `{{ env.<NAME> }}`, an environment variable of the process that renders
//!   the template.
//! - `{{ item }}`, the item of a member of a mapped node, or
//!   `{{ item.<path> }}`, the value at a dotted path into it.
//!
//! A reference to an unset variable fails the task instance. The error of a
//! failed `http` request includes the URL.

use std::collections::BTreeMap;

/// The values a [`Template`] is rendered against.
#[derive(Debug, Clone, Copy)]
pub struct RenderContext<'a> {
    /// The partition of the task instance.
    pub partition: &'a str,
    /// The run, for `{{ run.<field> }}`.
    pub run: RunContext<'a>,
    /// The output of each upstream node with a succeeded record.
    pub upstream: &'a BTreeMap<String, serde_json::Value>,
    /// The environment variables, by name.
    pub env: &'a BTreeMap<String, String>,
    /// The item of a member of a mapped node, for `{{ item }}`. `None` for
    /// every other run.
    pub item: Option<&'a serde_json::Value>,
}

/// The fields of the run of a task instance.
#[derive(Debug, Clone, Copy)]
pub struct RunContext<'a> {
    /// The run id of the task instance.
    pub id: &'a str,
    /// The summary of the graph run, JSON text.
    pub summary: &'a str,
}

/// A reference cannot be resolved against the [`RenderContext`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RenderError {
    /// No succeeded record exists for the upstream node, so there is no output.
    #[error("no output is recorded for upstream `{node}`")]
    MissingUpstream {
        /// The upstream node.
        node: String,
    },
    /// The path is absent from the upstream output.
    #[error("path `{path}` is absent from the output of upstream `{node}`")]
    MissingPath {
        /// The upstream node.
        node: String,
        /// The dotted path.
        path: String,
    },
    /// The environment variable is not set.
    #[error("environment variable `{name}` is not set")]
    MissingEnv {
        /// The variable name.
        name: String,
    },
    /// The run is not a member of a mapped node, so there is no item.
    #[error("the run does not have an item")]
    MissingItem,
    /// The path is absent from the item.
    #[error("path `{path}` is absent from the item")]
    MissingItemPath {
        /// The dotted path.
        path: String,
    },
}

/// One piece of a parsed [`Template`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    /// Text copied to the output unchanged.
    Literal(String),
    /// The partition of the task instance.
    Partition,
    /// A field of the run.
    Run(RunField),
    /// A value from the output of an upstream node: the node's name and the
    /// dotted path into the output, split at the dots.
    Upstream { node: String, path: Vec<String> },
    /// The environment variable of the name.
    Env(String),
    /// A value from the item of a member: the dotted path into the item, split
    /// at the dots.
    Item { path: Vec<String> },
}

/// A field of the run, the part after `run.` in a reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunField {
    /// `id`: the run id of the task instance.
    Id,
    /// `summary`: the summary of the graph run.
    Summary,
}

/// A parsed parameter string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Template {
    segments: Vec<Segment>,
}

/// The text is not a template.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TemplateError {
    /// A `{{` without a `}}` after it.
    #[error("`{{{{` at byte {0} is not closed")]
    Unclosed(usize),
    /// A reference that is not one of the language's references.
    #[error("`{0}` is not a template reference")]
    UnknownReference(String),
}

impl Template {
    /// Parses a parameter string.
    pub fn parse(source: &str) -> Result<Template, TemplateError> {
        let mut segments = Vec::new();
        let mut rest = source;
        let mut offset = 0;
        while let Some(open) = rest.find("{{") {
            if open > 0 {
                segments.push(Segment::Literal(rest[..open].to_string()));
            }
            let after_open = &rest[open + 2..];
            let close = after_open
                .find("}}")
                .ok_or(TemplateError::Unclosed(offset + open))?;
            segments.push(reference(after_open[..close].trim())?);
            let consumed = open + 2 + close + 2;
            offset += consumed;
            rest = &rest[consumed..];
        }
        if !rest.is_empty() {
            segments.push(Segment::Literal(rest.to_string()));
        }
        Ok(Template { segments })
    }

    /// The names of the upstream nodes the template refers to, in source order
    /// and with repeats.
    pub fn upstream_nodes(&self) -> impl Iterator<Item = &str> {
        self.segments.iter().filter_map(|segment| match segment {
            Segment::Upstream { node, .. } => Some(node.as_str()),
            _ => None,
        })
    }

    /// Whether the template refers to `{{ item }}`.
    pub fn refers_to_item(&self) -> bool {
        self.segments
            .iter()
            .any(|segment| matches!(segment, Segment::Item { .. }))
    }

    /// Renders the template. A string value from an upstream output or from the
    /// item is copied as is, and any other JSON value as its JSON text.
    pub fn render(&self, ctx: &RenderContext<'_>) -> Result<String, RenderError> {
        let mut out = String::new();
        for segment in &self.segments {
            match segment {
                Segment::Literal(text) => out.push_str(text),
                Segment::Partition => out.push_str(ctx.partition),
                Segment::Run(RunField::Id) => out.push_str(ctx.run.id),
                Segment::Run(RunField::Summary) => out.push_str(ctx.run.summary),
                Segment::Upstream { node, path } => {
                    let output = ctx
                        .upstream
                        .get(node)
                        .ok_or_else(|| RenderError::MissingUpstream { node: node.clone() })?;
                    let value = lookup(output, path).ok_or_else(|| RenderError::MissingPath {
                        node: node.clone(),
                        path: path.join("."),
                    })?;
                    push_value(&mut out, value);
                }
                Segment::Env(name) => {
                    let value = ctx
                        .env
                        .get(name)
                        .ok_or_else(|| RenderError::MissingEnv { name: name.clone() })?;
                    out.push_str(value);
                }
                Segment::Item { path } => {
                    let item = ctx.item.ok_or(RenderError::MissingItem)?;
                    let value = lookup(item, path).ok_or_else(|| RenderError::MissingItemPath {
                        path: path.join("."),
                    })?;
                    push_value(&mut out, value);
                }
            }
        }
        Ok(out)
    }
}

/// The value at `path` into `value`: a key of an object or an index of an array
/// at each part.
fn lookup<'a>(value: &'a serde_json::Value, path: &[String]) -> Option<&'a serde_json::Value> {
    path.iter().try_fold(value, |value, key| match value {
        serde_json::Value::Object(map) => map.get(key),
        serde_json::Value::Array(items) => key.parse::<usize>().ok().and_then(|i| items.get(i)),
        _ => None,
    })
}

/// Appends a string value as is, and any other JSON value as its JSON text.
fn push_value(out: &mut String, value: &serde_json::Value) {
    match value {
        serde_json::Value::String(text) => out.push_str(text),
        other => out.push_str(&other.to_string()),
    }
}

fn reference(text: &str) -> Result<Segment, TemplateError> {
    match text {
        "partition" => Ok(Segment::Partition),
        "item" => Ok(Segment::Item { path: Vec::new() }),
        _ => {
            let unknown = || TemplateError::UnknownReference(text.to_string());
            if let Some(rest) = text.strip_prefix("item.") {
                let path: Vec<String> = rest.split('.').map(str::to_string).collect();
                return if path.iter().any(String::is_empty) {
                    Err(unknown())
                } else {
                    Ok(Segment::Item { path })
                };
            }
            if let Some(field) = text.strip_prefix("run.") {
                return match field {
                    "id" => Ok(Segment::Run(RunField::Id)),
                    "summary" => Ok(Segment::Run(RunField::Summary)),
                    _ => Err(unknown()),
                };
            }
            if let Some(name) = text.strip_prefix("env.") {
                return if is_env_name(name) {
                    Ok(Segment::Env(name.to_string()))
                } else {
                    Err(unknown())
                };
            }
            let rest = text.strip_prefix("upstream.").ok_or_else(unknown)?;
            let mut parts = rest.split('.').map(str::to_string);
            let node = parts.next().expect("a split yields one item");
            let path: Vec<String> = parts.collect();
            if node.is_empty() || path.iter().any(String::is_empty) {
                return Err(unknown());
            }
            Ok(Segment::Upstream { node, path })
        }
    }
}

/// Whether `name` is an environment variable name of the form
/// `[A-Za-z_][A-Za-z0-9_]*`.
fn is_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_literals_and_each_reference_in_source_order() {
        let template = Template::parse(
            "a/{{ partition }}/{{run.id}}-{{ run.summary }}{{ upstream.extract.rows.key }}{{ env.TOKEN }}{{ item }}{{ item.url.0 }}",
        )
        .unwrap();
        assert_eq!(
            template.segments,
            [
                Segment::Literal("a/".into()),
                Segment::Partition,
                Segment::Literal("/".into()),
                Segment::Run(RunField::Id),
                Segment::Literal("-".into()),
                Segment::Run(RunField::Summary),
                Segment::Upstream {
                    node: "extract".into(),
                    path: vec!["rows".into(), "key".into()],
                },
                Segment::Env("TOKEN".into()),
                Segment::Item { path: vec![] },
                Segment::Item {
                    path: vec!["url".into(), "0".into()],
                },
            ]
        );
        assert!(template.refers_to_item());
        assert!(!Template::parse("{{ partition }}").unwrap().refers_to_item());
    }

    #[test]
    fn upstream_reference_without_a_path_is_the_whole_output() {
        let template = Template::parse("{{ upstream.extract }}").unwrap();
        assert_eq!(
            template.segments,
            [Segment::Upstream {
                node: "extract".into(),
                path: vec![],
            }]
        );
        assert_eq!(template.upstream_nodes().collect::<Vec<_>>(), ["extract"]);
    }

    #[test]
    fn text_without_a_reference_is_one_literal() {
        let template = Template::parse("plain").unwrap();
        assert_eq!(template.segments, [Segment::Literal("plain".into())]);
        assert_eq!(template.upstream_nodes().count(), 0);
    }

    #[test]
    fn render_substitutes_each_reference_and_copies_strings_as_is() {
        let upstream = BTreeMap::from([(
            "extract".to_string(),
            serde_json::json!({"rows": 3, "key": "a/b", "list": [{"x": true}]}),
        )]);
        let env = BTreeMap::from([("TOKEN".to_string(), "t0k".to_string())]);
        let item = serde_json::json!({"url": "https://a.test/", "tags": ["x"]});
        let ctx = RenderContext {
            partition: "20260915",
            run: RunContext {
                id: "g-20260915-load-r0",
                summary: "{}",
            },
            upstream: &upstream,
            env: &env,
            item: Some(&item),
        };
        let render = |text: &str| Template::parse(text).unwrap().render(&ctx);
        assert_eq!(
            render("{{ partition }}/{{ run.id }}/{{ run.summary }}").unwrap(),
            "20260915/g-20260915-load-r0/{}"
        );
        assert_eq!(render("n={{ upstream.extract.rows }}").unwrap(), "n=3");
        assert_eq!(render("{{ upstream.extract.key }}").unwrap(), "a/b");
        assert_eq!(render("{{ upstream.extract.list.0.x }}").unwrap(), "true");
        assert_eq!(
            render("{{ upstream.extract }}").unwrap(),
            r#"{"key":"a/b","list":[{"x":true}],"rows":3}"#
        );
        assert_eq!(
            render("{{ upstream.other.rows }}"),
            Err(RenderError::MissingUpstream {
                node: "other".into()
            })
        );
        assert_eq!(
            render("{{ upstream.extract.rows.deeper }}"),
            Err(RenderError::MissingPath {
                node: "extract".into(),
                path: "rows.deeper".into(),
            })
        );
        assert_eq!(render("Bearer {{ env.TOKEN }}").unwrap(), "Bearer t0k");
        assert_eq!(
            render("{{ env.OTHER }}"),
            Err(RenderError::MissingEnv {
                name: "OTHER".into()
            })
        );
        assert_eq!(render("{{ item.url }}").unwrap(), "https://a.test/");
        assert_eq!(render("{{ item.tags.0 }}").unwrap(), "x");
        assert_eq!(
            render("{{ item }}").unwrap(),
            r#"{"tags":["x"],"url":"https://a.test/"}"#
        );
        assert_eq!(
            render("{{ item.tags.1 }}"),
            Err(RenderError::MissingItemPath {
                path: "tags.1".into()
            })
        );
        let without_item = RenderContext { item: None, ..ctx };
        assert_eq!(
            Template::parse("{{ item }}").unwrap().render(&without_item),
            Err(RenderError::MissingItem)
        );
    }

    #[test]
    fn rejects_an_unclosed_reference() {
        assert_eq!(
            Template::parse("ab{{ partition"),
            Err(TemplateError::Unclosed(2))
        );
    }

    #[test]
    fn rejects_an_unknown_reference() {
        for text in [
            "{{ }}",
            "{{ date }}",
            "{{ upstream. }}",
            "{{ upstream.a..b }}",
            "{{ run }}",
            "{{ run.node }}",
            "{{ env. }}",
            "{{ env.1A }}",
            "{{ env.A.B }}",
            "{{ env.A-B }}",
            "{{ item. }}",
            "{{ item.a..b }}",
            "{{ items }}",
        ] {
            assert!(
                matches!(
                    Template::parse(text),
                    Err(TemplateError::UnknownReference(_))
                ),
                "{text}"
            );
        }
    }
}
