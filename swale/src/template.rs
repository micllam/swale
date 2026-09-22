//! The substitution language of a parameter string. A reference is written
//! between `{{` and `}}`, and the language is substitution only.
//!
//! The references:
//!
//! - `{{ partition }}`, the partition of the task instance.
//! - `{{ run.<field> }}`, a field of the run: `id`, the run id of the task
//!   instance, or `summary`, the summary of the graph run as JSON text.
//! - `{{ upstream.<node>.<path> }}`, the output of an upstream node, or the
//!   value at a dotted path into it.
//! - `{{ env.<NAME> }}`, an environment variable of the process that renders
//!   the template.
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
    /// No succeeded record exists for the upstream node, so there is no
    /// output.
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

    /// The names of the upstream nodes the template refers to, in source
    /// order and with repeats.
    pub fn upstream_nodes(&self) -> impl Iterator<Item = &str> {
        self.segments.iter().filter_map(|segment| match segment {
            Segment::Upstream { node, .. } => Some(node.as_str()),
            _ => None,
        })
    }

    /// Renders the template. A string value from an upstream output is
    /// copied as is, and any other JSON value as its JSON text.
    pub fn render(&self, ctx: &RenderContext<'_>) -> Result<String, RenderError> {
        let mut out = String::new();
        for segment in &self.segments {
            match segment {
                Segment::Literal(text) => out.push_str(text),
                Segment::Partition => out.push_str(ctx.partition),
                Segment::Run(RunField::Id) => out.push_str(ctx.run.id),
                Segment::Run(RunField::Summary) => out.push_str(ctx.run.summary),
                Segment::Upstream { node, path } => {
                    let mut value = ctx
                        .upstream
                        .get(node)
                        .ok_or_else(|| RenderError::MissingUpstream { node: node.clone() })?;
                    for key in path {
                        value = match value {
                            serde_json::Value::Object(map) => map.get(key),
                            serde_json::Value::Array(items) => {
                                key.parse::<usize>().ok().and_then(|i| items.get(i))
                            }
                            _ => None,
                        }
                        .ok_or_else(|| RenderError::MissingPath {
                            node: node.clone(),
                            path: path.join("."),
                        })?;
                    }
                    match value {
                        serde_json::Value::String(text) => out.push_str(text),
                        other => out.push_str(&other.to_string()),
                    }
                }
                Segment::Env(name) => {
                    let value = ctx
                        .env
                        .get(name)
                        .ok_or_else(|| RenderError::MissingEnv { name: name.clone() })?;
                    out.push_str(value);
                }
            }
        }
        Ok(out)
    }
}

fn reference(text: &str) -> Result<Segment, TemplateError> {
    match text {
        "partition" => Ok(Segment::Partition),
        _ => {
            let unknown = || TemplateError::UnknownReference(text.to_string());
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
            "a/{{ partition }}/{{run.id}}-{{ run.summary }}{{ upstream.extract.rows.key }}{{ env.TOKEN }}",
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
            ]
        );
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
        let ctx = RenderContext {
            partition: "20260915",
            run: RunContext {
                id: "g-20260915-load-r0",
                summary: "{}",
            },
            upstream: &upstream,
            env: &env,
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
