//! The substitution language of a parameter string. A reference is written
//! between `{{` and `}}`, and the language is substitution only.
//!
//! The references are `{{ partition }}`, `{{ run.id }}`, `{{ run.summary }}`
//! and `{{ upstream.<node>.<path> }}`, where `<path>` is a dotted path into the
//! upstream node's output and can be empty.

use std::collections::BTreeMap;

/// The values a [`Template`] is rendered against.
#[derive(Debug, Clone, Copy)]
pub struct RenderContext<'a> {
    /// The partition of the task instance.
    pub partition: &'a str,
    /// The run id of the task instance.
    pub run_id: &'a str,
    /// The summary of the graph run, JSON text.
    pub run_summary: &'a str,
    /// The output of each upstream node with a succeeded record.
    pub upstream: &'a BTreeMap<String, serde_json::Value>,
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
}

/// One piece of a parsed [`Template`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    /// Text copied to the output unchanged.
    Literal(String),
    /// The partition of the task instance.
    Partition,
    /// The run id of the task instance.
    RunId,
    /// The summary of the graph run.
    RunSummary,
    /// A value from the output of an upstream node: the node's name and the
    /// dotted path into the output, split at the dots.
    Upstream { node: String, path: Vec<String> },
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
                Segment::RunId => out.push_str(ctx.run_id),
                Segment::RunSummary => out.push_str(ctx.run_summary),
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
            }
        }
        Ok(out)
    }
}

fn reference(text: &str) -> Result<Segment, TemplateError> {
    match text {
        "partition" => Ok(Segment::Partition),
        "run.id" => Ok(Segment::RunId),
        "run.summary" => Ok(Segment::RunSummary),
        _ => {
            let unknown = || TemplateError::UnknownReference(text.to_string());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_literals_and_each_reference_in_source_order() {
        let template = Template::parse(
            "a/{{ partition }}/{{run.id}}-{{ run.summary }}{{ upstream.extract.rows.key }}",
        )
        .unwrap();
        assert_eq!(
            template.segments,
            [
                Segment::Literal("a/".into()),
                Segment::Partition,
                Segment::Literal("/".into()),
                Segment::RunId,
                Segment::Literal("-".into()),
                Segment::RunSummary,
                Segment::Upstream {
                    node: "extract".into(),
                    path: vec!["rows".into(), "key".into()],
                },
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
        let ctx = RenderContext {
            partition: "20260915",
            run_id: "g-20260915-load-r0",
            run_summary: "{}",
            upstream: &upstream,
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
