//! The two Jinja calls Freshet can resolve without running Jinja.
//!
//! A model without `compiled_code` (the manifest came from `dbt parse`, or this model
//! was not selected by the compile) only has its raw source. Two calls in that source
//! render to something knowable from the manifest alone: `{{ ref('x') }}` /
//! `{{ ref('pkg', 'x') }}` and `{{ source('s', 't') }}` render to a relation, and the
//! manifest records which one in the model's `depends_on`. Everything else — `{% if
//! is_incremental() %}`, `{{ config(...) }}`, a macro call, `{{ var(...) }}` — renders
//! to something only dbt can compute, so a model that still contains any of it after
//! these two are rewritten is not analyzed at all (see [`has_jinja`]).
//!
//! Only the exact, argument-only forms are recognized, whitespace-tolerant inside the
//! braces: `{{ ref("x") }}` yes, `{{ ref('x', v=2) }}` or `{{- ref('x') -}}` no. A form
//! this module does not recognize is left in place, and so it makes the model
//! unanalyzable rather than being guessed at.

/// A recognized call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Call<'a> {
    Ref {
        package: Option<&'a str>,
        name: &'a str,
    },
    Source {
        source: &'a str,
        table: &'a str,
    },
}

/// Replace every recognized call with what `resolve` returns for it. A call `resolve`
/// returns `None` for is left exactly as written.
pub(crate) fn rewrite(raw: &str, mut resolve: impl FnMut(&Call<'_>) -> Option<String>) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(at) = rest.find("{{") {
        out.push_str(&rest[..at]);
        let tail = &rest[at..];
        let Some((call, len)) = parse_call(tail) else {
            // Not a form we recognize: keep the braces, move past them, keep looking.
            out.push_str("{{");
            rest = &tail[2..];
            continue;
        };
        match resolve(&call) {
            Some(replacement) => out.push_str(&replacement),
            None => out.push_str(&tail[..len]),
        }
        rest = &tail[len..];
    }
    out.push_str(rest);
    out
}

/// Whether any Jinja is left: an expression, a statement, or a comment.
///
/// Comments count. They render to nothing, but a model whose source still contains
/// one has not been reduced to plain SQL, and the rule is kept simple enough to state
/// in one line of the report.
pub(crate) fn has_jinja(text: &str) -> bool {
    text.contains("{{") || text.contains("{%") || text.contains("{#")
}

/// Parse one recognized call at the start of `s` (which begins with `{{`), returning
/// it and the number of bytes it spans.
fn parse_call(s: &str) -> Option<(Call<'_>, usize)> {
    let mut p = Cursor { s, at: 0 };
    p.eat("{{")?;
    p.skip_ws();
    let func = p.word()?;
    p.skip_ws();
    p.eat("(")?;
    let mut args: Vec<&str> = Vec::new();
    loop {
        p.skip_ws();
        args.push(p.string_literal()?);
        p.skip_ws();
        if p.eat(",").is_some() {
            continue;
        }
        p.eat(")")?;
        break;
    }
    p.skip_ws();
    p.eat("}}")?;
    let call = match (func, args.as_slice()) {
        ("ref", [name]) => Call::Ref { package: None, name },
        ("ref", [package, name]) => Call::Ref { package: Some(package), name },
        ("source", [source, table]) => Call::Source { source, table },
        _ => return None,
    };
    Some((call, p.at))
}

struct Cursor<'a> {
    s: &'a str,
    at: usize,
}

impl<'a> Cursor<'a> {
    fn rest(&self) -> &'a str {
        &self.s[self.at..]
    }

    fn eat(&mut self, token: &str) -> Option<()> {
        if self.rest().starts_with(token) {
            self.at += token.len();
            Some(())
        } else {
            None
        }
    }

    fn skip_ws(&mut self) {
        let trimmed = self.rest().trim_start();
        self.at = self.s.len() - trimmed.len();
    }

    fn word(&mut self) -> Option<&'a str> {
        let rest = self.rest();
        let len = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        if len == 0 {
            return None;
        }
        self.at += len;
        Some(&rest[..len])
    }

    /// A single- or double-quoted string with no escapes. A backslash makes the call
    /// unrecognized rather than half-understood.
    fn string_literal(&mut self) -> Option<&'a str> {
        let rest = self.rest();
        let quote = rest.chars().next().filter(|c| *c == '\'' || *c == '"')?;
        let body = &rest[1..];
        let end = body.find(quote)?;
        let value = &body[..end];
        if value.contains('\\') || value.is_empty() {
            return None;
        }
        self.at += 1 + end + 1;
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn describe(call: &Call<'_>) -> Option<String> {
        Some(match call {
            Call::Ref { package: None, name } => format!("<{name}>"),
            Call::Ref { package: Some(p), name } => format!("<{p}.{name}>"),
            Call::Source { source, table } => format!("<{source}:{table}>"),
        })
    }

    #[test]
    fn the_three_exact_forms_are_rewritten() {
        let raw = "select * from {{ ref('a') }} join {{ref(\"pkg\",'b')}} using (id) \
                   join {{  source( 'shop' , 'orders' )  }} using (id)";
        assert_eq!(
            rewrite(raw, describe),
            "select * from <a> join <pkg.b> using (id) join <shop:orders> using (id)"
        );
    }

    #[test]
    fn any_other_form_is_left_exactly_as_written() {
        for raw in [
            "select * from {{ ref('a', v=2) }}",
            "select * from {{- ref('a') -}}",
            "select * from {{ ref(model_name) }}",
            "select * from {{ source('only_one') }}",
            "select * from {{ ref('a\\'b') }}",
            "{{ config(materialized='table') }} select 1",
            "select {{ var('x') }}",
        ] {
            let out = rewrite(raw, describe);
            assert_eq!(out, raw, "rewrote a form it should not recognize");
            assert!(has_jinja(&out));
        }
    }

    #[test]
    fn a_call_the_resolver_declines_is_kept() {
        let out = rewrite("select * from {{ ref('a') }}, {{ ref('b') }}", |call| match call {
            Call::Ref { name: "a", .. } => Some("A".into()),
            _ => None,
        });
        assert_eq!(out, "select * from A, {{ ref('b') }}");
    }

    #[test]
    fn statements_and_comments_are_jinja_too() {
        assert!(has_jinja("select 1 {% if is_incremental() %} where x {% endif %}"));
        assert!(has_jinja("{# a note #} select 1"));
        assert!(!has_jinja("select '{' || '}' as braces"));
    }
}
