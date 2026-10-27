//! `freshet submit` — hand a plan to whatever consumes the wire contract.
//!
//! ## What this knows, and what it refuses to know
//!
//! This is the one place in Freshet that opens a socket, so it is worth being
//! precise about how much it is allowed to understand.
//!
//! It knows exactly two things:
//!
//! 1. There is an HTTP endpoint — **whose URL the caller supplies in full** —
//!    that accepts a JSON body with a `plan` key.
//! 2. That key holds the planner's own frozen wire contract ([`freshet_embed`]),
//!    which this crate already produces for `--json`.
//!
//! It does **not** know what a dagron is, that dagron is what is on the other
//! end, what a run graph or a workflow YAML looks like, or what any key outside
//! `plan` means. `--to` has no default for that reason: a default would encode a
//! guess about the consumer's identity and mount path, and the whole architecture
//! (the planner core's first design decision) exists to keep that guess out of the planner.
//!
//! Everything outside `plan` and `graph` is caller-supplied bytes this module
//! forwards without inspecting — see [`build_options`], and the test that proves
//! `{{ model }}` reaches the wire unsubstituted.
//!
//! ## Why there is no `--commit`
//!
//! `plan --commit` advances the fingerprint snapshot, and the discipline is that
//! state advances only after a rebuild has actually *succeeded*. A submit has not
//! run anything — it has created a run that may still fail. Committing here would
//! record "everything is up to date" over models that were never rebuilt, which is
//! the under-rebuild bug this entire planner exists to prevent. So `submit` never
//! writes state, and the run id it prints is what you wait on before calling
//! `freshet plan --commit`.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use freshet_core::Project;
use freshet_embed::PlanResponse;
use serde::Serialize;
use serde_json::{Map, Value};

/// The request body.
///
/// Only `plan` and `graph` are ours. `options` is an opaque object the caller
/// hands us for the consumer, and this type is deliberately the weakest one that
/// can express that: mirroring the consumer's own options struct here would create
/// a second contract to keep in sync, and drift between them would be silent.
#[derive(Debug, Serialize)]
pub struct Envelope {
    pub plan: PlanResponse,
    /// The whole project's edges — every model, not just the ones being rebuilt.
    ///
    /// A plan carries edges among the models it *selected*; only this carries the
    /// ones it skipped, which is what lets a consumer report "rebuilding 3 of 40"
    /// rather than just "rebuilding 3". Nothing but the planner has it, so sending
    /// it is not coupling — it is the one piece of project truth the consumer
    /// cannot recover on its own.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph: Option<BTreeMap<String, Vec<String>>>,
    /// Omitted entirely when empty rather than sent as `{}`: a consumer that
    /// requires a field inside it should be able to say so with its own error,
    /// and `{}` would instead fail as a malformed object before it got the chance.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Value>,
}

/// Every model's upstreams, in name order.
pub fn project_graph(project: &Project) -> BTreeMap<String, Vec<String>> {
    project
        .names()
        .map(|name| {
            let ups = project.get(name).map(|m| m.depends_on.clone()).unwrap_or_default();
            (name.clone(), ups)
        })
        .collect()
}

/// Assemble the `options` object from a file and/or the `--command` shorthand.
///
/// The file's contents are parsed only far enough to confirm they are a JSON
/// object that can be merged into; the *meaning* of what is inside is the
/// consumer's business. `--command` writes exactly one key, and the shell string
/// goes through untouched — placeholders like `{{ model }}` are the consumer's
/// templating, not ours, and substituting them here would be this crate quietly
/// learning the consumer's language.
pub fn build_options(command: Option<&str>, options_file: Option<&Path>) -> Result<Option<Value>> {
    let mut obj: Map<String, Value> = match options_file {
        Some(path) => {
            let bytes = std::fs::read(path)
                .with_context(|| format!("reading options file {}", path.display()))?;
            let parsed: Value = serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing options file {}", path.display()))?;
            match parsed {
                Value::Object(map) => map,
                other => bail!(
                    "{}: options must be a JSON object, found {}",
                    path.display(),
                    kind_of(&other)
                ),
            }
        }
        None => Map::new(),
    };

    if let Some(shell) = command {
        obj.insert(
            "command_template".to_string(),
            Value::Array(vec![
                Value::String("sh".into()),
                Value::String("-c".into()),
                Value::String(shell.to_string()),
            ]),
        );
    }

    Ok(if obj.is_empty() { None } else { Some(Value::Object(obj)) })
}

fn kind_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// What came back. Kept as status + raw body because this module has no business
/// asserting a response schema it does not own.
pub struct Reply {
    pub status: u16,
    pub body: String,
}

impl Reply {
    /// The created run's id, if the consumer named one.
    ///
    /// Best-effort on purpose: a `run_id` string is read when it is there, and its
    /// absence is not an error — a different consumer may answer differently, and
    /// this crate should not be the reason a valid response is rejected.
    pub fn run_id(&self) -> Option<String> {
        serde_json::from_str::<Value>(&self.body)
            .ok()?
            .get("run_id")?
            .as_str()
            .map(str::to_string)
    }
}

/// POST `body` to `url` with the caller's extra headers.
///
/// A 4xx/5xx is returned, not raised: the consumer's own message ("plan is empty",
/// "options.command_template is required") is far more useful to a human than a
/// status code, and it can only be surfaced by reading the body of a failed
/// response.
pub fn post(url: &str, headers: &[String], body: &str) -> Result<Reply> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_secs(60)))
        .build()
        .into();

    let mut req = agent.post(url).header("content-type", "application/json");
    for raw in headers {
        let (name, value) = raw
            .split_once(':')
            .with_context(|| format!("malformed --header `{raw}`: expected `Name: value`"))?;
        req = req.header(name.trim(), value.trim());
    }

    let mut resp = req
        .send(body)
        .with_context(|| format!("posting the plan to {url}"))?;
    let status = resp.status().as_u16();
    let body = resp
        .body_mut()
        .read_to_string()
        .context("reading the response body")?;
    Ok(Reply { status, body })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_command_becomes_one_key_and_keeps_its_placeholders() {
        let opts = build_options(Some("dbt run --select {{ model }}"), None).unwrap().unwrap();
        assert_eq!(
            opts["command_template"],
            serde_json::json!(["sh", "-c", "dbt run --select {{ model }}"]),
            "the shell string must reach the wire byte-for-byte: templating is the \
             consumer's, and expanding it here would be this crate learning their language"
        );
    }

    #[test]
    fn no_command_and_no_file_means_no_options_key_at_all() {
        assert!(build_options(None, None).unwrap().is_none());
    }

    #[test]
    fn a_command_overrides_the_same_key_from_the_file() {
        let dir = std::env::temp_dir().join("freshet-submit-opts-override");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("opts.json");
        std::fs::write(&path, r#"{"command_template":["from-file"],"tags":["nightly"]}"#).unwrap();

        let opts = build_options(Some("from-flag"), Some(&path)).unwrap().unwrap();
        assert_eq!(opts["command_template"], serde_json::json!(["sh", "-c", "from-flag"]));
        assert_eq!(opts["tags"], serde_json::json!(["nightly"]), "other keys survive");
    }

    #[test]
    fn a_non_object_options_file_is_rejected_by_name() {
        let dir = std::env::temp_dir().join("freshet-submit-opts-bad");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("opts.json");
        std::fs::write(&path, "[1,2,3]").unwrap();

        let err = build_options(None, Some(&path)).unwrap_err().to_string();
        assert!(err.contains("must be a JSON object"), "got: {err}");
        assert!(err.contains("an array"), "the error should say what was found: {err}");
    }

    #[test]
    fn a_run_id_is_read_when_present_and_shrugged_off_when_not() {
        let with = Reply { status: 201, body: r#"{"run_id":"run_42","model_count":2}"#.into() };
        assert_eq!(with.run_id().as_deref(), Some("run_42"));

        for body in ["{}", "not json at all", r#"{"run_id":7}"#] {
            let without = Reply { status: 201, body: body.into() };
            assert_eq!(without.run_id(), None, "body {body:?} should not yield an id");
        }
    }
}
