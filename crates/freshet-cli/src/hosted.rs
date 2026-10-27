//! The hosted-state client: `--state https://<edge>/state/v1`.
//!
//! Speaks the hosted state protocol dagron Cloud's edge serves (and any server that
//! implements the same three routes):
//!
//! | Request | Answer |
//! |---|---|
//! | `GET  {url}/current` | `200 {"version": int\|null, "snapshot": StateSnapshot}` |
//! | `POST {url}/commit` `{"expected_version": int\|null, "snapshot", "summary"}` | `200 {"version"}` · `409 {"error", "expected", "found"}` |
//!
//! `{url}` is what `--state` names, in full: like `submit --to`, freshet does not
//! guess where anyone mounted anything. The bearer token comes from
//! [`TOKEN_ENV`] and nowhere else — never a flag (shell history, `ps`) and never the
//! URL (it would be printed in every message that names the URL).
//!
//! ## The snapshot crosses the wire unchanged
//!
//! What a local state file holds *is* a `freshet_state::StateSnapshot`:
//! `{"fingerprints": {model: u64}, "watermarks": {model: string}}`. Fingerprints are
//! the AST-canonical hashes (whose scheme is not stored — `freshet_fingerprint::
//! reconcile` recognises every scheme by recomputing, so nothing about it needs to
//! travel), and each watermark is a model's partition ledger, a JSON document kept
//! as a string. The hosted store keeps exactly that type, so the mapping is the
//! identity: serde_json round-trips u64 fingerprints exactly, and ledger strings are
//! opaque to everything in between.
//!
//! Lossless in both directions, or refused. A server that returns fields this
//! freshet has no slot for is refused rather than planned against: planning without
//! them, then committing, would drop them from hosted state for good. (The server
//! refuses the mirror image — see the hosted state service, dagron Cloud.)

use std::time::Duration;

use anyhow::{bail, Context, Result};
use freshet_state::StateSnapshot;
use serde_json::{json, Value};

/// The environment variable the bearer token is read from.
pub const TOKEN_ENV: &str = "FRESHET_STATE_TOKEN";

/// The largest `current` answer read. The edge caps a commit body well below this;
/// the headroom is for a server with a larger cap, not a figure anyone should hit.
const MAX_REPLY_BYTES: u64 = 64 * 1024 * 1024;

/// A hosted state API and the credential to use it.
pub struct HostedState {
    url: String,
    token: String,
}

/// What a commit came back with.
#[derive(Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    /// Landed: the version the workspace is now at.
    Committed(i64),
    /// Lost the race: the state moved after this plan was computed.
    Conflict {
        expected: Option<i64>,
        found: Option<i64>,
        message: String,
    },
}

impl HostedState {
    /// Validate a `--state` URL and pick up the token from [`TOKEN_ENV`].
    pub fn from_url(url: &str) -> Result<Self> {
        let token = std::env::var(TOKEN_ENV)
            .ok()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        let Some(token) = token else {
            bail!(
                "--state {url} is hosted state: set {TOKEN_ENV} to an API token for the workspace \
                 (a token with the state:commit scope if this run commits)"
            );
        };
        Self::with_token(url, token)
    }

    /// As [`from_url`](Self::from_url), with the token given.
    pub fn with_token(url: &str, token: String) -> Result<Self> {
        // First, and without echoing the URL: a credential in it would otherwise be
        // printed by this very error, and by every later message that names the URL.
        if has_userinfo(url) {
            bail!(
                "--state: credentials in the URL are refused — put the token in {TOKEN_ENV} \
                 instead, where it is never printed"
            );
        }
        let url = url.trim_end_matches('/').to_string();
        let uri: ureq::http::Uri = url
            .parse()
            .with_context(|| format!("--state {url} is not a valid URL"))?;
        let scheme = uri.scheme_str().unwrap_or("");
        if scheme != "http" && scheme != "https" {
            bail!("--state {url}: hosted state is reached over http:// or https://");
        }
        let Some(authority) = uri.authority() else {
            bail!("--state {url}: the URL names no host");
        };
        if uri.query().is_some() || url.contains('#') {
            bail!("--state {url}: the state URL takes no query or fragment; routes are appended to it");
        }
        if scheme == "http" && !is_loopback(authority.host()) {
            eprintln!(
                "warning: --state {url} is plain http:// to {}: the {TOKEN_ENV} bearer token \
                 crosses the network unencrypted; use https://",
                authority.host()
            );
        }
        Ok(Self { url, token })
    }

    /// The API's URL, as given (without a trailing `/`). Safe to print: it carries no
    /// credential ([`with_token`](Self::with_token) refuses one).
    pub fn url(&self) -> &str {
        &self.url
    }

    fn agent() -> ureq::Agent {
        ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(60)))
            .build()
            .into()
    }

    /// `GET {url}/current`: the version (`None` before the first commit) and snapshot.
    pub fn load(&self) -> Result<(Option<i64>, StateSnapshot)> {
        let endpoint = format!("{}/current", self.url);
        let mut resp = Self::agent()
            .get(&endpoint)
            .header("authorization", &format!("Bearer {}", self.token))
            .call()
            .with_context(|| format!("loading hosted state from {endpoint}"))?;
        let status = resp.status().as_u16();
        let body = read_body(&mut resp).with_context(|| format!("reading {endpoint}"))?;
        if status != 200 {
            bail!("{endpoint} refused to load state (HTTP {status}): {}", server_message(&body));
        }
        let reply: Value = serde_json::from_str(&body)
            .with_context(|| format!("{endpoint} answered something other than JSON"))?;
        parse_current(&reply).with_context(|| format!("reading the state {endpoint} returned"))
    }

    /// `POST {url}/commit`: land `snapshot` iff the state is still at `expected`.
    pub fn commit(
        &self,
        expected: Option<i64>,
        snapshot: &StateSnapshot,
        context: Value,
    ) -> Result<CommitOutcome> {
        let endpoint = format!("{}/commit", self.url);
        let body = json!({
            "expected_version": expected,
            "snapshot": snapshot,
            "summary": context,
        })
        .to_string();
        let mut resp = Self::agent()
            .post(&endpoint)
            .header("authorization", &format!("Bearer {}", self.token))
            .header("content-type", "application/json")
            .send(body.as_str())
            .with_context(|| format!("committing hosted state to {endpoint}"))?;
        let status = resp.status().as_u16();
        let reply = read_body(&mut resp).with_context(|| format!("reading {endpoint}"))?;
        match status {
            200 => {
                let version = serde_json::from_str::<Value>(&reply)
                    .ok()
                    .and_then(|v| v.get("version").and_then(Value::as_i64));
                match version {
                    Some(v) => Ok(CommitOutcome::Committed(v)),
                    None => bail!(
                        "{endpoint} accepted the commit but named no version (HTTP 200): {}",
                        reply.trim()
                    ),
                }
            }
            409 => {
                let v: Value = serde_json::from_str(&reply).unwrap_or(Value::Null);
                Ok(CommitOutcome::Conflict {
                    expected: v.get("expected").and_then(Value::as_i64),
                    found: v.get("found").and_then(Value::as_i64),
                    message: server_message(&reply),
                })
            }
            _ => bail!("{endpoint} refused the commit (HTTP {status}): {}", server_message(&reply)),
        }
    }
}

fn read_body(resp: &mut ureq::http::Response<ureq::Body>) -> Result<String> {
    Ok(resp
        .body_mut()
        .with_config()
        .limit(MAX_REPLY_BYTES)
        .read_to_string()?)
}

/// The server's own words: `{"error": …}` when it sent JSON, else the body as-is.
fn server_message(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_else(|| body.trim().to_string())
}

/// Does the URL's authority carry userinfo (`user[:pass]@host`)? Read off the text
/// rather than a parse, so it can be answered before anything prints the URL.
fn has_userinfo(url: &str) -> bool {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    rest.split(['/', '?', '#']).next().unwrap_or("").contains('@')
}

fn is_loopback(host: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// The fields a `StateSnapshot` carries. A reply with any other is refused.
const SNAPSHOT_FIELDS: [&str; 2] = ["fingerprints", "watermarks"];

/// Read `{version, snapshot}`, refusing anything this freshet would not keep whole.
fn parse_current(reply: &Value) -> Result<(Option<i64>, StateSnapshot)> {
    let version = match reply.get("version") {
        Some(Value::Null) => None,
        Some(v) => match v.as_i64() {
            Some(n) if n >= 1 => Some(n),
            _ => bail!("`version` must be null or a positive integer, not {v}"),
        },
        None => bail!("the reply has no `version`"),
    };
    let Some(raw) = reply.get("snapshot") else {
        bail!("the reply has no `snapshot`");
    };
    let Some(fields) = raw.as_object() else {
        bail!("`snapshot` is not an object");
    };
    let unknown: Vec<&str> = fields
        .keys()
        .map(String::as_str)
        .filter(|k| !SNAPSHOT_FIELDS.contains(k))
        .collect();
    if !unknown.is_empty() {
        bail!(
            "hosted state carries field(s) {unknown:?} this freshet does not understand; \
             refusing to plan without them (a commit would drop them for good) — upgrade freshet"
        );
    }
    let snapshot: StateSnapshot =
        serde_json::from_value(raw.clone()).context("`snapshot` is not a planner state snapshot")?;
    Ok((version, snapshot))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hosted(url: &str) -> Result<HostedState> {
        HostedState::with_token(url, "tok".into())
    }

    #[test]
    fn a_state_url_is_normalised_and_checked_before_anything_is_sent() {
        assert_eq!(hosted("https://edge.example/state/v1/").unwrap().url(), "https://edge.example/state/v1");
        assert_eq!(hosted("http://127.0.0.1:9/state/v1").unwrap().url(), "http://127.0.0.1:9/state/v1");

        for (url, says) in [
            ("https://user:hunter2@edge.example/state/v1", "credentials in the URL"),
            ("https://hunter2@edge.example/state/v1", "credentials in the URL"),
            ("ftp://edge.example/state/v1", "http:// or https://"),
            ("https://edge.example/state/v1?tenant=x", "no query"),
            ("https://edge.example/state/v1#x", "no query"),
            ("not a url at all", "not a valid URL"),
        ] {
            let err = hosted(url).err().unwrap_or_else(|| panic!("{url} must be refused")).to_string();
            assert!(err.contains(says), "{url}: {err}");
            assert!(!err.contains("hunter2"), "a refusal must not print the credential: {err}");
        }
        // An `@` past the authority is a path, not a credential.
        assert!(hosted("https://edge.example/state/v1/@team").is_ok());
    }

    #[test]
    fn loopback_hosts_are_recognised() {
        for host in ["127.0.0.1", "localhost", "LOCALHOST", "[::1]", "::1", "127.8.9.10"] {
            assert!(is_loopback(host), "{host}");
        }
        for host in ["edge.example", "10.0.0.1", "[2001:db8::1]", "localhost.evil.example"] {
            assert!(!is_loopback(host), "{host}");
        }
    }

    #[test]
    fn a_current_reply_maps_onto_the_snapshot_exactly() {
        let reply = json!({
            "version": 7,
            "snapshot": {
                "fingerprints": { "max": u64::MAX, "stg": 42 },
                "watermarks": { "events": "{\"v\":2,\"grain\":\"day\"}" }
            }
        });
        let (version, snapshot) = parse_current(&reply).unwrap();
        assert_eq!(version, Some(7));
        assert_eq!(snapshot.fingerprints["max"], u64::MAX, "a u64 fingerprint survives exactly");
        assert_eq!(snapshot.watermarks["events"], "{\"v\":2,\"grain\":\"day\"}", "the ledger is opaque");
        assert_eq!(serde_json::to_value(&snapshot).unwrap(), reply["snapshot"], "and maps back unchanged");

        let cold = json!({ "version": null, "snapshot": { "fingerprints": {}, "watermarks": {} } });
        assert_eq!(parse_current(&cold).unwrap(), (None, StateSnapshot::default()));
    }

    #[test]
    fn a_reply_this_freshet_could_not_keep_whole_is_refused() {
        let extra = json!({
            "version": 1,
            "snapshot": { "fingerprints": {}, "watermarks": {}, "lineage": {} }
        });
        let err = parse_current(&extra).unwrap_err().to_string();
        assert!(err.contains("lineage") && err.contains("upgrade freshet"), "{err}");

        for bad in [
            json!({ "snapshot": { "fingerprints": {} } }),
            json!({ "version": 0, "snapshot": { "fingerprints": {} } }),
            json!({ "version": "1", "snapshot": { "fingerprints": {} } }),
            json!({ "version": 1 }),
            json!({ "version": 1, "snapshot": [] }),
            json!({ "version": 1, "snapshot": { "fingerprints": { "a": -1 } } }),
        ] {
            assert!(parse_current(&bad).is_err(), "{bad} must be refused");
        }
    }

    #[test]
    fn the_servers_own_words_are_kept() {
        assert_eq!(server_message(r#"{"error":"token lacks the state:commit scope"}"#), "token lacks the state:commit scope");
        assert_eq!(server_message("  plain text\n"), "plain text");
    }
}
