//! Where planner state lives: a local file, or hosted state (`--state https://…`).
//!
//! Both hold the same document — a `freshet_state::StateSnapshot` of per-model
//! fingerprints and the partition ledger — so everything above this module plans
//! and commits identically against either. What differs is what a commit may
//! overwrite:
//!
//! * **A file** is last-writer-wins, exactly as it always was.
//! * **Hosted state** is versioned, and every commit is a compare-and-swap against
//!   the version this run loaded. If another commit landed in between, nothing is
//!   written and the command exits with [`EXIT_STATE_CONFLICT`]: re-plan against
//!   what the winner recorded, rather than overwrite it.
//!
//! The window that matters is usually longer than one invocation: `plan` → run the
//! plan elsewhere → `plan --commit`. Carry the version across it with
//! `--expect-version`: every hosted `plan` and `submit` prints the version it
//! planned against, and a commit given that version refuses to land on any other.

use std::path::PathBuf;

use anyhow::{Context, Result};
use freshet_state::{FileStateBackend, StateBackend, StateSnapshot};
use serde_json::Value;

/// Exit status when hosted state moved after the plan was computed — a commit that
/// lost its compare-and-swap, or an `--expect-version` the state is no longer at.
/// Distinct from `1` (an error) and `2` (a usage error) so a script can tell "re-plan
/// and try again" from "something is broken".
pub const EXIT_STATE_CONFLICT: i32 = 3;

/// `--expect-version`: the hosted state version a plan was computed against — a
/// version, or `none` for a workspace that had never committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpectedVersion(pub Option<i64>);

/// The clap value parser for `--expect-version`.
pub fn parse_expected_version(raw: &str) -> Result<ExpectedVersion, String> {
    match raw.trim() {
        "none" => Ok(ExpectedVersion(None)),
        s => match s.parse::<i64>() {
            Ok(n) if n >= 1 => Ok(ExpectedVersion(Some(n))),
            _ => Err(format!(
                "`{raw}` is not a hosted state version: an integer >= 1, or `none` for a \
                 workspace that had never committed"
            )),
        },
    }
}

/// Spell a version the way `--expect-version` takes it.
pub fn version_arg(version: Option<i64>) -> String {
    version.map_or_else(|| "none".to_string(), |v| v.to_string())
}

/// Hosted state moved after this plan was computed. `main` answers it with
/// [`EXIT_STATE_CONFLICT`].
#[derive(Debug)]
pub struct StateConflict {
    pub url: String,
    pub expected: Option<i64>,
    pub found: Option<i64>,
}

impl std::fmt::Display for StateConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let at = |v: Option<i64>| v.map_or_else(|| "no state".to_string(), |v| format!("version {v}"));
        write!(
            f,
            "hosted state at {} moved: this plan was computed against {}, and the state is \
             now at {} — another commit landed after this plan was computed; re-plan. Nothing \
             was committed.",
            self.url,
            at(self.expected),
            at(self.found),
        )
    }
}

impl std::error::Error for StateConflict {}

/// Where `--state` points.
pub enum StateLocation {
    File(PathBuf),
    #[cfg(feature = "hosted")]
    Hosted(crate::hosted::HostedState),
}

/// State as loaded, and the version a commit of it must compare against.
pub struct Loaded {
    pub snapshot: StateSnapshot,
    /// `None` for a file; `Some(version)` for hosted state (itself `None` when the
    /// workspace has never committed).
    pub hosted_version: Option<Option<i64>>,
}

impl StateLocation {
    /// Read `--state`: an `http://` or `https://` URL is hosted state, anything else
    /// a file path.
    pub fn parse(arg: &str) -> Result<Self> {
        let lower = arg.trim_start().to_ascii_lowercase();
        if lower.starts_with("http://") || lower.starts_with("https://") {
            #[cfg(feature = "hosted")]
            return Ok(Self::Hosted(crate::hosted::HostedState::from_url(arg.trim())?));
            #[cfg(not(feature = "hosted"))]
            anyhow::bail!(
                "--state names hosted state, and this freshet was built without it (the `hosted` \
                 feature); rebuild with default features, or use a state file"
            );
        }
        Ok(Self::File(PathBuf::from(arg)))
    }

    /// What `--state` named, for messages. A hosted URL carries no credential.
    pub fn describe(&self) -> String {
        match self {
            Self::File(path) => path.display().to_string(),
            #[cfg(feature = "hosted")]
            Self::Hosted(hosted) => hosted.url().to_string(),
        }
    }

    /// Load the state. With `expect`, hosted state must still be at that version —
    /// the one the plan being committed was computed against — or this is a
    /// [`StateConflict`] before anything is planned or written.
    pub fn load(&self, expect: Option<ExpectedVersion>) -> Result<Loaded> {
        match self {
            Self::File(path) => {
                if expect.is_some() {
                    anyhow::bail!(
                        "--expect-version applies to hosted state (an http(s):// --state); {} is a \
                         file, whose writes are last-writer-wins",
                        path.display()
                    );
                }
                let snapshot = FileStateBackend::new(path)
                    .load()
                    .context("loading prior state")?;
                Ok(Loaded { snapshot, hosted_version: None })
            }
            #[cfg(feature = "hosted")]
            Self::Hosted(hosted) => {
                let (version, snapshot) = hosted.load()?;
                if let Some(ExpectedVersion(expected)) = expect {
                    if expected != version {
                        return Err(StateConflict {
                            url: hosted.url().to_string(),
                            expected,
                            found: version,
                        }
                        .into());
                    }
                }
                Ok(Loaded { snapshot, hosted_version: Some(version) })
            }
        }
    }

    /// Commit `next` over `loaded`: a plain write to a file; for hosted state, a
    /// compare-and-swap against the version `loaded` came from, which fails as a
    /// [`StateConflict`] if another commit landed since. `context` rides along as the
    /// commit's summary (hosted state keeps it in history; a file has nowhere to).
    ///
    /// A hosted commit that would change nothing is **not sent**. It would record
    /// nothing, and bumping the version would make every other writer's commit —
    /// each pinned to the version it planned against — conflict, and re-run its
    /// plan, for no reason at all.
    pub fn commit(&self, loaded: &Loaded, next: &StateSnapshot, context: Value) -> Result<Wrote> {
        match self {
            Self::File(path) => {
                // A file keeps no history and compares against nothing.
                let _ = (loaded, context);
                FileStateBackend::new(path).save(next).context("committing state")?;
                Ok(Wrote { to: path.display().to_string(), hosted_version: None, unchanged: false })
            }
            #[cfg(feature = "hosted")]
            Self::Hosted(hosted) => {
                let expected = loaded
                    .hosted_version
                    .context("hosted state is committed only over hosted state it loaded")?;
                if *next == loaded.snapshot {
                    return Ok(Wrote {
                        to: format!("hosted state version {} at {}", version_arg(expected), hosted.url()),
                        hosted_version: expected,
                        unchanged: true,
                    });
                }
                match hosted.commit(expected, next, context)? {
                    crate::hosted::CommitOutcome::Committed(version) => Ok(Wrote {
                        to: format!("hosted state version {version} at {}", hosted.url()),
                        hosted_version: Some(version),
                        unchanged: false,
                    }),
                    crate::hosted::CommitOutcome::Conflict { expected: e, found, .. } => {
                        Err(StateConflict {
                            url: hosted.url().to_string(),
                            // The server's view of what was expected, falling back to ours.
                            expected: e.or(expected),
                            found,
                        }
                        .into())
                    }
                }
            }
        }
    }
}

impl Loaded {
    /// The hosted version this state was loaded at, as `--expect-version` spells it;
    /// `None` for a file.
    pub fn expect_version_arg(&self) -> Option<String> {
        self.hosted_version.map(version_arg)
    }
}

/// What a commit wrote.
pub struct Wrote {
    /// Where, for the message: the file, or the hosted version and URL.
    pub to: String,
    /// The version hosted state is now at; `None` for a file (or a workspace whose
    /// first commit was skipped as unchanged).
    pub hosted_version: Option<i64>,
    /// Hosted state already held exactly this snapshot, so nothing was sent.
    pub unchanged: bool,
}

impl Wrote {
    /// The line a command prints after committing.
    pub fn message(&self) -> String {
        if self.unchanged {
            format!("state unchanged: {} already holds this snapshot — nothing to commit", self.to)
        } else {
            format!("committed state -> {}", self.to)
        }
    }
}

/// The `summary` a freshet commit sends: which client, which command, how much. Kept
/// small — the server bounds it and stores it on every history row.
pub fn commit_context(command: &str, detail: Value) -> Value {
    let mut context = serde_json::json!({
        "client": concat!("freshet/", env!("CARGO_PKG_VERSION")),
        "command": command,
    });
    if let (Some(fields), Value::Object(extra)) = (context.as_object_mut(), detail) {
        fields.extend(extra);
    }
    context
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expect_version_takes_a_version_or_none_and_nothing_else() {
        assert_eq!(parse_expected_version("7"), Ok(ExpectedVersion(Some(7))));
        assert_eq!(parse_expected_version("none"), Ok(ExpectedVersion(None)));
        assert_eq!(parse_expected_version(" 12 "), Ok(ExpectedVersion(Some(12))));
        for bad in ["0", "-1", "null", "None", "", "1.5", "v7"] {
            assert!(parse_expected_version(bad).is_err(), "{bad:?}");
        }
        assert_eq!(version_arg(Some(7)), "7");
        assert_eq!(version_arg(None), "none");
    }

    #[test]
    fn a_conflict_says_what_moved_and_what_to_do() {
        let msg = StateConflict { url: "https://edge/state/v1".into(), expected: Some(3), found: Some(4) }.to_string();
        assert!(msg.contains("computed against version 3"), "{msg}");
        assert!(msg.contains("now at version 4"), "{msg}");
        assert!(msg.contains("another commit landed after this plan was computed; re-plan"), "{msg}");
        let cold = StateConflict { url: "u".into(), expected: None, found: Some(1) }.to_string();
        assert!(cold.contains("computed against no state"), "{cold}");
    }

    #[test]
    fn a_plain_path_is_a_file() {
        assert!(matches!(StateLocation::parse(".planner/state.json").unwrap(), StateLocation::File(_)));
        assert!(matches!(StateLocation::parse("state.http").unwrap(), StateLocation::File(_)));
    }

    /// A build without the HTTP client names what it is missing rather than reading
    /// a URL as a file path and planning cold against a file that will never exist.
    #[cfg(not(feature = "hosted"))]
    #[test]
    fn without_the_hosted_feature_a_url_is_refused_by_name() {
        let err = StateLocation::parse("https://edge.example/state/v1").err().expect("refused").to_string();
        assert!(err.contains("`hosted` feature"), "{err}");
    }

    /// Hosted either way the scheme is spelt — and without a token that is an error,
    /// never a file quietly named `HTTPS:/…`.
    #[cfg(feature = "hosted")]
    #[test]
    fn a_url_is_hosted_state_whatever_the_scheme_case_and_needs_a_token() {
        std::env::remove_var(crate::hosted::TOKEN_ENV);
        for url in ["https://edge.example/state/v1", "HTTPS://edge.example/state/v1", "http://127.0.0.1:1/s"] {
            let err = StateLocation::parse(url).err().expect("no token is set").to_string();
            assert!(err.contains(crate::hosted::TOKEN_ENV), "{err}");
        }
    }

    #[test]
    fn the_commit_context_names_the_client_and_the_command() {
        let c = commit_context("plan --commit", serde_json::json!({ "models_rebuilt": 3 }));
        assert_eq!(c["command"], "plan --commit");
        assert_eq!(c["models_rebuilt"], 3);
        assert!(c["client"].as_str().unwrap().starts_with("freshet/"));
    }
}
