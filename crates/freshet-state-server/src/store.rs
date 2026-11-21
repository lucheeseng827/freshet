//! The store: every named state's commits, in one directory, a pair of files each.
//!
//! ```text
//! <dir>/.lock                         held by the one server serving <dir>
//! <dir>/<name>/<version>.snapshot.json   the snapshot as of that commit
//! <dir>/<name>/<version>.commit.json     {"version", "committed_at", "summary"}
//! ```
//!
//! A commit's snapshot is written first and its `.commit.json` last, each to a temporary
//! name, synced, then renamed into place. The `.commit.json` is the commit: a version
//! exists if and only if that file does. A crash between the two leaves a snapshot with
//! no commit, which nothing reads and the next commit overwrites, so a crash can lose
//! the commit in flight and nothing else.
//!
//! Compare-and-swap needs one writer, so the server holds the whole store behind one
//! lock, and `<dir>/.lock` keeps a second server process off the same directory.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{bail, Context, Result};
use freshet_state::StateSnapshot;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// One commit, as history lists it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub version: i64,
    /// RFC 3339, UTC, to the second.
    pub committed_at: String,
    /// What the commit changed against the one before it, and the committer's own
    /// words: `{"changed", "removed", "watermarks_changed", "context"}`.
    pub summary: Value,
}

/// What a commit came to.
#[derive(Debug, PartialEq, Eq)]
pub enum Commit {
    /// It landed, and this is the state's version now.
    Landed(i64),
    /// The state is not at the version the commit expected, so nothing was written.
    Conflict {
        expected: Option<i64>,
        found: Option<i64>,
    },
}

/// One page of history, newest first.
#[derive(Debug, PartialEq)]
pub struct Page {
    pub commits: Vec<Entry>,
    /// Pass as `before` for the next page; `None` when no older commit is kept.
    pub next_before: Option<i64>,
}

/// A state as the listing shows it: its newest commit, and how many models it tracks.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Listed {
    pub name: String,
    pub version: i64,
    pub committed_at: String,
    /// Models the newest snapshot fingerprints.
    pub models: usize,
}

/// A state as loaded: every commit still kept, and the newest one's snapshot.
#[derive(Default)]
struct Loaded {
    commits: BTreeMap<i64, Entry>,
    current: StateSnapshot,
}

pub struct Store {
    dir: PathBuf,
    keep: usize,
    states: HashMap<String, Loaded>,
    /// Held for as long as the store is open. Never read: the lock is the point.
    _lock: File,
}

/// Whether `name` can name a state: a single path component, `[A-Za-z0-9._-]`, at
/// most 128 characters, starting with a letter or digit (so never `.` or `..`, and
/// never one of the store's own dot-files).
pub fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphanumeric())
        && name.len() <= 128
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

impl Store {
    /// Open (creating it if need be) the store in `dir`, keeping the newest `keep`
    /// commits of each state. Refuses a directory another server already serves.
    pub fn open(dir: impl Into<PathBuf>, keep: usize) -> Result<Self> {
        let dir = dir.into();
        if keep == 0 {
            bail!("keep must be at least 1: a state's newest commit is the state");
        }
        fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let lock_path = dir.join(".lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("opening {}", lock_path.display()))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => bail!(
                "another freshet-state-server is serving {}: two servers on one directory \
                 would each accept a commit against the same version",
                dir.display()
            ),
            Err(fs::TryLockError::Error(e)) => {
                return Err(e).with_context(|| format!("locking {}", lock_path.display()))
            }
        }
        Ok(Self {
            dir,
            keep,
            states: HashMap::new(),
            _lock: lock,
        })
    }

    /// The state's version (`None` before its first commit) and snapshot. A state never
    /// committed to is empty, as a state file that does not exist yet is.
    pub fn current(&mut self, name: &str) -> Result<(Option<i64>, StateSnapshot)> {
        let state = self.load(name)?;
        Ok((
            state.commits.keys().next_back().copied(),
            state.current.clone(),
        ))
    }

    /// Land `snapshot` if and only if the state is at `expected` (`None`: never
    /// committed to). The summary kept with it is the server's own diff against the
    /// commit before, with `context` (the committer's words) beside it.
    pub fn commit(
        &mut self,
        name: &str,
        expected: Option<i64>,
        snapshot: StateSnapshot,
        context: Value,
        now: SystemTime,
    ) -> Result<Commit> {
        let keep = self.keep;
        let dir = self.dir.join(name);
        let state = self.load(name)?;
        let found = state.commits.keys().next_back().copied();
        if expected != found {
            return Ok(Commit::Conflict { expected, found });
        }
        let version = found.map_or(1, |v| v + 1);
        let entry = Entry {
            version,
            committed_at: crate::rfc3339(now),
            summary: summarize(&state.current, &snapshot, context),
        };

        fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        write_atomically(
            &dir,
            &snapshot_file(version),
            &serde_json::to_vec(&snapshot)?,
        )?;
        write_atomically(&dir, &commit_file(version), &serde_json::to_vec(&entry)?)?;
        // Only now, with both on disk, does the commit exist in memory too.
        state.commits.insert(version, entry);
        state.current = snapshot;

        // Retention is cleanup: the commit has landed, and a file that will not go
        // must not tell its committer otherwise (a retry would then conflict with its
        // own commit). It is said on stderr, and tried again on the next commit.
        while state.commits.len() > keep {
            let Some((&oldest, _)) = state.commits.first_key_value() else {
                break;
            };
            // The commit file first: without it the snapshot is an orphan nothing reads.
            if let Err(e) = remove_if_present(&dir.join(commit_file(oldest))) {
                eprintln!("warning: retention kept v{oldest} of `{name}`: {e:#}");
                break;
            }
            state.commits.remove(&oldest);
            if let Err(e) = remove_if_present(&dir.join(snapshot_file(oldest))) {
                eprintln!("warning: retention left the snapshot of v{oldest} of `{name}`: {e:#}");
            }
        }
        Ok(Commit::Landed(version))
    }

    /// Up to `limit` commits older than `before` (all of them without it), newest first.
    pub fn history(&mut self, name: &str, limit: usize, before: Option<i64>) -> Result<Page> {
        let state = self.load(name)?;
        let older_than = before.unwrap_or(i64::MAX);
        let commits: Vec<Entry> = state
            .commits
            .range(..older_than)
            .rev()
            .take(limit)
            .map(|(_, e)| e.clone())
            .collect();
        let next_before = commits
            .last()
            .filter(|last| state.commits.range(..last.version).next().is_some())
            .map(|last| last.version);
        Ok(Page {
            commits,
            next_before,
        })
    }

    /// Every state with at least one commit, by name.
    pub fn states(&mut self) -> Result<Vec<Listed>> {
        let mut names = Vec::new();
        for entry in
            fs::read_dir(&self.dir).with_context(|| format!("reading {}", self.dir.display()))?
        {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if let Some(name) = entry.file_name().to_str().filter(|n| valid_name(n)) {
                names.push(name.to_string());
            }
        }
        names.sort();
        let mut listed = Vec::new();
        for name in names {
            let state = self.load(&name)?;
            // A directory whose only commit never finished holds no state yet.
            let Some((&version, newest)) = state.commits.last_key_value() else {
                continue;
            };
            listed.push(Listed {
                committed_at: newest.committed_at.clone(),
                models: state.current.fingerprints.len(),
                name,
                version,
            });
        }
        Ok(listed)
    }

    /// `version`'s commit and its snapshot, while the state still keeps it (`None`
    /// once retention has dropped it, or for a version that never landed).
    pub fn version(&mut self, name: &str, version: i64) -> Result<Option<(Entry, StateSnapshot)>> {
        let dir = self.dir.join(name);
        let state = self.load(name)?;
        let Some(entry) = state.commits.get(&version).cloned() else {
            return Ok(None);
        };
        if state.commits.keys().next_back() == Some(&version) {
            return Ok(Some((entry, state.current.clone())));
        }
        let path = dir.join(snapshot_file(version));
        let bytes = fs::read(&path).with_context(|| {
            format!(
                "{} is missing: version {version} is kept, so its snapshot should be too",
                path.display()
            )
        })?;
        let snapshot = serde_json::from_slice(&bytes)
            .with_context(|| format!("{} is not a snapshot", path.display()))?;
        Ok(Some((entry, snapshot)))
    }

    /// The state, read from disk the first time it is asked for.
    fn load(&mut self, name: &str) -> Result<&mut Loaded> {
        if !valid_name(name) {
            bail!("`{name}` is not a state name");
        }
        if !self.states.contains_key(name) {
            let loaded = read_state(&self.dir.join(name))?;
            self.states.insert(name.to_string(), loaded);
        }
        Ok(self.states.get_mut(name).expect("inserted above"))
    }
}

fn read_state(dir: &Path) -> Result<Loaded> {
    let mut loaded = Loaded::default();
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(loaded),
        Err(e) => return Err(e).with_context(|| format!("reading {}", dir.display())),
    };
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(version) = file_name
            .to_str()
            .and_then(|n| n.strip_suffix(".commit.json"))
            .and_then(|v| v.parse::<i64>().ok())
        else {
            continue;
        };
        let path = entry.path();
        let commit: Entry = serde_json::from_slice(
            &fs::read(&path).with_context(|| format!("reading {}", path.display()))?,
        )
        .with_context(|| format!("{} is not a commit", path.display()))?;
        if commit.version != version {
            bail!(
                "{} records version {}, not {version}",
                path.display(),
                commit.version
            );
        }
        loaded.commits.insert(version, commit);
    }
    if let Some(&newest) = loaded.commits.keys().next_back() {
        let path = dir.join(snapshot_file(newest));
        let bytes = fs::read(&path).with_context(|| {
            format!(
                "{} is missing: version {newest} was committed, so its snapshot was written first",
                path.display()
            )
        })?;
        loaded.current = serde_json::from_slice(&bytes)
            .with_context(|| format!("{} is not a snapshot", path.display()))?;
    }
    Ok(loaded)
}

/// The server's record of what a commit changed: models whose fingerprint is new or
/// different, models no longer fingerprinted, and models whose partition ledger moved.
fn summarize(before: &StateSnapshot, after: &StateSnapshot, context: Value) -> Value {
    let changed: Vec<&String> = after
        .fingerprints
        .iter()
        .filter(|(model, fp)| before.fingerprints.get(*model) != Some(fp))
        .map(|(model, _)| model)
        .collect();
    let removed: Vec<&String> = before
        .fingerprints
        .keys()
        .filter(|m| !after.fingerprints.contains_key(*m))
        .collect();
    let mut ledgers: Vec<&String> = after
        .watermarks
        .iter()
        .filter(|(model, w)| before.watermarks.get(*model) != Some(w))
        .map(|(model, _)| model)
        .chain(
            before
                .watermarks
                .keys()
                .filter(|m| !after.watermarks.contains_key(*m)),
        )
        .collect();
    ledgers.sort();
    json!({"changed": changed, "removed": removed, "watermarks_changed": ledgers, "context": context})
}

fn snapshot_file(version: i64) -> String {
    format!("{version}.snapshot.json")
}

fn commit_file(version: i64) -> String {
    format!("{version}.commit.json")
}

/// Write `bytes` to `dir/name` so that a reader sees the old file or the whole new
/// one, and a crash leaves one or the other.
fn write_atomically(dir: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let tmp = dir.join(format!(".{name}.tmp"));
    let target = dir.join(name);
    let mut file = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("writing {}", tmp.display()))?;
    file.sync_all()
        .with_context(|| format!("syncing {}", tmp.display()))?;
    fs::rename(&tmp, &target)
        .with_context(|| format!("renaming {} into place", target.display()))?;
    sync_dir(dir)
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("syncing {}", dir.display()))
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> Result<()> {
    Ok(())
}

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn dir(case: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("freshet-state-store-{case}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn snap(fps: &[(&str, u64)], wms: &[(&str, &str)]) -> StateSnapshot {
        StateSnapshot {
            fingerprints: fps.iter().map(|(m, f)| (m.to_string(), *f)).collect(),
            watermarks: wms
                .iter()
                .map(|(m, w)| (m.to_string(), w.to_string()))
                .collect(),
        }
    }

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn a_commit_lands_only_against_the_version_it_expects() {
        let mut store = Store::open(dir("cas"), 10).unwrap();
        assert_eq!(
            store.current("p").unwrap(),
            (None, StateSnapshot::default())
        );
        let one = snap(&[("a", 1)], &[]);
        assert_eq!(
            store
                .commit("p", None, one.clone(), Value::Null, at(0))
                .unwrap(),
            Commit::Landed(1)
        );
        assert_eq!(
            store
                .commit("p", None, snap(&[], &[]), Value::Null, at(0))
                .unwrap(),
            Commit::Conflict {
                expected: None,
                found: Some(1)
            },
            "a second writer that planned against the empty state loses"
        );
        assert_eq!(store.current("p").unwrap(), (Some(1), one));
        assert_eq!(
            store
                .commit("p", Some(1), snap(&[("a", 2)], &[]), Value::Null, at(0))
                .unwrap(),
            Commit::Landed(2)
        );
        assert_eq!(
            store
                .commit("p", Some(1), snap(&[("a", 3)], &[]), Value::Null, at(0))
                .unwrap(),
            Commit::Conflict {
                expected: Some(1),
                found: Some(2)
            }
        );
    }

    #[test]
    fn a_u64_fingerprint_is_stored_and_read_back_exactly() {
        let path = dir("u64");
        let big = snap(
            &[("m", u64::MAX), ("n", 16_899_773_155_021_735_464)],
            &[("m", r#"{"v":2}"#)],
        );
        {
            let mut store = Store::open(&path, 10).unwrap();
            store
                .commit("p", None, big.clone(), Value::Null, at(0))
                .unwrap();
        }
        let mut reopened = Store::open(&path, 10).unwrap();
        assert_eq!(reopened.current("p").unwrap(), (Some(1), big));
    }

    #[test]
    fn the_summary_is_the_servers_own_diff_with_the_committers_words_beside_it() {
        let mut store = Store::open(dir("summary"), 10).unwrap();
        store
            .commit(
                "p",
                None,
                snap(&[("a", 1), ("b", 1), ("c", 1)], &[("a", "x")]),
                Value::Null,
                at(0),
            )
            .unwrap();
        let context =
            json!({"client": "freshet/0.1.0", "command": "plan --commit", "models_rebuilt": 2});
        store
            .commit(
                "p",
                Some(1),
                snap(&[("a", 1), ("b", 2), ("d", 1)], &[("b", "y")]),
                context.clone(),
                at(60),
            )
            .unwrap();
        let page = store.history("p", 10, None).unwrap();
        assert_eq!(page.commits[0].committed_at, "1970-01-01T00:01:00Z");
        assert_eq!(
            page.commits[0].summary,
            json!({"changed": ["b", "d"], "removed": ["c"], "watermarks_changed": ["a", "b"], "context": context})
        );
        assert_eq!(
            page.commits[1].summary["changed"],
            json!(["a", "b", "c"]),
            "the first commit changes everything"
        );
    }

    #[test]
    fn history_pages_newest_first_with_a_cursor() {
        let mut store = Store::open(dir("pages"), 10).unwrap();
        for v in 0..5 {
            store
                .commit(
                    "p",
                    if v == 0 { None } else { Some(v) },
                    snap(&[("a", v as u64)], &[]),
                    Value::Null,
                    at(0),
                )
                .unwrap();
        }
        let first = store.history("p", 2, None).unwrap();
        assert_eq!(
            first.commits.iter().map(|c| c.version).collect::<Vec<_>>(),
            [5, 4]
        );
        assert_eq!(first.next_before, Some(4));
        let second = store.history("p", 2, first.next_before).unwrap();
        assert_eq!(
            second.commits.iter().map(|c| c.version).collect::<Vec<_>>(),
            [3, 2]
        );
        let last = store.history("p", 2, second.next_before).unwrap();
        assert_eq!(
            last.commits.iter().map(|c| c.version).collect::<Vec<_>>(),
            [1]
        );
        assert_eq!(last.next_before, None, "nothing older");
        assert_eq!(
            store.history("p", 5, None).unwrap().next_before,
            None,
            "a page that holds everything has no next"
        );
        assert_eq!(
            store.history("never", 5, None).unwrap(),
            Page {
                commits: vec![],
                next_before: None
            }
        );
    }

    #[test]
    fn retention_keeps_the_newest_commits_and_their_snapshots_only() {
        let path = dir("keep");
        let mut store = Store::open(&path, 2).unwrap();
        for v in 0..4 {
            store
                .commit(
                    "p",
                    if v == 0 { None } else { Some(v) },
                    snap(&[("a", v as u64)], &[]),
                    Value::Null,
                    at(0),
                )
                .unwrap();
        }
        let page = store.history("p", 10, None).unwrap();
        assert_eq!(
            page.commits.iter().map(|c| c.version).collect::<Vec<_>>(),
            [4, 3]
        );
        assert_eq!(page.next_before, None);
        let mut files: Vec<String> = fs::read_dir(path.join("p"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        files.sort();
        assert_eq!(
            files,
            [
                "3.commit.json",
                "3.snapshot.json",
                "4.commit.json",
                "4.snapshot.json"
            ]
        );
        assert_eq!(
            store.current("p").unwrap().0,
            Some(4),
            "and versions keep counting"
        );
    }

    #[test]
    fn a_retention_that_cannot_delete_still_reports_the_commit_landed() {
        let path = dir("keep-fails");
        let mut store = Store::open(&path, 1).unwrap();
        store
            .commit("p", None, snap(&[("a", 1)], &[]), Value::Null, at(0))
            .unwrap();
        // Something retention cannot remove where v1's commit file is (a directory,
        // which `remove_file` refuses even for root).
        let blocker = path.join("p").join("1.commit.json");
        fs::remove_file(&blocker).unwrap();
        fs::create_dir(&blocker).unwrap();
        fs::write(blocker.join("held"), b"x").unwrap();
        assert_eq!(
            store
                .commit("p", Some(1), snap(&[("a", 2)], &[]), Value::Null, at(0))
                .unwrap(),
            Commit::Landed(2),
            "the commit landed, whatever retention could not do"
        );
        assert_eq!(store.current("p").unwrap().0, Some(2));
        let kept = |store: &mut Store| {
            store
                .history("p", 10, None)
                .unwrap()
                .commits
                .iter()
                .map(|c| c.version)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            kept(&mut store),
            [2, 1],
            "v1 is still on disk, so still listed"
        );

        // Once the obstacle is gone, the next commit's retention catches up.
        fs::remove_dir_all(&blocker).unwrap();
        assert_eq!(
            store
                .commit("p", Some(2), snap(&[("a", 3)], &[]), Value::Null, at(0))
                .unwrap(),
            Commit::Landed(3)
        );
        assert_eq!(kept(&mut store), [3]);
    }

    #[test]
    fn a_snapshot_whose_commit_never_landed_is_ignored_and_overwritten() {
        let path = dir("crash");
        {
            let mut store = Store::open(&path, 10).unwrap();
            store
                .commit("p", None, snap(&[("a", 1)], &[]), Value::Null, at(0))
                .unwrap();
        }
        // What a crash between the two writes of commit 2 leaves behind.
        fs::write(
            path.join("p").join("2.snapshot.json"),
            br#"{"fingerprints":{"a":99},"watermarks":{}}"#,
        )
        .unwrap();
        let mut store = Store::open(&path, 10).unwrap();
        assert_eq!(
            store.current("p").unwrap(),
            (Some(1), snap(&[("a", 1)], &[]))
        );
        assert_eq!(
            store
                .commit("p", Some(1), snap(&[("a", 2)], &[]), Value::Null, at(0))
                .unwrap(),
            Commit::Landed(2)
        );
        drop(store);
        assert_eq!(
            Store::open(&path, 10).unwrap().current("p").unwrap(),
            (Some(2), snap(&[("a", 2)], &[]))
        );
    }

    #[test]
    fn the_listing_names_every_committed_state_and_nothing_else() {
        let path = dir("list");
        {
            let mut store = Store::open(&path, 10).unwrap();
            store
                .commit(
                    "b",
                    None,
                    snap(&[("x", 1), ("y", 2)], &[]),
                    Value::Null,
                    at(60),
                )
                .unwrap();
            store
                .commit("a", None, snap(&[("x", 1)], &[]), Value::Null, at(0))
                .unwrap();
            store
                .commit("a", Some(1), snap(&[("x", 2)], &[]), Value::Null, at(120))
                .unwrap();
            // Asking for a state that was never committed to does not create it.
            store.current("never").unwrap();
        }
        // A crash before a first commit landed, a stray file and a name the store
        // could not have written are not states either.
        fs::create_dir_all(path.join("half")).unwrap();
        fs::write(
            path.join("half").join("1.snapshot.json"),
            br#"{"fingerprints":{}}"#,
        )
        .unwrap();
        fs::write(path.join("notes.txt"), b"x").unwrap();
        fs::create_dir_all(path.join(".hidden")).unwrap();

        let mut store = Store::open(&path, 10).unwrap();
        let listed = store.states().unwrap();
        assert_eq!(
            listed,
            [
                Listed {
                    name: "a".into(),
                    version: 2,
                    committed_at: "1970-01-01T00:02:00Z".into(),
                    models: 1
                },
                Listed {
                    name: "b".into(),
                    version: 1,
                    committed_at: "1970-01-01T00:01:00Z".into(),
                    models: 2
                },
            ]
        );
    }

    #[test]
    fn a_kept_version_reads_back_and_a_dropped_one_is_gone() {
        let mut store = Store::open(dir("versions"), 2).unwrap();
        for (v, fp) in [(None, 1), (Some(1), 2), (Some(2), u64::MAX)] {
            store
                .commit(
                    "p",
                    v,
                    snap(&[("m", fp)], &[]),
                    json!({"run": fp.to_string()}),
                    at(0),
                )
                .unwrap();
        }
        let (entry, old) = store.version("p", 2).unwrap().expect("kept");
        assert_eq!((entry.version, old), (2, snap(&[("m", 2)], &[])));
        assert_eq!(entry.summary["context"], json!({"run": "2"}));
        let (_, newest) = store.version("p", 3).unwrap().expect("the newest is kept");
        assert_eq!(newest, snap(&[("m", u64::MAX)], &[]));
        assert_eq!(store.version("p", 1).unwrap(), None, "retention dropped it");
        assert_eq!(store.version("p", 9).unwrap(), None, "never landed");
        assert_eq!(store.version("other", 1).unwrap(), None);
    }

    #[test]
    fn a_second_server_on_the_same_directory_is_refused() {
        let path = dir("lock");
        let _first = Store::open(&path, 10).unwrap();
        let second = Store::open(&path, 10).err().expect("refused").to_string();
        assert!(
            second.contains("another freshet-state-server is serving"),
            "{second}"
        );
    }

    #[test]
    fn a_name_is_one_safe_path_component() {
        for ok in ["analytics", "team-a.prod", "p_1", "0"] {
            assert!(valid_name(ok), "{ok}");
        }
        for bad in [
            "",
            ".",
            "..",
            ".lock",
            "a/b",
            "../etc",
            "a b",
            "-x",
            &"x".repeat(129),
        ] {
            assert!(!valid_name(bad), "{bad}");
        }
        let mut store = Store::open(dir("names"), 10).unwrap();
        assert!(store.current("../escape").is_err());
    }

    #[test]
    fn keep_zero_is_refused() {
        assert!(Store::open(dir("zero"), 0).is_err());
    }
}
