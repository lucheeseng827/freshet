//! freshet-state — incremental state for the backfill planner.
//!
//! dbt Core's killer weakness is that it has *no state*: every change risks a full
//! rebuild because nothing remembers what each model looked like last time. This
//! crate is the missing memory.
//!
//! Two pieces:
//!
//! 1. **Fingerprints.** A content hash per model over its **own definition only**
//!    (normalized SQL, column names, dependency names) — deliberately *not* a
//!    Merkle roll-up of upstream hashes; see [`fingerprint_project`]. Comparing
//!    the current fingerprints to the last persisted ones tells you exactly which
//!    models were directly edited — without diffing the whole project.
//!
//! 2. **[`StateBackend`].** Where fingerprints (and, later, materialized-partition
//!    watermarks) are persisted between runs. The OSS [`FileStateBackend`] writes
//!    JSON to local disk. An optional multi-tenant hosted backend (the
//!    hosted state service, dagron Cloud) implements this same trait.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use freshet_core::Project;
use serde::{Deserialize, Serialize};

/// A content fingerprint of a single model's own definition (see
/// [`fingerprint_project`] for exactly what participates). Stable for a given
/// binary; not guaranteed stable across Rust std versions (`DefaultHasher`).
pub type Fingerprint = u64;

/// The persisted snapshot of a project's state as of the last successful run.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateSnapshot {
    /// model name -> own-definition fingerprint (SQL + column shape + declared
    /// dependency names — not a roll-up of upstream fingerprints; see
    /// [`fingerprint_project`]).
    pub fingerprints: BTreeMap<String, Fingerprint>,
    /// Optional opaque watermark per model (e.g. last materialized partition).
    /// Reserved for partition-scoped backfill; unused in v0.
    #[serde(default)]
    pub watermarks: BTreeMap<String, String>,
}

/// Fingerprint each model by its **own definition only** — normalized SQL, column
/// shape, and the names of its declared dependencies. Deliberately *not* a
/// Merkle roll-up of upstream fingerprints: state's job is to detect which models
/// were *directly* edited; computing the downstream blast radius (column-aware) is
/// [`freshet_core::plan_backfill`]'s job. Rolling upstream hashes in here would
/// mark every transitive downstream as "changed" and defeat column-level pruning.
pub fn fingerprint_project(project: &Project) -> BTreeMap<String, Fingerprint> {
    let mut out: BTreeMap<String, Fingerprint> = BTreeMap::new();
    for name in project.names() {
        let model = project.get(name).expect("names() only yields known models");
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        normalize_sql(&model.sql).hash(&mut hasher);
        // Column shape participates: adding/removing/renaming a column is a change
        // even if the SQL text barely moved.
        for c in &model.columns {
            c.name.hash(&mut hasher);
        }
        // The set of upstreams (names only) — re-pointing a dependency is a change.
        let mut deps: Vec<&String> = model.depends_on.iter().collect();
        deps.sort();
        for up in deps {
            up.hash(&mut hasher);
        }
        out.insert(name.clone(), hasher.finish());
    }
    out
}

/// Models whose own-definition fingerprint differs from (or is absent in)
/// `previous`. These are the inputs to [`freshet_core::plan_backfill`].
pub fn changed_models(
    previous: &StateSnapshot,
    current: &BTreeMap<String, Fingerprint>,
) -> BTreeSet<String> {
    let mut changed = BTreeSet::new();
    for (name, fp) in current {
        match previous.fingerprints.get(name) {
            Some(prev) if prev == fp => {}
            _ => {
                changed.insert(name.clone());
            }
        }
    }
    changed
}

/// Cheap, deterministic SQL normalization for fingerprinting: collapse runs of
/// whitespace and trim. Intentionally not a parser — a real column-lineage pass
/// (idea #9) can replace this with AST-canonical hashing later without changing
/// the state format.
fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Where planner state is persisted between runs. Object-safe so callers can hold
/// a `Box<dyn StateBackend>` and swap the local backend for the hosted one.
pub trait StateBackend {
    /// Load the last persisted snapshot (an empty snapshot if none yet).
    fn load(&self) -> Result<StateSnapshot>;
    /// Persist a snapshot, replacing any previous one. Called *after* a backfill
    /// run succeeds, so the next run diffs against reality.
    fn save(&self, snapshot: &StateSnapshot) -> Result<()>;
}

/// Blanket impl so a shared `&backend` is itself a `StateBackend` — lets several
/// planners reuse one backend without moving or wrapping it in `Arc`.
impl<B: StateBackend + ?Sized> StateBackend for &B {
    fn load(&self) -> Result<StateSnapshot> {
        (**self).load()
    }
    fn save(&self, snapshot: &StateSnapshot) -> Result<()> {
        (**self).save(snapshot)
    }
}

/// OSS local backend: a single JSON file on disk.
pub struct FileStateBackend {
    path: PathBuf,
}

impl FileStateBackend {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }
}

impl StateBackend for FileStateBackend {
    fn load(&self) -> Result<StateSnapshot> {
        match std::fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing state file {}", self.path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(StateSnapshot::default()),
            Err(e) => Err(e).with_context(|| format!("reading state file {}", self.path.display())),
        }
    }

    fn save(&self, snapshot: &StateSnapshot) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("creating state dir {}", parent.display()))?;
            }
        }
        let bytes = serde_json::to_vec_pretty(snapshot).context("serializing state")?;
        // Write-then-rename so a crash mid-write can never leave a half-written
        // (invalid-JSON) state file that would block the next planning run. The
        // temp name is unique per save (pid + process-local counter) so concurrent
        // saves through a shared backend never collide on one temp path; rename is
        // atomic, so a reader sees either the old or the complete new file.
        use std::sync::atomic::{AtomicU64, Ordering};
        static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp = self
            .path
            .with_extension(format!("tmp.{}.{}", std::process::id(), seq));
        std::fs::write(&tmp, bytes)
            .with_context(|| format!("writing temp state file {}", tmp.display()))?;
        std::fs::rename(&tmp, &self.path)
            .with_context(|| format!("replacing state file {}", self.path.display()))
    }
}

/// In-memory backend for tests and ephemeral planning.
#[derive(Default)]
pub struct MemoryStateBackend {
    inner: std::sync::Mutex<StateSnapshot>,
}

impl StateBackend for MemoryStateBackend {
    fn load(&self) -> Result<StateSnapshot> {
        Ok(self.inner.lock().unwrap().clone())
    }
    fn save(&self, snapshot: &StateSnapshot) -> Result<()> {
        *self.inner.lock().unwrap() = snapshot.clone();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freshet_core::{Column, ColumnRef, Materialization, Model};

    fn model(name: &str, sql: &str, deps: &[&str]) -> Model {
        Model {
            name: name.into(),
            sql: sql.into(),
            materialization: Materialization::Table,
            columns: vec![Column::new(
                "x",
                deps.iter().map(|d| ColumnRef::new(*d, "x")).collect(),
            )],
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn fingerprint_detects_only_direct_edits() {
        let v1 = Project::from_models([
            model("a", "select 1 as x", &[]),
            model("b", "select x from a", &["a"]),
        ]);
        let snap = StateSnapshot {
            fingerprints: fingerprint_project(&v1),
            ..Default::default()
        };

        // Edit only `a`'s SQL. `a`'s fingerprint changes; `b`'s own definition did
        // NOT, so `b` is not "changed" at the state level — propagating to `b` is
        // freshet-core's column-aware job, not the fingerprint's.
        let v2 = Project::from_models([
            model("a", "select 2 as x", &[]),
            model("b", "select x from a", &["a"]),
        ]);
        let changed = changed_models(&snap, &fingerprint_project(&v2));
        assert!(changed.contains("a"));
        assert!(
            !changed.contains("b"),
            "downstream impact is the planner's job, not state's"
        );
    }

    #[test]
    fn whitespace_only_change_is_not_a_change() {
        let v1 = Project::from_models([model("a", "select 1 as x", &[])]);
        let v2 = Project::from_models([model("a", "select   1   as x\n", &[])]);
        let fp1 = fingerprint_project(&v1);
        let fp2 = fingerprint_project(&v2);
        assert_eq!(fp1.get("a"), fp2.get("a"));
    }

    #[test]
    fn file_backend_roundtrips() {
        let dir = std::env::temp_dir().join(format!("planner_state_test_{}", std::process::id()));
        let path = dir.join("state.json");
        let be = FileStateBackend::new(&path);
        assert_eq!(be.load().unwrap(), StateSnapshot::default());
        let mut snap = StateSnapshot::default();
        snap.fingerprints.insert("a".into(), 42);
        be.save(&snap).unwrap();
        assert_eq!(be.load().unwrap(), snap);
        std::fs::remove_dir_all(&dir).ok();
    }
}
