//! Watermarks: which partitions of a model are built, and which are owed.
//!
//! ## Why this exists
//!
//! A code change survives a crash for free: fingerprints are committed only after a
//! successful run, so the next plan diffs against the last *good* state and sees the
//! change again. A **restatement** has no such memory. Nothing in the project changed,
//! so if the run that was rewriting last Tuesday dies half-way, the next plan finds
//! nothing to do and the half-written partitions stay that way.
//!
//! The ledger is that memory. Planning a restatement records its partitions as
//! *owed*; a successful run settles them as *built*; and every plan folds whatever is
//! still owed back into its seed ([`recover_gaps`]). It lives in the reserved
//! [`StateSnapshot::watermarks`] map, one JSON document per model, so the state
//! file's shape is unchanged and a freshet-state reader still loads it.
//!
//! ## The shape (the SQLMesh model)
//!
//! Both sets are **sorted, coalesced, half-open ranges** of partition keys, spelled in
//! the model's grain: `[["2026-06-01", "2026-06-21"]]` is twenty days, not two keys.
//! Recording owed partitions is a union; settling them is a difference that splits a
//! range where it has to ([`IntervalSet::remove`]) — the tombstone.
//!
//! ## Conservative by construction
//!
//! * Recording something owed is written *before* the run, and only ever adds work.
//! * Settling is written only after a run succeeded.
//! * A ledger that cannot be read — garbage JSON, an encoding version this reader does
//!   not know, a key that no longer parses — is treated as owing a **full rebuild** of
//!   its model, never as owing nothing. The full rebuild then writes a clean ledger,
//!   so the state heals itself at the cost of one over-rebuild.

use std::collections::{BTreeMap, BTreeSet};

use freshet_core::{BackfillPlan, BackfillUnit, ChangeSpec, PartitionChange, Project};
use freshet_state::StateSnapshot;
use serde::{Deserialize, Serialize};

use crate::domain::{CalendarDomain, Grain, Partition, PartitionDomain};
use crate::IncrementalConfig;

/// The encoding version [`Watermark`] is written in and the only one read back.
pub const WATERMARK_VERSION: u32 = 2;

/// Sorted, non-overlapping, non-adjacent half-open ranges `[lo, hi)` of ordinals.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IntervalSet {
    ranges: Vec<(i64, i64)>,
}

impl IntervalSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ranges(&self) -> &[(i64, i64)] {
        &self.ranges
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// How many ordinals are covered, saturating.
    pub fn count(&self) -> u64 {
        self.ranges.iter().fold(0u64, |n, (lo, hi)| n.saturating_add(hi.abs_diff(*lo)))
    }

    pub fn contains(&self, n: i64) -> bool {
        self.ranges.iter().any(|&(lo, hi)| lo <= n && n < hi)
    }

    /// Add `[lo, hi)`, coalescing with every range it overlaps or touches.
    pub fn insert(&mut self, lo: i64, hi: i64) {
        if lo >= hi {
            return;
        }
        self.ranges.push((lo, hi));
        self.ranges.sort_unstable();
        let mut merged: Vec<(i64, i64)> = Vec::with_capacity(self.ranges.len());
        for (lo, hi) in self.ranges.drain(..) {
            match merged.last_mut() {
                // `<=`, not `<`: [1,3) and [3,5) are one run of partitions, and
                // storing them apart would make "owed" depend on insertion order.
                Some(last) if lo <= last.1 => last.1 = last.1.max(hi),
                _ => merged.push((lo, hi)),
            }
        }
        self.ranges = merged;
    }

    /// Remove `[lo, hi)`, splitting any range it cuts through.
    pub fn remove(&mut self, lo: i64, hi: i64) {
        if lo >= hi {
            return;
        }
        let mut kept = Vec::with_capacity(self.ranges.len() + 1);
        for &(a, b) in &self.ranges {
            if b <= lo || a >= hi {
                kept.push((a, b));
                continue;
            }
            if a < lo {
                kept.push((a, lo));
            }
            if b > hi {
                kept.push((hi, b));
            }
        }
        self.ranges = kept;
    }

    /// Every covered ordinal, in order. Bound it with [`count`](Self::count) first.
    pub fn ordinals(&self) -> impl Iterator<Item = i64> + '_ {
        self.ranges.iter().flat_map(|&(lo, hi)| lo..hi)
    }
}

/// One model's ledger, as stored in [`StateSnapshot::watermarks`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    /// Encoding version; see [`WATERMARK_VERSION`].
    pub v: u32,
    /// The grain the ranges below are spelled in. Stored rather than read from the
    /// config, so a grain change in the config cannot reinterpret an old ledger.
    pub grain: Grain,
    /// Confirmed built by a successful run: half-open `[from, to)` key ranges.
    #[serde(default)]
    pub built: Vec<(Partition, Partition)>,
    /// Planned by a restatement, not yet confirmed: owed to the next plan.
    #[serde(default)]
    pub pending: Vec<(Partition, Partition)>,
    /// The whole model is owed a rebuild — a restatement widened it to a full one, or
    /// a key it planned could not be placed in the grain.
    #[serde(default)]
    pub pending_full: bool,
}

impl Watermark {
    pub fn new(grain: Grain) -> Self {
        Self { v: WATERMARK_VERSION, grain, built: Vec::new(), pending: Vec::new(), pending_full: false }
    }

    fn domain(&self) -> CalendarDomain {
        CalendarDomain::new(self.grain)
    }

    /// Both sets as ordinals, or `None` if a stored key no longer parses.
    fn sets(&self) -> Option<(IntervalSet, IntervalSet)> {
        let d = self.domain();
        let to_set = |ranges: &[(Partition, Partition)]| {
            let mut set = IntervalSet::new();
            for (from, to) in ranges {
                set.insert(d.ordinal(from)?, d.ordinal(to)?);
            }
            Some(set)
        };
        Some((to_set(&self.built)?, to_set(&self.pending)?))
    }

    fn store(&mut self, built: &IntervalSet, pending: &IntervalSet) {
        let d = self.domain();
        let to_keys = |set: &IntervalSet| {
            set.ranges().iter().map(|&(lo, hi)| (d.partition_at(lo), d.partition_at(hi))).collect()
        };
        self.built = to_keys(built);
        self.pending = to_keys(pending);
    }

    /// Record these partitions as owed. A key the grain cannot place makes the whole
    /// model owed — it cannot be tracked, so it cannot be allowed to be forgotten.
    pub fn mark_pending<'a>(&mut self, keys: impl IntoIterator<Item = &'a Partition>) {
        let Some((built, mut pending)) = self.sets() else {
            self.pending_full = true;
            return;
        };
        let d = self.domain();
        for k in keys {
            match d.ordinal(k) {
                Some(n) => pending.insert(n, n + 1),
                None => self.pending_full = true,
            }
        }
        self.store(&built, &pending);
    }

    /// Record the whole model as owed.
    pub fn mark_pending_full(&mut self) {
        self.pending_full = true;
    }

    /// Settle these partitions: a run rebuilt them successfully.
    pub fn mark_built<'a>(&mut self, keys: impl IntoIterator<Item = &'a Partition>) {
        let Some((mut built, mut pending)) = self.sets() else {
            // Unreadable ranges are dropped rather than trusted; `pending_full` is
            // left alone, because rebuilding *some* partitions does not pay a debt
            // on the whole model.
            *self = Watermark { pending_full: self.pending_full, ..Watermark::new(self.grain) };
            return self.mark_built(keys);
        };
        let d = self.domain();
        for k in keys {
            if let Some(n) = d.ordinal(k) {
                built.insert(n, n + 1);
                pending.remove(n, n + 1);
            }
        }
        self.store(&built, &pending);
    }

    /// Settle the whole model: a run rebuilt all of it, so nothing is owed.
    pub fn mark_built_full(&mut self) {
        self.pending.clear();
        self.pending_full = false;
    }

    /// What this model is owed: `None` when nothing, `Full` when the whole model or
    /// more than `cap` partitions, the owed keys otherwise.
    pub fn owed(&self, cap: usize) -> Option<PartitionChange> {
        if self.pending_full {
            return Some(PartitionChange::Full);
        }
        let Some((_, pending)) = self.sets() else {
            return Some(PartitionChange::Full);
        };
        if pending.is_empty() {
            return None;
        }
        if pending.count() > cap as u64 {
            return Some(PartitionChange::Full);
        }
        let d = self.domain();
        Some(PartitionChange::Partitions(pending.ordinals().map(|n| d.partition_at(n)).collect()))
    }
}

/// Read a model's ledger. `None` when there is none — **and** when there is one this
/// reader cannot trust (garbage, an unknown encoding version); callers that plan from
/// it must treat that second case as owing a full rebuild, which [`recover_gaps`] does.
pub fn read_watermark(snap: &StateSnapshot, model: &str) -> Option<Watermark> {
    let wm: Watermark = serde_json::from_str(snap.watermarks.get(model)?).ok()?;
    (wm.v == WATERMARK_VERSION).then_some(wm)
}

/// Persist a model's ledger. No-op on a serialization failure (cannot happen for
/// `Watermark`, but the planning path never panics).
pub fn write_watermark(snap: &mut StateSnapshot, model: &str, wm: &Watermark) {
    if let Ok(s) = serde_json::to_string(wm) {
        snap.watermarks.insert(model.to_string(), s);
    }
}

/// The grain a model's ledger is kept in: its declared grain, else `fallback`.
fn ledger_grain(cfg: &IncrementalConfig, model: &str, fallback: Grain) -> Grain {
    cfg.grain(model).unwrap_or(fallback)
}

/// Load a model's ledger for update, starting a fresh one when there is none. An
/// entry that exists but cannot be read starts fresh **owing a full rebuild**.
fn ledger_for_update(snap: &StateSnapshot, cfg: &IncrementalConfig, model: &str, fallback: Grain) -> Watermark {
    match (snap.watermarks.contains_key(model), read_watermark(snap, model)) {
        (_, Some(wm)) => wm,
        (true, None) => Watermark { pending_full: true, ..Watermark::new(ledger_grain(cfg, model, fallback)) },
        (false, None) => Watermark::new(ledger_grain(cfg, model, fallback)),
    }
}

/// A restatement was planned: record everything it will rebuild as owed.
///
/// Written **before** the run, which is safe because it only ever adds work: if the
/// run dies, the next plan re-covers it; if the run succeeds, [`record_built`]
/// settles it.
pub fn record_pending(snap: &mut StateSnapshot, cfg: &IncrementalConfig, fallback: Grain, plan: &BackfillPlan) {
    for name in &plan.models {
        let mut wm = ledger_for_update(snap, cfg, name, fallback);
        match plan.unit.get(name) {
            Some(BackfillUnit::Partitions(ps)) if !ps.is_empty() => wm.mark_pending(ps),
            _ => wm.mark_pending_full(),
        }
        write_watermark(snap, name, &wm);
    }
}

/// A plan ran successfully: settle what it rebuilt. Written **only** after the run.
pub fn record_built(snap: &mut StateSnapshot, cfg: &IncrementalConfig, fallback: Grain, plan: &BackfillPlan) {
    for name in &plan.models {
        // A model with no ledger and nothing partition-scoped to record needs none.
        let scoped = matches!(plan.unit.get(name), Some(BackfillUnit::Partitions(ps)) if !ps.is_empty());
        if !scoped && !snap.watermarks.contains_key(name) {
            continue;
        }
        let mut wm = ledger_for_update(snap, cfg, name, fallback);
        match plan.unit.get(name) {
            Some(BackfillUnit::Partitions(ps)) if !ps.is_empty() => wm.mark_built(ps),
            _ => wm.mark_built_full(),
        }
        write_watermark(snap, name, &wm);
    }
}

/// Fold everything still owed into the next plan's seed.
///
/// A recovered model is seeded with **all** its output columns: its data is what
/// changed, so every column is, and seeding fewer would let column pruning skip a
/// downstream that reads the rest. A ledger that cannot be read seeds a full rebuild.
/// Models the project no longer has are skipped — there is nothing left to rebuild.
pub fn recover_gaps(
    project: &Project,
    snap: &StateSnapshot,
    cap: usize,
    seed: &mut BTreeMap<String, ChangeSpec>,
) {
    for name in snap.watermarks.keys() {
        let Some(model) = project.get(name) else { continue };
        let owed = match read_watermark(snap, name) {
            Some(wm) => wm.owed(cap),
            None => Some(PartitionChange::Full),
        };
        let Some(owed) = owed else { continue };
        let entry = seed.entry(name.clone()).or_insert_with(|| ChangeSpec {
            columns: BTreeSet::new(),
            partitions: PartitionChange::Partitions(BTreeSet::new()),
        });
        entry.columns.extend(model.output_columns());
        crate::merge_into(&mut entry.partitions, &owed);
    }
}
