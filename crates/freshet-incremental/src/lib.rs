//! freshet-incremental — the partition/incremental kernel for the v2 planner.
//!
//! The planner core ships a correct, tested planner brain whose partition propagation is
//! **identity-mapped**: a restated partition of an upstream rebuilds the *same*
//! partition key of each incremental downstream. That is exactly right for aligned,
//! window-1 models and is the byte-for-byte behaviour this crate preserves when its
//! knobs are at their defaults.
//!
//! This crate adds the two things the planner core explicitly deferred (TODOs 1 + 2
//! in its architecture notes):
//!
//! 1. an **ordered partition domain** ([`domain`]): every [`Grain`] — hour, day, ISO
//!    week, month, integer bucket — with a total order, so a key has neighbours, and
//!    a [`Span`], so a key of one grain can be mapped onto another (a changed *day*
//!    rebuilds its *month*; a changed month rebuilds its days); and
//! 2. **persisted watermarks** ([`ledger`]): a per-model interval ledger of built and
//!    owed partitions in the reserved [`StateSnapshot::watermarks`] map, so a
//!    restatement a crash interrupted is not forgotten.
//!
//! It also carries the **replace-op abstraction** ([`replace`]): what a
//! partition-scoped rebuild does to the target relation.
//!
//! ## Extends, never re-implements
//!
//! [`plan_incremental`] hands freshet-core's own planner a [`PartitionMapper`] through
//! `plan_backfill_scoped_with`. Column pruning, attribution, topological order, unit
//! emission and cycle handling all stay in `freshet-core`; this crate decides only
//! what an upstream's partition change *means* to one downstream. An earlier scaffold
//! instead pre-seeded every downstream as directly changed with all its columns,
//! which quietly switched column pruning off for every incremental plan and blamed
//! each downstream on itself.
//!
//! ## Lookback runs forward
//!
//! An [`EdgePolicy`] `window` of N says the downstream's partition `d` *reads* the
//! upstream over the trailing N partitions `d-(N-1) ..= d` — a 7-day rolling sum. A
//! change to upstream partition `p` therefore makes `p ..= p+(N-1)` stale downstream:
//! the change is visible in every later window that contains it. (The scaffold widened
//! backwards, rebuilding days nobody's sum had changed and missing the ones it had.)
//! The window counts **downstream** partitions, after grain mapping.
//!
//! ## The one invariant: over-rebuild is safe, under-rebuild is a data bug
//!
//! Every fallback widens or escalates to [`PartitionChange::Full`] — an unparseable
//! partition key, an impossible date, grains with nothing in common, a refinement that
//! would enumerate more than [`DEFAULT_PARTITION_CAP`] keys, an unreadable ledger. We
//! never narrow. With no grain and no window declared the mapping is the identity and
//! the plan is **byte-identical** to freshet-core's `plan_backfill_scoped`; with windows
//! and one shared grain it is a superset of it. Both are asserted by the tests, as is
//! exactness against a brute-force oracle.

use std::collections::{BTreeMap, BTreeSet};

use freshet_core::{plan_backfill_scoped_with, BackfillPlan, ChangeSpec, PartitionChange, PartitionMapper, Project};
use serde::{Deserialize, Serialize};

pub mod domain;
pub mod ledger;
pub mod replace;

pub use domain::{map_across_grains, CalendarDomain, Grain, Partition, PartitionDomain, Span};
pub use ledger::{
    read_watermark, record_built, record_pending, recover_gaps, write_watermark, IntervalSet, Watermark,
    WATERMARK_VERSION,
};
pub use replace::{PartitionType, ReplaceOp, ReplaceSpec, ReplaceStrategy, ReplaceTarget, Widening};

/// The most partitions one model may be widened to before the planner gives up
/// enumerating them and rebuilds the model whole. A month refined into hours is 744;
/// a coarse integer bucket refined into fine ones can be millions, and a plan carrying
/// millions of keys is a worse outcome than a full rebuild it would have cost anyway.
pub const DEFAULT_PARTITION_CAP: usize = 10_000;

/// Per-upstream-edge trailing window. `window == 1` is identity (the v1 default).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct EdgePolicy {
    /// How many **downstream** partitions back the downstream reads: its partition
    /// `d` reads the upstream data covering `d-(window-1) ..= d`.
    pub window: u32,
}

impl Default for EdgePolicy {
    fn default() -> Self {
        Self { window: 1 }
    }
}

/// Per-model partition config: own grain + per-upstream edge policies + how a
/// rebuild writes.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
// Every knob is independently optional: a model that declares only `replace` must
// parse, and so must one that declares only `grain`. Without this, serde requires
// each field's key to be present and a partial entry fails with "missing field".
#[serde(default)]
pub struct ModelPartitionConfig {
    pub grain: Option<Grain>,
    /// upstream model name -> trailing-window policy for that edge.
    pub edges: BTreeMap<String, EdgePolicy>,
    /// How this model's rows reach the target relation. Absent means undeclared,
    /// which stays undeclared — see [`replace`] on why there is no default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace: Option<ReplaceSpec>,
}

/// The whole project's incremental configuration (sidecar; never required — a model
/// with no entry uses window-1 identity, i.e. v1 behaviour).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct IncrementalConfig {
    pub models: BTreeMap<String, ModelPartitionConfig>,
}

impl IncrementalConfig {
    /// The grain `model` declares, if any.
    pub fn grain(&self, model: &str) -> Option<Grain> {
        self.models.get(model).and_then(|m| m.grain)
    }

    /// The trailing window `downstream` reads `upstream` over; 1 when undeclared.
    pub fn window(&self, upstream: &str, downstream: &str) -> u32 {
        self.models
            .get(downstream)
            .and_then(|m| m.edges.get(upstream))
            .map_or(1, |e| e.window.max(1))
    }

    /// Whether anything here changes partition propagation away from identity.
    pub fn is_identity(&self) -> bool {
        self.models.values().all(|m| m.grain.is_none() && m.edges.values().all(|e| e.window <= 1))
    }
}

// ---------------------------------------------------------------------------
// Planning entrypoint
// ---------------------------------------------------------------------------

/// Plan a partition-aware backfill: freshet-core's planner with this crate's edge
/// mapping.
///
/// `seed` is what changed directly — a code change seeds [`PartitionChange::Full`], a
/// restatement its partitions, recovered gaps ([`recover_gaps`]) theirs. `fallback`
/// is the grain assumed for a model that declares none but sits on an edge with a
/// window wider than 1 (a window needs neighbours, and neighbours need a grain).
pub fn plan_incremental(
    project: &Project,
    cfg: &IncrementalConfig,
    seed: &BTreeMap<String, ChangeSpec>,
    fallback: Grain,
) -> BackfillPlan {
    let seed = normalize_seed(cfg, seed);
    let mapper = EdgeMapper { cfg, fallback, cap: DEFAULT_PARTITION_CAP };
    plan_backfill_scoped_with(project, &seed, &mapper)
}

/// The [`PartitionMapper`] this crate plugs into freshet-core: grain mapping, then a
/// forward window in the downstream's grain.
pub struct EdgeMapper<'a> {
    pub cfg: &'a IncrementalConfig,
    pub fallback: Grain,
    pub cap: usize,
}

impl PartitionMapper for EdgeMapper<'_> {
    fn map(&self, upstream: &str, downstream: &str, change: &PartitionChange) -> PartitionChange {
        let window = self.cfg.window(upstream, downstream);
        let (up_grain, down_grain) = (self.cfg.grain(upstream), self.cfg.grain(downstream));
        if up_grain.is_none() && down_grain.is_none() && window == 1 {
            // Nothing declared on this edge: exactly v1, keys untouched.
            return change.clone();
        }
        let PartitionChange::Partitions(keys) = change else {
            return PartitionChange::Full;
        };
        // An undeclared side inherits the declared one's grain; with neither
        // declared (a window on an otherwise plain edge) both use the fallback.
        let up = CalendarDomain::new(up_grain.or(down_grain).unwrap_or(self.fallback));
        let down = CalendarDomain::new(down_grain.or(up_grain).unwrap_or(self.fallback));
        self.map_keys(&up, &down, keys, window).map_or(PartitionChange::Full, PartitionChange::Partitions)
    }
}

impl EdgeMapper<'_> {
    /// `None` whenever a key cannot be placed or the result outgrows the cap.
    fn map_keys(
        &self,
        up: &CalendarDomain,
        down: &CalendarDomain,
        keys: &BTreeSet<Partition>,
        window: u32,
    ) -> Option<BTreeSet<Partition>> {
        let mut out = BTreeSet::new();
        for key in keys {
            let covering = if up == down {
                vec![down.normalize(key)?]
            } else {
                map_across_grains(up, down, key, self.cap)?
            };
            for d in covering {
                out.extend(down.affected_by(&d, window)?);
            }
            if out.len() > self.cap {
                return None;
            }
        }
        Some(out)
    }
}

/// Canonicalise the seed's keys for models that declare a grain (`2026-6-5` →
/// `2026-06-05`), so a model and its downstreams spell the same partition the same
/// way. A key that does not parse is kept verbatim: the seed is what the caller asked
/// for, and every edge out of it will widen that key to a full rebuild anyway.
fn normalize_seed(cfg: &IncrementalConfig, seed: &BTreeMap<String, ChangeSpec>) -> BTreeMap<String, ChangeSpec> {
    seed.iter()
        .map(|(name, spec)| {
            let partitions = match (cfg.grain(name), &spec.partitions) {
                (Some(g), PartitionChange::Partitions(ps)) => {
                    let d = CalendarDomain::new(g);
                    PartitionChange::Partitions(ps.iter().map(|p| d.normalize(p).unwrap_or_else(|| p.clone())).collect())
                }
                (_, other) => other.clone(),
            };
            (name.clone(), ChangeSpec { columns: spec.columns.clone(), partitions })
        })
        .collect()
}

/// Union `other` into `slot`; `Full` dominates, otherwise set-union.
pub(crate) fn merge_into(slot: &mut PartitionChange, other: &PartitionChange) {
    match (&mut *slot, other) {
        (PartitionChange::Full, _) => {}
        (s @ PartitionChange::Partitions(_), PartitionChange::Full) => *s = PartitionChange::Full,
        (PartitionChange::Partitions(have), PartitionChange::Partitions(add)) => {
            have.extend(add.iter().cloned());
        }
    }
}

#[cfg(test)]
mod tests;
