//! freshet-core — the OSS brain of the backfill planner.
//!
//! This crate is deliberately small and dependency-light (no SQL engine, no dbt,
//! no async). It answers one question:
//!
//! > Given a set of changed models (and *which output columns* changed), what is
//! > the **minimal**, topologically-ordered set of models that must be rebuilt?
//!
//! The differentiator vs. naive "rebuild everything downstream" is **column-level
//! pruning**: a downstream model is only impacted if it actually consumes a column
//! that changed. A change to a column nobody reads downstream backfills nothing.
//!
//! Fingerprinting / persisted state lives in `freshet-state`; the embeddable
//! entrypoint that ties graph + state together lives in `freshet-api`. This crate
//! has no opinion on *where* models come from (dbt, SQLMesh, hand-written YAML) or
//! *who* runs the plan (dagron, Airflow, a shell script).

use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// How a model is persisted by the runtime. The planner is granularity-aware:
/// `Incremental` models are partition-scopable (a restatement rebuilds only the
/// affected partitions — see [`plan_backfill_scoped`]); `Table`/`View` rebuild whole.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Materialization {
    Table,
    View,
    Incremental,
}

/// A reference to a specific output column of an upstream model — the atom of
/// column-level lineage.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColumnRef {
    pub model: String,
    pub column: String,
}

impl ColumnRef {
    pub fn new(model: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            column: column.into(),
        }
    }
}

/// One output column of a model and the upstream columns that feed it.
///
/// Populating `upstream` is the job of a SQL column-lineage analyzer (overlaps the
/// project's lineage idea #9). The planner treats it as given: an empty `upstream`
/// means "lineage unknown for this column", which the planner handles
/// conservatively (see [`plan_backfill`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub upstream: Vec<ColumnRef>,
}

impl Column {
    pub fn new(name: impl Into<String>, upstream: Vec<ColumnRef>) -> Self {
        Self {
            name: name.into(),
            upstream,
        }
    }
}

/// A node in the transformation graph: a SQL model and its declared dependencies.
#[derive(Clone, Debug)]
pub struct Model {
    pub name: String,
    /// The (normalized) SQL text. Used by `freshet-state` for fingerprinting; the
    /// core graph logic does not parse it.
    pub sql: String,
    pub materialization: Materialization,
    pub columns: Vec<Column>,
    /// Names of models this one selects from.
    pub depends_on: Vec<String>,
}

impl Model {
    pub fn output_columns(&self) -> BTreeSet<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}

/// The transformation graph. Built once, queried many times.
#[derive(Clone, Debug, Default)]
pub struct Project {
    models: BTreeMap<String, Model>,
    /// Reverse adjacency: model -> models that depend on it. Cached at build time
    /// so downstream traversal is O(edges), not O(models^2).
    downstream: BTreeMap<String, BTreeSet<String>>,
}

impl Project {
    pub fn from_models(models: impl IntoIterator<Item = Model>) -> Self {
        let mut by_name = BTreeMap::new();
        for m in models {
            by_name.insert(m.name.clone(), m);
        }
        let mut downstream: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for m in by_name.values() {
            for up in &m.depends_on {
                downstream
                    .entry(up.clone())
                    .or_default()
                    .insert(m.name.clone());
            }
        }
        Self {
            models: by_name,
            downstream,
        }
    }

    pub fn get(&self, name: &str) -> Option<&Model> {
        self.models.get(name)
    }

    pub fn len(&self) -> usize {
        self.models.len()
    }

    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }

    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.models.keys()
    }

    /// Models directly depending on `name`.
    pub fn downstream_of(&self, name: &str) -> impl Iterator<Item = &String> {
        self.downstream.get(name).into_iter().flatten()
    }

    /// Kahn topological sort over the *whole* project. `Err` carries the models
    /// involved in a cycle (a project misconfiguration).
    pub fn topo_order(&self) -> Result<Vec<String>, Vec<String>> {
        let mut indegree: BTreeMap<&str, usize> =
            self.models.keys().map(|k| (k.as_str(), 0)).collect();
        for m in self.models.values() {
            for up in &m.depends_on {
                if self.models.contains_key(up) {
                    *indegree.get_mut(m.name.as_str()).unwrap() += 1;
                }
            }
        }
        let mut queue: VecDeque<&str> = indegree
            .iter()
            .filter(|(_, d)| **d == 0)
            .map(|(k, _)| *k)
            .collect();
        let mut order = Vec::with_capacity(self.models.len());
        while let Some(n) = queue.pop_front() {
            order.push(n.to_string());
            for d in self.downstream_of(n) {
                let e = indegree.get_mut(d.as_str()).unwrap();
                *e -= 1;
                if *e == 0 {
                    queue.push_back(d.as_str());
                }
            }
        }
        if order.len() == self.models.len() {
            Ok(order)
        } else {
            Err(indegree
                .into_iter()
                .filter(|(_, d)| *d > 0)
                .map(|(k, _)| k.to_string())
                .collect())
        }
    }
}

/// Why a model ended up in the backfill plan — drives both UX ("rebuilding X
/// because Y changed") and audit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackfillReason {
    /// The model's own definition changed.
    DirectlyChanged,
    /// A column this model consumes changed upstream.
    Downstream {
        because_of: String,
        via_columns: Vec<String>,
    },
}

/// The granularity a model is rebuilt at. Models materialized as
/// [`Materialization::Incremental`] are **partition-scopable** — a restatement can
/// rebuild just the affected partitions (the SDF/SQLMesh "minimal partitions"
/// win). `Table`/`View` models always rebuild whole.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackfillUnit {
    FullModel,
    Partitions(Vec<String>),
}

/// How much of a (selected) model is affected, partition-wise. `Full` means the
/// whole model; `Partitions` names a specific, partition-scoped set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionChange {
    Full,
    Partitions(BTreeSet<String>),
}

impl PartitionChange {
    /// Merge two partition changes; `Full` dominates, otherwise sets union.
    /// Returns whether `self` grew (escalated to `Full` or gained partitions).
    fn merge_from(&mut self, other: &PartitionChange) -> bool {
        match (&mut *self, other) {
            (PartitionChange::Full, _) => false,
            (slot @ PartitionChange::Partitions(_), PartitionChange::Full) => {
                *slot = PartitionChange::Full;
                true
            }
            (PartitionChange::Partitions(have), PartitionChange::Partitions(add)) => {
                let before = have.len();
                have.extend(add.iter().cloned());
                have.len() != before
            }
        }
    }
}

/// A single model's change: which output columns changed (for column-aware
/// pruning) and which partitions (for partition-scoped backfill).
#[derive(Clone, Debug)]
pub struct ChangeSpec {
    pub columns: BTreeSet<String>,
    pub partitions: PartitionChange,
}

/// The result: an ordered, minimal set of models to rebuild.
#[derive(Clone, Debug, Default)]
pub struct BackfillPlan {
    /// Models to rebuild, in dependency-safe (topological) order.
    pub models: Vec<String>,
    pub reasons: BTreeMap<String, BackfillReason>,
    pub unit: BTreeMap<String, BackfillUnit>,
}

impl BackfillPlan {
    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }
}

/// Who supplies the compute that *executes* a plan.
///
/// The planner is the brain — it never runs SQL. But the same plan can be handed
/// to two very different worlds, and downstream tooling (metering, quotas, the
/// submit handoff) needs to know which one:
///
/// * [`ComputeMode::Byo`] — **Bring Your Own Compute.** The embedder hosts the
///   runner (their dagron/Airflow/k8s), so the **compute-unit** hook does not fire —
///   there is no compute of ours to account for. A per-plan hook still does: an
///   embedder that counts plans counts them in both modes (the hosted state
///   service's `record_metering` calls `record_backfill` unconditionally and gates only
///   `record_compute_units` on the mode). This is the OSS default and the
///   on-prem/private-cloud deployment shape.
/// * [`ComputeMode::Hosted`] — **someone else's workers run it.** The embedder that
///   supplies them reads the [`ComputeUnits`] this crate estimates (see the
///   `compute_units` helpers) and does whatever metering it does.
///
/// **This crate prices nothing.** It estimates effort in [`ComputeUnits`] and reports
/// the mode; converting either into money is the embedder's job, and doing it here
/// would make the planner un-embeddable by any orchestrator that is not the one whose
/// cost model got compiled in.
///
/// The planner stays compute-agnostic; this enum is metadata the embedder reads
/// to drive metering / quota / submit behavior.
///
/// > **Renamed:** this variant was `Saas`. "SaaS" is a way of selling software, not a
/// > fact about where a plan executes, and naming a business model inside an
/// > Apache-2.0 planner is the same mistake as compiling in a cost model: an
/// > air-gapped enterprise running its own shared workers is `Hosted` and is nobody's
/// > SaaS tenant. `Hosted` states the mechanism — the embedder supplies the compute —
/// > and leaves the commercial framing to whoever is doing the selling. Mechanical
/// > rename: no serde derive, so no wire or config carried the old spelling, and the
/// > crate is unpublished (ADR-0001), so there were no external dependents.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ComputeMode {
    // Fail-safe default: a local user with a FileStateBackend isn't paying anyone to
    // run their compute, and a control-plane wiring bug should never silently start
    // metering a tenant by CU.
    #[default]
    Byo,
    Hosted,
}

/// Predicted execution cost of a plan, in the planner's abstract unit.
///
/// The unit is deliberately small and dimensionless so the estimator stays honest
/// about its precision. One [`ComputeUnits`] is defined as "one full rebuild of one
/// `Table` model" — a quantity of predicted work, with no currency attached. A
/// metering embedder refines the weights post-hoc from real execution telemetry; what
/// this crate publishes is the *prediction*, which callers use to:
///
/// 1. show a "this rebuild is about N CU of work" preview before committing,
/// 2. enforce a [`crate::Materialization`]-blind cap a deployment set for itself
///    (see `freshet_api::ProcessingBudget`), and
/// 3. feed a hosted embedder's metering under [`ComputeMode::Hosted`].
///
/// Stored as `u64` (no fractions) but interpreted in units where one whole CU
/// is `1_000` — i.e. the underlying value is **milliCU**. That gives view models
/// a non-zero cost without needing floats and keeps comparisons exact across
/// add/multiply.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ComputeUnits(pub u64);

impl ComputeUnits {
    /// One whole compute unit, expressed in the internal milliCU scale.
    pub const ONE: ComputeUnits = ComputeUnits(1_000);

    pub fn zero() -> Self {
        ComputeUnits(0)
    }

    /// Convenience: build from a whole CU count (1 CU == `ComputeUnits::ONE`).
    pub fn from_whole(n: u64) -> Self {
        ComputeUnits(n.saturating_mul(1_000))
    }

    /// The whole-CU portion (truncated). Useful for human display / billing
    /// cost models. Use `.0` for the exact milliCU value.
    pub fn as_whole(self) -> u64 {
        self.0 / 1_000
    }

    pub fn saturating_add(self, other: ComputeUnits) -> ComputeUnits {
        ComputeUnits(self.0.saturating_add(other.0))
    }

    pub fn saturating_sub(self, other: ComputeUnits) -> ComputeUnits {
        ComputeUnits(self.0.saturating_sub(other.0))
    }
}

/// Weights the [`estimate_compute_units`] heuristic uses to score a plan.
///
/// These defaults make a [`CostEstimate`] comparable across deployments. They are a
/// definition of predicted effort, not a price: a metering embedder refines them from
/// real execution telemetry, and an on-prem deployment can plug its own [`CostModel`]
/// to make a quota reflect local hardware. Values are in milliCU (so `1_000` ==
/// 1 whole CU; see [`ComputeUnits`]).
#[derive(Clone, Copy, Debug)]
pub struct CostModel {
    /// Cost to fully materialize a `Table`.
    pub table_full: ComputeUnits,
    /// Cost to (re)create a `View` — definition-only, much cheaper than a Table.
    pub view_full: ComputeUnits,
    /// Cost to fully refresh an `Incremental` model (code change → full rebuild).
    pub incremental_full: ComputeUnits,
    /// Cost per partition of a partition-scoped `Incremental` rebuild — the
    /// SDF/SQLMesh "minimal partitions" win shows up here as a lower bill.
    pub incremental_partition: ComputeUnits,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            table_full: ComputeUnits::ONE,
            view_full: ComputeUnits(50),
            incremental_full: ComputeUnits::ONE,
            incremental_partition: ComputeUnits(100),
        }
    }
}

/// What a single model contributes to plan cost. Kept separate from the total
/// so callers can render a per-model breakdown ("mart_a: 1.0 CU; events: 0.4
/// CU for 4 partitions").
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelCost {
    pub model: String,
    pub units: ComputeUnits,
    /// Why this cost — drives the same UX shape as [`BackfillReason`].
    pub basis: CostBasis,
}

/// Why a model costs what it does — the dimension the [`CostModel`] weighted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CostBasis {
    TableFull,
    ViewFull,
    IncrementalFull,
    IncrementalPartitions {
        count: usize,
    },
    /// The plan referenced a model the project no longer knows about — billed
    /// zero rather than crashing the estimator. Should never happen on a plan
    /// produced from the same project, but the planner's tolerance for missing
    /// models in `plan_backfill_scoped` means the estimator has to match.
    UnknownModel,
}

/// Total predicted cost plus a per-model breakdown.
#[derive(Clone, Debug, Default)]
pub struct CostEstimate {
    pub total: ComputeUnits,
    pub per_model: Vec<ModelCost>,
}

/// Score a plan against a [`CostModel`]. Pure: reads the plan + project, returns
/// a [`CostEstimate`]. Use the default weights for a figure comparable across
/// deployments; pass a custom one when an embedder wants quotas that reflect their
/// hardware.
///
/// **Effort, not money.** A [`ComputeUnits`] is defined as one full rebuild of one
/// [`Materialization::Table`] model — a unit of predicted work, with no currency
/// attached and no rate to convert it at. Column-level pruning and partition scoping
/// translate directly into smaller numbers here, which is the property a metering
/// embedder can build on; what it is worth is that embedder's decision.
pub fn estimate_compute_units(
    project: &Project,
    plan: &BackfillPlan,
    cost: &CostModel,
) -> CostEstimate {
    let mut total = ComputeUnits::zero();
    let mut per_model = Vec::with_capacity(plan.models.len());
    for name in &plan.models {
        let Some(model) = project.get(name) else {
            per_model.push(ModelCost {
                model: name.clone(),
                units: ComputeUnits::zero(),
                basis: CostBasis::UnknownModel,
            });
            continue;
        };
        let (units, basis) = match (model.materialization, plan.unit.get(name)) {
            (Materialization::View, _) => (cost.view_full, CostBasis::ViewFull),
            (Materialization::Table, _) => (cost.table_full, CostBasis::TableFull),
            (Materialization::Incremental, Some(BackfillUnit::Partitions(ps))) => {
                let count = ps.len();
                let units = ComputeUnits(cost.incremental_partition.0.saturating_mul(count as u64));
                (units, CostBasis::IncrementalPartitions { count })
            }
            (Materialization::Incremental, _) => {
                (cost.incremental_full, CostBasis::IncrementalFull)
            }
        };
        total = total.saturating_add(units);
        per_model.push(ModelCost {
            model: name.clone(),
            units,
            basis,
        });
    }
    CostEstimate { total, per_model }
}

/// What the same change would cost **without pruning**: every model the plan's
/// directly-changed models reach downstream — them included — rebuilt whole. dbt's
/// `--full-refresh -s changed+`, priced by the same [`CostModel`].
///
/// The gap to [`estimate_compute_units`] of the plan is the rebuild work the planner
/// avoided — the product's north-star metric (`docs/PRODUCT_ROADMAP.md` §7) — and
/// [`avoided_compute_units`] computes it. Model-level and column-level pruning only
/// ever shrink the plan below this baseline. Partition scoping usually does too, but
/// not always: under the default weights more than ten partitions of an incremental
/// model cost more than one full refresh of it, so the difference saturates at zero
/// rather than claiming a negative saving.
pub fn estimate_unpruned(project: &Project, plan: &BackfillPlan, cost: &CostModel) -> CostEstimate {
    let mut reach: BTreeSet<String> = BTreeSet::new();
    let mut stack: Vec<String> = plan
        .reasons
        .iter()
        .filter(|(_, reason)| matches!(reason, BackfillReason::DirectlyChanged))
        .map(|(model, _)| model.clone())
        .collect();
    while let Some(model) = stack.pop() {
        if reach.insert(model.clone()) {
            stack.extend(project.downstream_of(&model).cloned());
        }
    }
    let whole = BackfillPlan {
        models: reach.into_iter().collect(),
        reasons: BTreeMap::new(),
        unit: BTreeMap::new(),
    };
    estimate_compute_units(project, &whole, cost)
}

/// Rebuild work the plan avoids: [`estimate_unpruned`] minus
/// [`estimate_compute_units`], never below zero.
pub fn avoided_compute_units(project: &Project, plan: &BackfillPlan, cost: &CostModel) -> ComputeUnits {
    let unpruned = estimate_unpruned(project, plan, cost).total;
    let planned = estimate_compute_units(project, plan, cost).total;
    ComputeUnits(unpruned.0.saturating_sub(planned.0))
}

fn is_partition_scopable(model: &Model) -> bool {
    matches!(model.materialization, Materialization::Incremental)
}

/// Column-aware backfill at whole-model granularity (every selected model rebuilds
/// fully). Thin wrapper over [`plan_backfill_scoped`] — use that for partition
/// scoping. `changed` maps each directly-changed model to its changed columns.
pub fn plan_backfill(
    project: &Project,
    changed: &BTreeMap<String, BTreeSet<String>>,
) -> BackfillPlan {
    let scoped: BTreeMap<String, ChangeSpec> = changed
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                ChangeSpec {
                    columns: v.clone(),
                    partitions: PartitionChange::Full,
                },
            )
        })
        .collect();
    plan_backfill_scoped(project, &scoped)
}

/// How an upstream's partition change is seen by a partition-scopable downstream.
///
/// [`plan_backfill_scoped`] maps by **identity** — a changed `2026-06-20` upstream
/// rebuilds `2026-06-20` downstream — which is right for aligned, window-1 models.
/// A downstream that reads a trailing window of its upstream, or is partitioned at a
/// different grain, needs a different answer; `freshet-incremental` (Freshet)
/// supplies one through [`plan_backfill_scoped_with`] without re-implementing the
/// column pruning, attribution and ordering below.
///
/// The one rule an implementation must keep: **never narrower than the truth**. When
/// a key cannot be placed, answer [`PartitionChange::Full`] — over-rebuild is safe,
/// under-rebuild is a silent data bug.
pub trait PartitionMapper {
    /// `change`, a partition change on `upstream`, as the change it causes on
    /// `downstream`. Called only for partition-scopable downstreams; a table or view
    /// downstream is always rebuilt in full whatever this returns.
    fn map(&self, upstream: &str, downstream: &str, change: &PartitionChange) -> PartitionChange;
}

/// The v1 mapping: every key maps to itself.
pub struct IdentityMapper;

impl PartitionMapper for IdentityMapper {
    fn map(&self, _upstream: &str, _downstream: &str, change: &PartitionChange) -> PartitionChange {
        change.clone()
    }
}

/// Compute the minimal, column- **and** partition-aware backfill.
///
/// * `changed` — each directly-changed model with its changed columns and
///   partition extent. A code edit uses [`PartitionChange::Full`]; a data
///   restatement uses [`PartitionChange::Partitions`].
///
/// Column propagation is unchanged from [`plan_backfill`]: a downstream model is
/// included only if it consumes a changed column (empty lineage ⇒ conservative).
///
/// Partition propagation uses **identity mapping**: a partition-scopable
/// (incremental) downstream inherits the upstream's affected partitions for the
/// same keys; a non-scopable downstream (or a `Full` upstream) becomes `Full`.
/// Lookback windows and non-aligned grains go through [`plan_backfill_scoped_with`]
/// with a [`PartitionMapper`] that knows the ordered partition domain.
pub fn plan_backfill_scoped(
    project: &Project,
    changed: &BTreeMap<String, ChangeSpec>,
) -> BackfillPlan {
    plan_backfill_scoped_with(project, changed, &IdentityMapper)
}

/// [`plan_backfill_scoped`] with a caller-supplied partition mapping per edge.
///
/// Everything but the partition contribution of an edge is identical — column
/// pruning, attribution, topological order, cycle handling — so a mapper can widen
/// what a downstream rebuilds but never changes *whether* it is selected.
pub fn plan_backfill_scoped_with(
    project: &Project,
    changed: &BTreeMap<String, ChangeSpec>,
    mapper: &dyn PartitionMapper,
) -> BackfillPlan {
    let mut changed_cols: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut part: BTreeMap<String, PartitionChange> = BTreeMap::new();
    let mut reasons: BTreeMap<String, BackfillReason> = BTreeMap::new();
    let mut selected: BTreeSet<String> = BTreeSet::new();

    let mut queue: VecDeque<String> = VecDeque::new();
    for (name, spec) in changed {
        let model = match project.get(name) {
            Some(m) => m,
            None => continue,
        };
        selected.insert(name.clone());
        reasons
            .entry(name.clone())
            .or_insert(BackfillReason::DirectlyChanged);
        changed_cols.insert(name.clone(), spec.columns.clone());
        // A directly-changed model that can't be partition-scoped is always Full.
        let p = if is_partition_scopable(model) {
            spec.partitions.clone()
        } else {
            PartitionChange::Full
        };
        part.insert(name.clone(), p);
        queue.push_back(name.clone());
    }

    while let Some(current) = queue.pop_front() {
        let cur_changed = changed_cols.get(&current).cloned().unwrap_or_default();
        let cur_part = part.get(&current).cloned().unwrap_or(PartitionChange::Full);
        // A model with no known output columns cannot say *which* of its columns
        // changed, so every reference to it counts as a reference to a changed column.
        // Without this, `cur_changed` is empty for it and the blast radius stops at the
        // one model lineage could not see into.
        let cur_opaque = project.get(&current).is_some_and(|m| m.columns.is_empty());
        let down: Vec<String> = project.downstream_of(&current).cloned().collect();
        for d in down {
            let model = match project.get(&d) {
                Some(m) => m,
                None => continue,
            };
            // Which of d's output columns are impacted by current's changed columns?
            let mut impacted_outputs: BTreeSet<String> = BTreeSet::new();
            // A downstream with no known output columns reads an unknown subset of what
            // it depends on — the same conservative answer as an empty `upstream`. The
            // column loop below cannot reach that conclusion for it: it has no columns
            // to loop over, and used to skip the model silently.
            let reads_current = model.depends_on.iter().any(|u| u == &current);
            let unknown_lineage_dependent = model.columns.is_empty() && reads_current;
            for col in &model.columns {
                if col.upstream.is_empty() {
                    // Empty upstream means "may depend on anything this model reads":
                    // this column changed, whatever else did. Leaving it out because a
                    // sibling column was hit would stop the change here for every
                    // downstream that reads only this column.
                    if reads_current {
                        impacted_outputs.insert(col.name.clone());
                    }
                    continue;
                }
                let hit = col
                    .upstream
                    .iter()
                    .any(|r| r.model == current && (cur_opaque || cur_changed.contains(&r.column)));
                if hit {
                    impacted_outputs.insert(col.name.clone());
                }
            }

            if impacted_outputs.is_empty() && !unknown_lineage_dependent {
                continue;
            }

            let new_changed = if unknown_lineage_dependent && impacted_outputs.is_empty() {
                model.output_columns()
            } else {
                impacted_outputs
            };

            // Partition contribution: mapped when d is scopable, else Full.
            let contrib = if is_partition_scopable(model) {
                mapper.map(&current, &d, &cur_part)
            } else {
                PartitionChange::Full
            };

            let cols_entry = changed_cols.entry(d.clone()).or_default();
            let cols_grew = new_changed.iter().any(|c| !cols_entry.contains(c));
            cols_entry.extend(new_changed.iter().cloned());

            let part_entry = part
                .entry(d.clone())
                .or_insert(PartitionChange::Partitions(BTreeSet::new()));
            let part_grew = part_entry.merge_from(&contrib);

            let newly_selected = selected.insert(d.clone());
            if newly_selected {
                reasons.insert(
                    d.clone(),
                    BackfillReason::Downstream {
                        because_of: current.clone(),
                        via_columns: new_changed.iter().cloned().collect(),
                    },
                );
            }
            // Re-enqueue only when something actually grew (and isn't already
            // queued) — re-queuing on "not in queue" alone never terminates on a
            // cyclic project, before topo_order's cycle handling runs. Being selected
            // for the first time counts as growth: a model whose change set happens to
            // be empty (it has no known columns) must still have its own downstreams
            // visited, or the traversal ends at it.
            if (newly_selected || cols_grew || part_grew) && queue.iter().all(|q| q != &d) {
                queue.push_back(d.clone());
            }
        }
    }

    // Emit in topological order so a runner can execute the plan directly.
    let order = project
        .topo_order()
        .unwrap_or_else(|_| selected.iter().cloned().collect());
    let mut models = Vec::new();
    let mut unit = BTreeMap::new();
    for n in order {
        if selected.contains(&n) {
            let u = match part.get(&n) {
                Some(PartitionChange::Partitions(ps)) => {
                    BackfillUnit::Partitions(ps.iter().cloned().collect())
                }
                _ => BackfillUnit::FullModel,
            };
            unit.insert(n.clone(), u);
            models.push(n);
        }
    }

    BackfillPlan {
        models,
        reasons,
        unit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, ups: &[(&str, &str)]) -> Column {
        Column::new(
            name,
            ups.iter().map(|(m, c)| ColumnRef::new(*m, *c)).collect(),
        )
    }

    fn model(name: &str, deps: &[&str], cols: Vec<Column>) -> Model {
        Model {
            name: name.into(),
            sql: format!("select * from {name}"),
            materialization: Materialization::Table,
            columns: cols,
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
        }
    }

    // raw -> stg(a,b) -> mart_a(uses stg.a), mart_b(uses stg.b)
    fn sample() -> Project {
        Project::from_models([
            model("raw", &[], vec![col("a", &[]), col("b", &[])]),
            model(
                "stg",
                &["raw"],
                vec![col("a", &[("raw", "a")]), col("b", &[("raw", "b")])],
            ),
            model("mart_a", &["stg"], vec![col("x", &[("stg", "a")])]),
            model("mart_b", &["stg"], vec![col("y", &[("stg", "b")])]),
        ])
    }

    #[test]
    fn column_pruning_excludes_unaffected_branch() {
        let p = sample();
        // Only stg.a changed -> mart_a rebuilds, mart_b must NOT.
        let mut changed = BTreeMap::new();
        changed.insert("stg".to_string(), BTreeSet::from(["a".to_string()]));
        let plan = plan_backfill(&p, &changed);
        assert!(plan.models.contains(&"stg".to_string()));
        assert!(plan.models.contains(&"mart_a".to_string()));
        assert!(
            !plan.models.contains(&"mart_b".to_string()),
            "mart_b reads stg.b, not stg.a"
        );
    }

    #[test]
    fn topological_order_is_respected() {
        let p = sample();
        let mut changed = BTreeMap::new();
        changed.insert(
            "raw".to_string(),
            BTreeSet::from(["a".to_string(), "b".to_string()]),
        );
        let plan = plan_backfill(&p, &changed);
        let pos = |n: &str| plan.models.iter().position(|m| m == n).unwrap();
        assert!(pos("raw") < pos("stg"));
        assert!(pos("stg") < pos("mart_a"));
    }

    #[test]
    fn unknown_lineage_is_conservative() {
        // mart with no column lineage must rebuild on any upstream change.
        let p = Project::from_models([
            model("u", &[], vec![col("a", &[])]),
            model("d", &["u"], vec![col("z", &[])]), // empty upstream = unknown
        ]);
        let mut changed = BTreeMap::new();
        changed.insert("u".to_string(), BTreeSet::from(["a".to_string()]));
        let plan = plan_backfill(&p, &changed);
        assert!(plan.models.contains(&"d".to_string()));
    }

    fn incremental(name: &str, deps: &[&str], cols: Vec<Column>) -> Model {
        Model {
            materialization: Materialization::Incremental,
            ..model(name, deps, cols)
        }
    }

    // A restatement changes the data of those partitions, hence ALL columns of the
    // restated model — column pruning still applies to what *downstream* reads.
    fn restate(model: &str, cols: &[&str], parts: &[&str]) -> BTreeMap<String, ChangeSpec> {
        BTreeMap::from([(
            model.to_string(),
            ChangeSpec {
                columns: cols.iter().map(|s| s.to_string()).collect(),
                partitions: PartitionChange::Partitions(
                    parts.iter().map(|s| s.to_string()).collect(),
                ),
            },
        )])
    }

    // raw -> stg(incremental) -> mart(incremental), all carrying column lineage so
    // pruning doesn't short-circuit partition logic.
    fn incremental_chain() -> Project {
        Project::from_models([
            incremental("stg", &[], vec![col("a", &[])]),
            incremental("mart", &["stg"], vec![col("a", &[("stg", "a")])]),
        ])
    }

    #[test]
    fn restatement_scopes_partitions_through_incrementals() {
        let p = incremental_chain();
        let plan = plan_backfill_scoped(&p, &restate("stg", &["a"], &["2026-06-01", "2026-06-02"]));
        // Identity mapping: mart inherits the same partition keys.
        assert_eq!(
            plan.unit["stg"],
            BackfillUnit::Partitions(vec!["2026-06-01".into(), "2026-06-02".into()])
        );
        assert_eq!(
            plan.unit["mart"],
            BackfillUnit::Partitions(vec!["2026-06-01".into(), "2026-06-02".into()])
        );
    }

    #[test]
    fn non_incremental_downstream_falls_back_to_full() {
        // stg incremental, mart is a Table -> can't partition-scope mart.
        let p = Project::from_models([
            incremental("stg", &[], vec![col("a", &[])]),
            model("mart", &["stg"], vec![col("a", &[("stg", "a")])]),
        ]);
        let plan = plan_backfill_scoped(&p, &restate("stg", &["a"], &["2026-06-01"]));
        assert_eq!(
            plan.unit["stg"],
            BackfillUnit::Partitions(vec!["2026-06-01".into()])
        );
        assert_eq!(
            plan.unit["mart"],
            BackfillUnit::FullModel,
            "Table model can't be scoped"
        );
    }

    #[test]
    fn code_change_to_incremental_is_full_refresh() {
        let p = incremental_chain();
        // plan_backfill (the whole-model wrapper) marks Full even for incrementals.
        let plan = plan_backfill(
            &p,
            &BTreeMap::from([("stg".to_string(), BTreeSet::from(["a".to_string()]))]),
        );
        assert_eq!(plan.unit["stg"], BackfillUnit::FullModel);
        assert_eq!(plan.unit["mart"], BackfillUnit::FullModel);
    }

    #[test]
    fn cyclic_project_terminates() {
        // a <-> b cycle. Must not hang; topo_order falls back, plan still returns.
        let p = Project::from_models([
            model("a", &["b"], vec![col("x", &[("b", "x")])]),
            model("b", &["a"], vec![col("x", &[("a", "x")])]),
        ]);
        let plan = plan_backfill(
            &p,
            &BTreeMap::from([("a".to_string(), BTreeSet::from(["x".to_string()]))]),
        );
        assert!(plan.models.contains(&"a".to_string()));
        assert!(plan.models.contains(&"b".to_string()));
    }

    #[test]
    fn full_upstream_escalates_scoped_downstream_to_full() {
        // If stg is Full, an incremental mart can't stay partition-scoped.
        let p = incremental_chain();
        let change = BTreeMap::from([(
            "stg".to_string(),
            ChangeSpec {
                columns: BTreeSet::from(["a".to_string()]),
                partitions: PartitionChange::Full,
            },
        )]);
        let plan = plan_backfill_scoped(&p, &change);
        assert_eq!(plan.unit["mart"], BackfillUnit::FullModel);
    }

    #[test]
    fn a_downstream_with_no_known_columns_is_still_rebuilt() {
        // Lineage produced no output columns for `d` (a VALUES body, an unanalysable
        // statement, a hand-built model). It cannot say which upstream columns it
        // reads, so it reads all of them — the same answer as an empty `upstream`.
        let p = Project::from_models([
            model("u", &[], vec![col("a", &[])]),
            model("d", &["u"], vec![]),
        ]);
        let plan = plan_backfill(&p, &BTreeMap::from([("u".to_string(), BTreeSet::from(["a".to_string()]))]));
        assert!(plan.models.contains(&"d".to_string()), "a zero-column downstream was silently skipped");
    }

    #[test]
    fn what_reads_a_zero_column_model_is_rebuilt_through_it() {
        // `d` has no known columns, so "which of d's columns changed" is unknowable —
        // everything that reads d must be treated as reading a changed column.
        let p = Project::from_models([
            model("u", &[], vec![col("a", &[])]),
            model("d", &["u"], vec![]),
            model("e", &["d"], vec![col("x", &[("d", "x")])]),
        ]);
        let plan = plan_backfill(&p, &BTreeMap::from([("u".to_string(), BTreeSet::from(["a".to_string()]))]));
        assert!(plan.models.contains(&"e".to_string()), "the blast radius stopped at the model it could not see into");
        let pos = |n: &str| plan.models.iter().position(|m| m == n).unwrap();
        assert!(pos("u") < pos("d") && pos("d") < pos("e"));
    }

    /// A mapper that widens every key to itself and the next day's key, standing in
    /// for a trailing window — just enough to prove the hook is used per edge.
    struct PlusNext;
    impl PartitionMapper for PlusNext {
        fn map(&self, _up: &str, _down: &str, change: &PartitionChange) -> PartitionChange {
            match change {
                PartitionChange::Full => PartitionChange::Full,
                PartitionChange::Partitions(ps) => PartitionChange::Partitions(
                    ps.iter().flat_map(|p| [p.clone(), format!("{p}+1")]).collect(),
                ),
            }
        }
    }

    #[test]
    fn the_identity_mapper_is_exactly_the_v1_planner() {
        let p = incremental_chain();
        let change = restate("stg", &["a"], &["2026-06-01", "2026-06-02"]);
        let v1 = plan_backfill_scoped(&p, &change);
        let hooked = plan_backfill_scoped_with(&p, &change, &IdentityMapper);
        assert_eq!(v1.models, hooked.models);
        assert_eq!(v1.unit, hooked.unit);
        assert_eq!(v1.reasons, hooked.reasons);
    }

    #[test]
    fn a_mapper_widens_the_downstream_but_never_the_seed() {
        let p = incremental_chain();
        let plan = plan_backfill_scoped_with(&p, &restate("stg", &["a"], &["d1"]), &PlusNext);
        assert_eq!(plan.unit["stg"], BackfillUnit::Partitions(vec!["d1".into()]), "the seed is what was asked");
        assert_eq!(plan.unit["mart"], BackfillUnit::Partitions(vec!["d1".into(), "d1+1".into()]));
    }

    #[test]
    fn a_mapper_cannot_rescue_a_downstream_that_column_pruning_excluded() {
        // stg(a, b) -> mart reads only stg.b. Restating stg with only `a` changed
        // must still prune mart, whatever the mapper would have done with its keys.
        let p = Project::from_models([
            incremental("stg", &[], vec![col("a", &[]), col("b", &[])]),
            incremental("mart", &["stg"], vec![col("b", &[("stg", "b")])]),
        ]);
        let plan = plan_backfill_scoped_with(&p, &restate("stg", &["a"], &["d1"]), &PlusNext);
        assert!(!plan.models.contains(&"mart".to_string()));
    }

    #[test]
    fn a_table_downstream_is_full_whatever_the_mapper_says() {
        let p = Project::from_models([
            incremental("stg", &[], vec![col("a", &[])]),
            model("mart", &["stg"], vec![col("a", &[("stg", "a")])]),
        ]);
        let plan = plan_backfill_scoped_with(&p, &restate("stg", &["a"], &["d1"]), &PlusNext);
        assert_eq!(plan.unit["mart"], BackfillUnit::FullModel);
    }

    #[test]
    fn cost_estimate_uses_materialization_weights() {
        // Three flavors of model — confirms each takes the right weight.
        let p = Project::from_models([
            model("t", &[], vec![col("a", &[])]), // Table
            Model {
                materialization: Materialization::View,
                ..model("v", &[], vec![col("a", &[])])
            },
            incremental("i", &[], vec![col("a", &[])]),
        ]);
        let plan = plan_backfill(
            &p,
            &BTreeMap::from([
                ("t".into(), BTreeSet::from(["a".into()])),
                ("v".into(), BTreeSet::from(["a".into()])),
                ("i".into(), BTreeSet::from(["a".into()])),
            ]),
        );
        let cm = CostModel::default();
        let est = estimate_compute_units(&p, &plan, &cm);
        let by_name: BTreeMap<_, _> = est
            .per_model
            .iter()
            .map(|m| (m.model.as_str(), m))
            .collect();
        assert_eq!(by_name["t"].basis, CostBasis::TableFull);
        assert_eq!(by_name["t"].units, cm.table_full);
        assert_eq!(by_name["v"].basis, CostBasis::ViewFull);
        assert_eq!(by_name["v"].units, cm.view_full);
        // plan_backfill (whole-model) escalates incrementals to Full.
        assert_eq!(by_name["i"].basis, CostBasis::IncrementalFull);
        assert_eq!(by_name["i"].units, cm.incremental_full);
        let expected = cm
            .table_full
            .saturating_add(cm.view_full)
            .saturating_add(cm.incremental_full);
        assert_eq!(est.total, expected);
    }

    #[test]
    fn partition_scoped_incremental_is_cheaper_than_full_refresh() {
        // The headline efficiency claim — restating two partitions should bill
        // less than re-running the whole incremental model.
        let p = incremental_chain();
        let scoped =
            plan_backfill_scoped(&p, &restate("stg", &["a"], &["2026-06-01", "2026-06-02"]));
        let full = plan_backfill(
            &p,
            &BTreeMap::from([("stg".into(), BTreeSet::from(["a".into()]))]),
        );
        let cm = CostModel::default();
        let scoped_est = estimate_compute_units(&p, &scoped, &cm);
        let full_est = estimate_compute_units(&p, &full, &cm);
        assert!(
            scoped_est.total < full_est.total,
            "partition scoping must bill less than full refresh: scoped={:?} full={:?}",
            scoped_est.total,
            full_est.total,
        );
        // And the per-partition basis is reported, not just the total.
        let stg = scoped_est
            .per_model
            .iter()
            .find(|m| m.model == "stg")
            .unwrap();
        assert_eq!(stg.basis, CostBasis::IncrementalPartitions { count: 2 });
    }

    #[test]
    fn empty_plan_costs_nothing() {
        // No edits since last commit -> 0 CU. Important: a deployment with a tight
        // cap shouldn't burn budget just by re-running the planner.
        let p = sample();
        let est = estimate_compute_units(&p, &BackfillPlan::default(), &CostModel::default());
        assert_eq!(est.total, ComputeUnits::zero());
        assert!(est.per_model.is_empty());
    }

    #[test]
    fn an_uncertain_column_changes_even_when_a_sibling_was_hit() {
        // mid.u may depend on anything mid reads; mid.x is hit precisely. The change
        // must carry on through u too — `leaf` reads only u, and used to be skipped
        // because the precise hit on x decided mid's changed columns alone.
        let p = Project::from_models([
            model("up", &[], vec![col("a", &[])]),
            model(
                "mid",
                &["up"],
                vec![col("x", &[("up", "a")]), col("u", &[])],
            ),
            model("leaf", &["mid"], vec![col("y", &[("mid", "u")])]),
        ]);
        let plan = plan_backfill(
            &p,
            &BTreeMap::from([("up".to_string(), BTreeSet::from(["a".to_string()]))]),
        );
        assert_eq!(plan.models, vec!["up", "mid", "leaf"]);
    }

    #[test]
    fn the_unpruned_baseline_is_the_whole_downstream_closure_rebuilt_whole() {
        // A value-only change to stg.b: mart_b rebuilds, mart_a is pruned.
        let p = sample();
        let plan = plan_backfill(
            &p,
            &BTreeMap::from([("stg".to_string(), BTreeSet::from(["b".to_string()]))]),
        );
        assert_eq!(plan.models, vec!["stg", "mart_b"]);
        let cost = CostModel::default();
        let unpruned = estimate_unpruned(&p, &plan, &cost);
        let mut names: Vec<&str> = unpruned.per_model.iter().map(|m| m.model.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["mart_a", "mart_b", "stg"], "`raw` is upstream: never in the baseline");
        assert_eq!(avoided_compute_units(&p, &plan, &cost), cost.table_full, "mart_a's rebuild");
    }

    #[test]
    fn avoided_work_never_goes_negative() {
        // 20 partitions cost more than one full refresh under the default weights.
        let p = Project::from_models([Model {
            materialization: Materialization::Incremental,
            ..model("events", &[], vec![col("dt", &[])])
        }]);
        let keys: BTreeSet<String> = (1..=20).map(|d| format!("2026-06-{d:02}")).collect();
        let plan = plan_backfill_scoped(
            &p,
            &BTreeMap::from([(
                "events".to_string(),
                ChangeSpec {
                    columns: BTreeSet::from(["dt".to_string()]),
                    partitions: PartitionChange::Partitions(keys),
                },
            )]),
        );
        assert_eq!(avoided_compute_units(&p, &plan, &CostModel::default()), ComputeUnits::zero());
    }

    #[test]
    fn compute_mode_default_is_byo() {
        // OSS default must be BYO: a local user with FileStateBackend is not
        // running on our infra and must not be billed by accident.
        assert_eq!(ComputeMode::default(), ComputeMode::Byo);
    }
}
