//! freshet-embed — the one pure-Rust funnel every binding marshals through.
//!
//! C-ABI ([`freshet-ffi`]), WASM ([`freshet-wasm`]), and Python ([`freshet-py`]) all
//! call into a SINGLE entry point here: JSON request in, stable JSON
//! [`PlanResponse`] out. This guarantees every surface answers identically (a
//! cross-binding golden test), and confines all `unsafe`/FFI to the thin binding
//! shims. In-process callers (CLI, daemon) use the typed [`plan`] instead and skip
//! the JSON hop.
//!
//! ## Why the wire DTOs live here, not upstream
//!
//! The planner core's `freshet-core` types ([`BackfillPlan`], [`BackfillReason`],
//! [`BackfillUnit`], …) deliberately derive only `Clone`/`Debug`/`Eq` — **not**
//! `serde`. That keeps the brain dependency-light, but it means you cannot
//! `serde_json::to_string` a `BackfillPlan` directly. So freshet-embed **owns the
//! frozen wire contract**: it mirrors those enums as [`WireReason`]/[`WireUnit`] and
//! converts via [`to_wire`] using an **exhaustive, no-wildcard** match — so the day
//! freshet-core adds a `BackfillReason` variant, this crate fails to *compile* until the
//! wire contract is updated, rather than silently dropping it.

use std::collections::{BTreeMap, BTreeSet};

use freshet_api::{
    estimate_compute_units, estimate_unpruned, BackfillPlan, BackfillReason, BackfillUnit, ChangeSpec, CostBasis, CostEstimate,
    CostModel, Materialization, ModelCost, PartitionChange,
};
use freshet_core::{Model, Project};
use freshet_incremental::{Grain, IncrementalConfig};
use freshet_sql::{build_project, Catalog};
use freshet_state::{StateBackend, StateSnapshot};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Wire contract (the FROZEN request/response shape)
// ---------------------------------------------------------------------------

/// The wire contract revision this crate speaks, for consumers that mirror the DTOs
/// instead of linking them (dagron-state keeps the same string as its
/// `WIRE_CONTRACT_VERSION`). Every revision is additive:
///
/// * **1** — the plan: models, reasons, units, estimate.
/// * **2** — `depends_on`, the plan-restricted edge set.
/// * **3** — `replace`, what each rebuild does to the relation.
/// * **4** — `sql`, the statements that perform it, when a dialect was asked for.
/// * **5** — `cost.unpruned_millicu`, the baseline the rebuild work avoided is
///   measured against.
///
/// Bump it with any field or variant change, in the same commit as the consumers'
/// golden fixtures.
pub const WIRE_CONTRACT_VERSION: &str = "planner-embed/5";

/// A request to plan a backfill. `models` + `sources` describe the project; the
/// caller passes prior state IN (stateless bindings have nowhere to persist it).
#[derive(Clone, Debug, Default, Deserialize)]
pub struct PlanRequest {
    #[serde(default)]
    pub models: Vec<WireModel>,
    /// Source-table schemas for wildcard expansion / unqualified-ref resolution.
    #[serde(default)]
    pub sources: BTreeMap<String, Vec<String>>,
    /// The last committed snapshot (fingerprints + watermarks). Omit for a cold plan.
    #[serde(default)]
    pub prior_state: Option<StateSnapshot>,
    /// If present, plan a *data restatement* of these partitions instead of a
    /// code-change plan (does not read or advance fingerprint state).
    #[serde(default)]
    pub restate: Option<Restate>,
    /// Attach the effort map ([`WireCost`]) to the response.
    ///
    /// Opt-in, and default-`false` on purpose. `dagron-state` mirrors this contract
    /// rather than importing it (its `crates/` tree is published to a repo where the
    /// planner's source does not exist), so its half is guarded by a golden fixture
    /// and a version constant instead of by the compiler. A caller that does not ask
    /// for cost gets a response byte-identical to the one it got before this field
    /// existed, which is what keeps that duplication honest.
    #[serde(default)]
    pub include_cost: bool,
    /// Attach the executable queue ([`WireQueue`]) to the response.
    ///
    /// Separate from `include_cost` because it is the expensive half: a plan scoped
    /// to N partitions expands to N items per model, and a caller that only wants a
    /// total should not pay for the expansion.
    #[serde(default)]
    pub include_queue: bool,
    /// Per-model incremental declarations: `replace` resolves each model's write,
    /// and `grain` / `edges` drive partition propagation through
    /// `freshet_incremental::plan_incremental`. Omit it and every model resolves to no
    /// replace op and identity propagation, which is the pre-v3 behaviour exactly.
    ///
    /// This lives on the request rather than being CLI-only so every binding —
    /// FFI, WASM, Python — can produce a v3 plan. A contract only one producer
    /// can satisfy is not a contract.
    #[serde(default)]
    pub incremental: IncrementalConfig,
    /// Cap this plan at N **milliCU**, the self-hosted processing budget.
    ///
    /// This is the wire half of `freshet_api::ProcessingBudget`, which existed as a
    /// library type with no way to reach it: before this field, the processing cap
    /// described in the planner core's architecture notes was
    /// documented but unreachable through any shipped binary or binding — the cap
    /// could only be set by a Rust caller constructing a `BackfillPlanner` itself.
    /// Putting it on the request rather than in the CLI means every producer can
    /// set it, for the same reason `incremental` lives here.
    ///
    /// `None` is `ProcessingBudget::Unlimited` and is the default, so an existing
    /// caller's response is byte-identical to the one it got before this field
    /// existed. Milli-units, not whole CU, because the wire carries exact integers
    /// (see [`WireCost`]) — a fractional cap would be the one place a rounding
    /// decision could silently change whether a plan is admitted.
    #[serde(default)]
    pub budget_millicu: Option<u64>,
    /// Render each planned model's write as SQL in this dialect — contract v4.
    ///
    /// Opt-in like `include_cost`, for the same reason: a caller that does not ask
    /// gets the bytes it got before the field existed, which is what keeps the
    /// mirrored consumers (`dagron-state`, the Airflow package) honest. Asking is
    /// all-or-nothing: if any model cannot be rendered the response is an
    /// `error` with code `render_error` naming every such model, never SQL for some
    /// of them — running part of a plan rebuilds downstreams over stale upstreams.
    #[serde(default)]
    pub render: Option<RenderRequest>,
}

/// What `PlanRequest::render` asks for.
#[derive(Clone, Debug, Deserialize)]
pub struct RenderRequest {
    /// `postgres` | `snowflake` | `bigquery` | `databricks` | `redshift`.
    pub dialect: String,
    /// Qualify target relations with this schema (`analytics`, `proj.analytics`).
    #[serde(default)]
    pub schema: Option<String>,
    /// Models to recreate from their SQL even though their definition did not
    /// change — the escape hatch for a relation someone altered by hand. The models
    /// whose definition *did* change are recreated without being named here.
    #[serde(default)]
    pub recreate: Vec<String>,
}

/// One model definition on the wire.
#[derive(Clone, Debug, Deserialize)]
pub struct WireModel {
    pub name: String,
    pub sql: String,
    /// `table` | `view` | `incremental`. Defaults to `table` (freshet-core's default).
    /// This is the minimal TODO-5 "materialization hint" — without it, a
    /// `build_project`-derived project is all `Table` and nothing is
    /// partition-scopable.
    #[serde(default)]
    pub materialization: Option<String>,
}

/// A data restatement request.
#[derive(Clone, Debug, Deserialize)]
pub struct Restate {
    pub model: String,
    pub partitions: Vec<String>,
}

/// The stable response: the minimal, topologically-ordered plan.
#[derive(Clone, Debug, Serialize)]
pub struct PlanResponse {
    pub models: Vec<WirePlanModel>,
    /// The snapshot to persist AFTER a successful run (code-change plans only;
    /// `None` for restatements). Stateless callers store this themselves.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_state: Option<StateSnapshot>,
    /// Present iff planning failed; `models` is then empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<WireError>,
    /// The effort map, when `PlanRequest::include_cost` asked for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<WireCost>,
    /// The executable queue, when `PlanRequest::include_queue` asked for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<WireQueue>,
    /// Budget headroom, present iff `PlanRequest::budget_millicu` set a cap.
    ///
    /// Not folded into `cost`: a capped deployment must be able to enforce its cap
    /// without also asking for the effort map, and `cost` is opt-in. They are
    /// computed from the same `estimate_compute_units` call when both are asked
    /// for, so they can never disagree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headroom: Option<WireHeadroom>,
}

/// One model in the plan: why it rebuilds, at what granularity, and what it must
/// wait for.
#[derive(Clone, Debug, Serialize)]
pub struct WirePlanModel {
    pub name: String,
    pub reason: WireReason,
    pub unit: WireUnit,
    /// Upstream models **this plan also rebuilds** that must finish first.
    ///
    /// This is the plan's dependency edge set, not its attribution. `reason`
    /// answers *why* a model is here and names a single cause; that is a
    /// human-facing explanation, and using it as an execution edge is wrong:
    /// a model downstream of both `a` and `b` is blamed on only one of them, so
    /// an orchestrator built from `because_of` alone can start it before the
    /// other finishes.
    ///
    /// Restricted to models in [`PlanResponse::models`] on purpose. A dependency
    /// the plan does not rebuild needs no waiting, so this list is directly
    /// usable as a task's `depends_on` with no filtering and no access to the
    /// wider project. Sorted and deduplicated, and — because the plan is
    /// topologically ordered — every entry appears earlier in `models` than the
    /// model that names it.
    ///
    /// **Always serialized, including when empty.** An absent field means the
    /// producer predates this field (contract v1) and the consumer must fall
    /// back; an empty array means "this model waits for nothing", which is a
    /// different and authoritative answer.
    pub depends_on: Vec<String>,
    /// What this rebuild does to the target relation — contract v3.
    ///
    /// Present only for a model whose project declares a replace strategy. See
    /// [`freshet_incremental::replace`] for the vocabulary and for why an
    /// undeclared model gets no default.
    ///
    /// **Unlike [`Self::depends_on`], absence here is not a version signal**, and
    /// deliberately so. A v2 producer omits the field because it cannot produce
    /// one; a v3 producer omits it because this model declared nothing. Those
    /// read differently but they mean the same thing to a consumer — *no
    /// replace-specific behaviour applies* — so there is nothing for a version
    /// check to change. The v2 case needed the distinction because "no edges"
    /// and "I don't know my edges" demand opposite handling; this one does not,
    /// and inventing a `null`-versus-absent dance to express a difference that
    /// changes no behaviour would be ceremony, not safety.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replace: Option<WireReplace>,
    /// The SQL that performs this model's rebuild — contract v4.
    ///
    /// Present iff the request asked for a dialect (`PlanRequest::render`), and then
    /// present on **every** model: a partially rendered plan is refused upstream
    /// rather than shipped. Like `replace`, absence is not a version signal — it
    /// means the caller did not ask.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<WireSql>,
}

/// Wire mirror of [`freshet_render::RenderedSql`].
#[derive(Clone, Debug, Serialize)]
pub struct WireSql {
    /// The dialect it was rendered for, echoed so the SQL explains itself.
    pub dialect: String,
    /// Run **in order, on one session**; none carries a trailing `;`.
    pub statements: Vec<String>,
    /// Whether the statements are all-or-nothing when run that way. `false` means a
    /// failure part-way can leave the relation half-written — show it to a human.
    pub atomic: bool,
}

/// Wire mirror of [`freshet_incremental::ReplaceOp`].
///
/// Mirrored rather than re-exported, like every other DTO here: the contract is
/// frozen and the upstream type is not, so a refactor across the seam must be a
/// compile error in [`wire_replace`] rather than a silent reshaping of the wire.
#[derive(Clone, Debug, Serialize)]
pub struct WireReplace {
    pub strategy: WireReplaceStrategy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_column: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unique_key: Vec<String>,
    pub target: WireReplaceTarget,
    /// Present when the declared strategy could not be honoured and this op was
    /// widened to a full refresh. A consumer that shows a human anything should
    /// show this: it is the difference between rewriting two partitions and
    /// rewriting the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub widened: Option<WireWidening>,
}

/// Wire mirror of [`freshet_incremental::ReplaceStrategy`].
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WireReplaceStrategy {
    InsertOverwrite,
    DeleteInsert,
    Merge,
    ReplaceWhere,
    FullRefresh,
}

/// Wire mirror of [`freshet_incremental::ReplaceTarget`].
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WireReplaceTarget {
    Whole,
    Partitions(Vec<String>),
}

/// Wire mirror of [`freshet_incremental::Widening`].
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case", tag = "widened_because")]
pub enum WireWidening {
    NotIncremental { materialization: String },
    WholeModelInPlan,
    NoPartitionColumn { declared: WireReplaceStrategy },
    NoUniqueKey,
}

/// Wire mirror of [`freshet_core::BackfillReason`].
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WireReason {
    DirectlyChanged,
    Downstream { because_of: String, via_columns: Vec<String> },
}

/// Wire mirror of [`freshet_core::BackfillUnit`].
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WireUnit {
    FullModel,
    Partitions(Vec<String>),
}

/// A structured planning error.
#[derive(Clone, Debug, Serialize)]
pub struct WireError {
    pub code: String,
    pub message: String,
}

// --- the exhaustive, no-wildcard conversions (adding an upstream variant breaks
//     compilation here on purpose) ---

fn wire_reason(r: &BackfillReason) -> WireReason {
    match r {
        BackfillReason::DirectlyChanged => WireReason::DirectlyChanged,
        BackfillReason::Downstream { because_of, via_columns } => WireReason::Downstream {
            because_of: because_of.clone(),
            via_columns: via_columns.clone(),
        },
    }
}

fn wire_unit(u: &BackfillUnit) -> WireUnit {
    match u {
        BackfillUnit::FullModel => WireUnit::FullModel,
        BackfillUnit::Partitions(ps) => WireUnit::Partitions(ps.clone()),
    }
}

/// Convert a [`BackfillPlan`] into the wire response (without `next_state`/`error`).
///
/// Takes the `project` as well as the plan because the plan alone does not carry
/// the dependency graph — only a single-cause attribution per model. Deriving
/// execution edges from that attribution is the defect this signature exists to
/// close; see [`WirePlanModel::depends_on`].
///
/// `cfg` supplies the per-model replace declarations. Pass
/// [`IncrementalConfig::default()`] when the caller has none: resolution then
/// yields nothing for every model, which is the correct answer — an undeclared
/// project gets no replace ops rather than guessed ones.
pub fn to_wire(plan: &BackfillPlan, project: &Project, cfg: &IncrementalConfig) -> PlanResponse {
    // The plan's own membership. Edges are filtered through this so the emitted
    // list is complete on its own terms: everything a consumer must wait for, and
    // nothing it would have to look up elsewhere.
    let in_plan: BTreeSet<&str> = plan.models.iter().map(String::as_str).collect();
    let mut replace_ops = freshet_incremental::replace::resolve_plan(project, cfg, plan);

    let models = plan
        .models
        .iter()
        .map(|name| {
            let reason = plan
                .reasons
                .get(name)
                .map(wire_reason)
                // A selected model always has a reason; if not, label conservatively.
                .unwrap_or(WireReason::DirectlyChanged);
            let unit = plan.unit.get(name).map(wire_unit).unwrap_or(WireUnit::FullModel);
            let depends_on = plan_edges(project, name, &in_plan);
            let replace = replace_ops.remove(name).map(wire_replace);
            WirePlanModel { name: name.clone(), reason, unit, depends_on, replace, sql: None }
        })
        .collect();
    PlanResponse { models, next_state: None, error: None, cost: None, queue: None, headroom: None }
}

// ---------------------------------------------------------------------------
// The effort map, and the queue it implies
// ---------------------------------------------------------------------------

/// Wire mirror of [`freshet_core::CostEstimate`] — the effort map.
///
/// The unit is the planner's own: **milliCU**, where `1_000` mCU is one whole
/// compute unit and one whole CU is defined upstream as "one full rebuild of one
/// `Table` model". This is a prediction read off a rate card ([`CostModel`]), not a
/// measurement: there is no benchmark behind it and no figure here is denominated
/// in seconds. A surface that renders it says CU.
///
/// Only the exact integer crosses the wire. The whole-CU figure a UI wants to print
/// is `total_millicu / 1_000`, derived at the edge rather than carried here, so the
/// contract has one number per fact.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WireCost {
    /// Exact total in milliCU. Integer, so sums and comparisons stay exact.
    pub total_millicu: u64,
    pub per_model: Vec<WireModelCost>,
    /// What the same change would cost without pruning: every model the directly
    /// changed ones reach downstream, them included, rebuilt whole (dbt's
    /// `--full-refresh -s changed+`). Contract v5. `unpruned_millicu - total_millicu`,
    /// floored at zero, is the rebuild work avoided — the product's north-star
    /// metric; see [`freshet_api::avoided_compute_units`]. Absent from a v4 producer.
    #[serde(default)]
    pub unpruned_millicu: u64,
}

/// How much of a self-hosted processing budget this plan would spend.
///
/// The point of carrying this rather than only a pass/fail is that a cap discovered
/// by hitting it is not a budget, it is an outage. An operator on a capped build can
/// watch `remaining_millicu` fall across a week of ordinary plans and act before a
/// Monday-morning schema change is the thing that finds the ceiling.
///
/// `remaining_millicu` is the one derived number this contract carries on purpose —
/// elsewhere the rule is one number per fact, and a UI derives whole CU from milli.
/// The difference is that "remaining" has a semantic choice inside it: it **saturates
/// at zero** rather than going negative. Deciding that once here beats each of five
/// bindings picking a convention, and a negative headroom is not a quantity anyone
/// acts on — `exceeded` is the fact they act on.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WireHeadroom {
    /// The cap that was asked for, echoed so the response explains itself without
    /// the reader having to hold the request alongside it.
    pub cap_millicu: u64,
    /// What this plan is estimated to cost — the same number `cost.total_millicu`
    /// carries when `include_cost` was also set.
    pub used_millicu: u64,
    /// `cap - used`, saturating at zero. Zero means *no headroom left*, which is
    /// true both exactly at the cap and past it; `exceeded` separates those.
    pub remaining_millicu: u64,
    /// Did this plan breach the cap?
    ///
    /// Authoritative rather than derived: it is the gate's own answer, and
    /// `freshet_api::gate_against_budget` admits a plan exactly *at* the cap
    /// (`used <= cap`). An edge recomputing `used > cap` would be a second place
    /// for that boundary to be decided, and the two could drift.
    pub exceeded: bool,
}

impl WireHeadroom {
    /// Build a headroom view from a cap and an estimate, keeping the admit boundary
    /// in one place.
    fn new(cap_millicu: u64, used_millicu: u64) -> Self {
        Self {
            cap_millicu,
            used_millicu,
            remaining_millicu: cap_millicu.saturating_sub(used_millicu),
            // `<=`, matching `freshet_api::gate_against_budget`: a plan that lands
            // exactly on the cap is admitted, not rejected.
            exceeded: used_millicu > cap_millicu,
        }
    }
}

/// What one model contributes to the total, and on what grounds.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireModelCost {
    pub model: String,
    pub millicu: u64,
    pub basis: WireCostBasis,
}

/// Wire mirror of [`freshet_core::CostBasis`] — the dimension the rate card charged.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireCostBasis {
    TableFull,
    ViewFull,
    IncrementalFull,
    IncrementalPartitions { count: usize },
    /// The plan named a model the project no longer knows about. Billed zero
    /// upstream rather than crashing the estimator; mirrored rather than dropped so
    /// a UI can show the row instead of silently losing it.
    UnknownModel,
}

/// One unit of executable work — the granularity a runner actually picks up.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireWorkItem {
    /// Position in the emission order. Dependency-safe: every item this one waits on
    /// has a strictly lower `seq`.
    pub seq: usize,
    pub model: String,
    /// `None` for a whole-model rebuild; `Some(p)` for one partition of one model.
    /// This is where a plan is *broken down*: a partition-scoped `Incremental` model
    /// with N affected partitions emits N items.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
    /// Models that must finish before this item may start. Model-level, not
    /// item-level — see [`to_queue`] for why.
    pub depends_on: Vec<String>,
    /// This item's share of its model's cost, in milliCU. The shares of one model sum
    /// to exactly that model's total, so a per-partition column and a per-model total
    /// rendered side by side cannot disagree.
    pub millicu: u64,
    /// The concurrency wave this item belongs to: the model's longest-path depth
    /// within the plan. Every item in a wave may run at the same time.
    pub wave: usize,
}

/// The plan as an executable queue: what to run, in what order, at what width.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WireQueue {
    pub items: Vec<WireWorkItem>,
    /// `waves[i]` holds the `seq` of every item runnable in wave `i`. The number of
    /// waves is the plan's critical-path depth; the widest wave is the most
    /// parallelism the plan can use.
    pub waves: Vec<Vec<usize>>,
}

fn wire_cost_basis(b: &CostBasis) -> WireCostBasis {
    // Exhaustive, no wildcard — the same discipline as `wire_reason`/`wire_unit`
    // above. A new upstream `CostBasis` variant must break this build rather than
    // quietly serialize as something else.
    match b {
        CostBasis::TableFull => WireCostBasis::TableFull,
        CostBasis::ViewFull => WireCostBasis::ViewFull,
        CostBasis::IncrementalFull => WireCostBasis::IncrementalFull,
        CostBasis::IncrementalPartitions { count } => {
            WireCostBasis::IncrementalPartitions { count: *count }
        }
        CostBasis::UnknownModel => WireCostBasis::UnknownModel,
    }
}

/// Convert a [`CostEstimate`] into its wire form.
pub fn wire_cost(est: &CostEstimate, unpruned: &CostEstimate) -> WireCost {
    WireCost {
        unpruned_millicu: unpruned.total.0,
        total_millicu: est.total.0,
        per_model: est
            .per_model
            .iter()
            .map(|m: &ModelCost| WireModelCost {
                model: m.model.clone(),
                millicu: m.units.0,
                basis: wire_cost_basis(&m.basis),
            })
            .collect(),
    }
}

/// Project a plan into the queue a runner would execute.
///
/// **Dependencies are model-level, deliberately.** An item for model `M` waits on
/// every in-plan upstream of `M`, not on the matching partition of that upstream.
/// Partition-to-partition edges would be sound only under the identity mapping
/// [`freshet_core::plan_backfill_scoped`] uses today; `freshet-incremental`
/// introduces lookback windows and non-aligned grains, under which partition `p` of
/// `M` reads *several* partitions of its upstream. A model-level barrier stays
/// correct under both, and the price of that conservatism is width inside a wave —
/// never a wrong result. Narrowing it is a change to make when the mapping is known,
/// not one to assume.
pub fn to_queue(project: &Project, plan: &BackfillPlan, cost: &CostEstimate) -> WireQueue {
    let in_plan: BTreeSet<&str> = plan.models.iter().map(String::as_str).collect();
    let by_model: BTreeMap<&str, u64> =
        cost.per_model.iter().map(|m| (m.model.as_str(), m.units.0)).collect();

    // Longest-path depth over in-plan edges. `plan.models` is topologically ordered,
    // so a single forward pass settles every depth.
    let mut depth: BTreeMap<&str, usize> = BTreeMap::new();
    for name in &plan.models {
        let d = upstreams_in_plan(project, name, &in_plan)
            .iter()
            .map(|u| depth.get(u.as_str()).copied().unwrap_or(0) + 1)
            .max()
            .unwrap_or(0);
        depth.insert(name.as_str(), d);
    }

    let mut items: Vec<WireWorkItem> = Vec::new();
    for name in &plan.models {
        let depends_on = upstreams_in_plan(project, name, &in_plan);
        let wave = depth.get(name.as_str()).copied().unwrap_or(0);
        let total = by_model.get(name.as_str()).copied().unwrap_or(0);
        // A whole-model rebuild is one item; a partition-scoped one is one item per
        // partition. An empty partition list means the plan scoped the model to
        // nothing, which is still one no-op item rather than a silently dropped model.
        let partitions: Vec<Option<String>> = match plan.unit.get(name) {
            Some(BackfillUnit::Partitions(ps)) if !ps.is_empty() => {
                ps.iter().map(|p| Some(p.clone())).collect()
            }
            _ => vec![None],
        };
        let shares = split_millicu(total, partitions.len());
        for (partition, millicu) in partitions.into_iter().zip(shares) {
            items.push(WireWorkItem {
                seq: items.len(),
                model: name.clone(),
                partition,
                depends_on: depends_on.clone(),
                millicu,
                wave,
            });
        }
    }

    let mut waves: Vec<Vec<usize>> = Vec::new();
    for item in &items {
        if waves.len() <= item.wave {
            waves.resize(item.wave + 1, Vec::new());
        }
        waves[item.wave].push(item.seq);
    }
    WireQueue { items, waves }
}

/// The models `name` selects from that are themselves in the plan. An upstream that
/// is *not* in the plan needs no barrier: the planner already decided it does not
/// rebuild, so it is available before the plan starts.
fn upstreams_in_plan(project: &Project, name: &str, in_plan: &BTreeSet<&str>) -> Vec<String> {
    project
        .get(name)
        .map(|m| m.depends_on.iter().filter(|u| in_plan.contains(u.as_str())).cloned().collect())
        .unwrap_or_default()
}

/// Split `total` into `n` shares summing to exactly `total`, remainder to the
/// earliest items.
///
/// Exactness is the point. A UI shows per-partition costs beside a per-model total,
/// and a column that does not add up reads as a bug in the planner rather than as
/// rounding.
fn split_millicu(total: u64, n: usize) -> Vec<u64> {
    if n == 0 {
        return Vec::new();
    }
    let n64 = n as u64;
    let (base, rem) = (total / n64, total % n64);
    (0..n64).map(|i| base + u64::from(i < rem)).collect()
}

/// [`to_wire`] plus the effort map and, when asked, the queue it implies.
///
/// Both are opt-in and both are computed from one [`estimate_compute_units`] call,
/// so the totals in `cost` and the shares in `queue` come from the same arithmetic.
pub fn to_wire_with_estimate(
    project: &Project,
    plan: &BackfillPlan,
    cfg: &IncrementalConfig,
    cost_model: &CostModel,
    include_cost: bool,
    include_queue: bool,
) -> PlanResponse {
    to_wire_with_estimate_and_budget(project, plan, cfg, cost_model, include_cost, include_queue, None)
}

/// [`to_wire_with_estimate`] plus the processing-budget gate.
///
/// A `budget_millicu` forces the estimate even when neither `include_cost` nor
/// `include_queue` asked for it. That is not an oversight: you cannot enforce a cap
/// without pricing the plan, so asking for a budget is asking to pay for the
/// estimate. It is the cheap half — `estimate_compute_units` walks the plan, while
/// the queue expansion is what costs per partition.
pub fn to_wire_with_estimate_and_budget(
    project: &Project,
    plan: &BackfillPlan,
    cfg: &IncrementalConfig,
    cost_model: &CostModel,
    include_cost: bool,
    include_queue: bool,
    budget_millicu: Option<u64>,
) -> PlanResponse {
    let mut resp = to_wire(plan, project, cfg);
    if !include_cost && !include_queue && budget_millicu.is_none() {
        return resp;
    }
    let est = estimate_compute_units(project, plan, cost_model);
    if include_queue {
        resp.queue = Some(to_queue(project, plan, &est));
    }
    if include_cost {
        resp.cost = Some(wire_cost(&est, &estimate_unpruned(project, plan, cost_model)));
    }
    if let Some(cap) = budget_millicu {
        resp.headroom = Some(WireHeadroom::new(cap, est.total.0));
    }
    resp
}

/// Attach each model's rendered SQL to a wire response — contract v4.
///
/// Renders through [`freshet_render::render_plan`], so the replace ops behind the
/// SQL are resolved exactly as the response's own `replace` fields were. All or
/// nothing: on failure `resp` is left untouched and the error names every model that
/// could not be rendered.
pub fn attach_sql(
    resp: &mut PlanResponse,
    project: &Project,
    plan: &BackfillPlan,
    cfg: &IncrementalConfig,
    opts: &freshet_render::RenderOptions,
) -> Result<(), freshet_render::RenderErrors> {
    let mut rendered = freshet_render::render_plan(project, plan, cfg, opts)?;
    for m in &mut resp.models {
        m.sql = rendered.remove(&m.name).map(|r| WireSql {
            dialect: r.dialect.as_str().to_string(),
            statements: r.statements,
            atomic: r.atomic,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The move contract (a document of its own, beside the plan contract)
// ---------------------------------------------------------------------------

/// The move contract revision. A document of its own rather than a field on the
/// plan, so a plan consumer is never asked to understand a move. dagron-state keeps
/// the same string as its `MOVE_CONTRACT_VERSION`.
///
/// * **1** — units of unload, load and parity, from `freshet move`.
pub const MOVE_CONTRACT_VERSION: &str = "freshet-move/1";

/// A rendered move, for dagron to run: every unit's unload, load and parity check.
#[derive(Clone, Debug, Serialize)]
pub struct WireMove {
    pub contract: &'static str,
    pub from: String,
    pub to: String,
    /// The stage's root. Every unit's `location` is directly under it.
    pub stage: String,
    pub units: Vec<WireMoveUnit>,
    /// Models the plan names that are not moved, and why (views).
    pub not_moved: Vec<WireNotMoved>,
}

/// One copy. The three steps run in order: `unload`, `load`, then the parity check
/// on `parity`'s two sides. Units are independent of one another.
#[derive(Clone, Debug, Serialize)]
pub struct WireMoveUnit {
    pub model: String,
    /// `None` moves the whole relation. Always serialized: `null` is an answer.
    pub partition: Option<WirePartition>,
    pub location: String,
    pub unload: WireStep,
    pub load: WireStep,
    pub parity: WireParity,
}

#[derive(Clone, Debug, Serialize)]
pub struct WirePartition {
    pub column: String,
    pub key: String,
    /// `string`, `date` or `int`, as in `incremental.json`.
    pub partition_type: &'static str,
}

/// Statements one task runs, in order, on one session.
#[derive(Clone, Debug, Serialize)]
pub struct WireStep {
    /// The `SQL_ENGINE` that runs them: the dialect, or `duckdb` on a Postgres side.
    pub engine: &'static str,
    /// None carries a trailing `;`.
    pub statements: Vec<String>,
    pub atomic: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct WireParity {
    pub source: WireParitySide,
    pub target: WireParitySide,
}

/// What `dagron-step-sql parity` compares on one side: its `PARITY_*_ENGINE`,
/// `…_SCHEMA`, `…_TABLE`, `…_RELATION` and `…_WHERE`.
#[derive(Clone, Debug, Serialize)]
pub struct WireParitySide {
    pub engine: &'static str,
    pub schema: Option<String>,
    pub table: String,
    pub relation: String,
    pub predicate: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct WireNotMoved {
    pub model: String,
    pub reason: String,
}

/// The wire form of a rendered move.
pub fn wire_move(m: &freshet_render::migrate::RenderedMove, opts: &freshet_render::migrate::MoveOptions) -> WireMove {
    use freshet_incremental::PartitionType;
    use freshet_render::migrate::{ParitySide, StepSql};
    let step = |s: &StepSql| WireStep { engine: s.engine, statements: s.statements.clone(), atomic: s.atomic };
    let side = |s: &ParitySide| WireParitySide {
        engine: s.engine,
        schema: s.schema.clone(),
        table: s.table.clone(),
        relation: s.relation.clone(),
        predicate: s.predicate.clone(),
    };
    WireMove {
        contract: MOVE_CONTRACT_VERSION,
        from: opts.from.as_str().to_string(),
        to: opts.to.as_str().to_string(),
        stage: opts.stage.root.clone(),
        units: m
            .units
            .iter()
            .map(|u| WireMoveUnit {
                model: u.model.clone(),
                partition: u.partition.as_ref().map(|p| WirePartition {
                    column: p.column.clone(),
                    key: p.key.clone(),
                    partition_type: match p.partition_type {
                        PartitionType::String => "string",
                        PartitionType::Date => "date",
                        PartitionType::Int => "int",
                    },
                }),
                location: u.location.clone(),
                unload: step(&u.unload),
                load: step(&u.load),
                parity: WireParity { source: side(&u.source), target: side(&u.target) },
            })
            .collect(),
        not_moved: m.not_moved.iter().map(|n| WireNotMoved { model: n.model.clone(), reason: n.reason.clone() }).collect(),
    }
}

/// A code-change plan against committed state, and what committing it writes.
#[derive(Clone, Debug)]
pub struct CodeChangePlan {
    pub plan: BackfillPlan,
    /// Models whose own definition changed since `prior` — new or edited. These, and
    /// only these, are what a renderer recreates rather than writes into
    /// (`freshet_render::RenderOptions::recreate`). Not the same set as the plan's
    /// `DirectlyChanged` models: a restatement a crash interrupted is recovered from
    /// the ledger as a direct change too, and its definition did not move.
    pub redefined: BTreeSet<String>,
    /// The snapshot to commit once the plan has run successfully: the new
    /// fingerprints, and the ledger settled for everything the plan rebuilt.
    pub next_state: StateSnapshot,
}

/// Plan the code changes between `prior` and the project's `current` fingerprints,
/// plus whatever the ledger says a crashed restatement still owes.
///
/// The seeds are every redefined model in full (with all its columns) and every owed
/// partition ([`freshet_incremental::recover_gaps`]); propagation is
/// [`freshet_incremental::plan_incremental`], so declared grains and windows apply.
/// With no ledger and no grains or windows this is exactly freshet-api's
/// `BackfillPlanner::plan`.
///
/// `current` and `redefined` are passed in rather than computed because callers
/// disagree on the scheme: the funnel fingerprints like freshet-state and diffs plainly
/// (`freshet_state::changed_models`); the CLI and daemon fingerprint AST-canonically
/// and forgive a scheme upgrade (`freshet_fingerprint::reconcile`).
pub fn plan_code_change(
    project: &Project,
    prior: &StateSnapshot,
    current: BTreeMap<String, u64>,
    redefined: BTreeSet<String>,
    cfg: &IncrementalConfig,
) -> CodeChangePlan {
    let mut seed: BTreeMap<String, ChangeSpec> = redefined
        .iter()
        .filter_map(|name| {
            let columns = project.get(name)?.output_columns();
            Some((name.clone(), ChangeSpec { columns, partitions: PartitionChange::Full }))
        })
        .collect();
    freshet_incremental::recover_gaps(project, prior, freshet_incremental::DEFAULT_PARTITION_CAP, &mut seed);
    let plan = freshet_incremental::plan_incremental(project, cfg, &seed, Grain::Day);
    let mut next_state = StateSnapshot { fingerprints: current, watermarks: prior.watermarks.clone() };
    freshet_incremental::record_built(&mut next_state, cfg, Grain::Day, &plan);
    CodeChangePlan { plan, redefined, next_state }
}

/// Plan a data restatement of `partitions` of `model`, honouring declared grains and
/// lookback windows. The restated model's data changed, so every column is seeded.
pub fn plan_restatement(
    project: &Project,
    cfg: &IncrementalConfig,
    model: &str,
    partitions: impl IntoIterator<Item = String>,
) -> anyhow::Result<BackfillPlan> {
    let columns = project
        .get(model)
        .map(|m| m.output_columns())
        .ok_or_else(|| anyhow::anyhow!("unknown model for restatement: {model}"))?;
    let seed = BTreeMap::from([(
        model.to_string(),
        ChangeSpec { columns, partitions: PartitionChange::Partitions(partitions.into_iter().collect()) },
    )]);
    Ok(freshet_incremental::plan_incremental(project, cfg, &seed, Grain::Day))
}

/// Mirror a resolved replace op onto the wire.
///
/// Every match here is exhaustive with no wildcard arm, which is the whole point
/// of mirroring rather than re-exporting: a new strategy or widening reason
/// upstream fails to compile here instead of vanishing from the contract.
fn wire_replace(op: freshet_incremental::ReplaceOp) -> WireReplace {
    WireReplace {
        strategy: wire_replace_strategy(op.strategy),
        partition_column: op.partition_column,
        unique_key: op.unique_key,
        target: match op.target {
            freshet_incremental::ReplaceTarget::Whole => WireReplaceTarget::Whole,
            freshet_incremental::ReplaceTarget::Partitions(ps) => WireReplaceTarget::Partitions(ps),
        },
        widened: op.widened.map(|w| match w {
            freshet_incremental::Widening::NotIncremental { materialization } => {
                WireWidening::NotIncremental { materialization }
            }
            freshet_incremental::Widening::WholeModelInPlan => WireWidening::WholeModelInPlan,
            freshet_incremental::Widening::NoPartitionColumn { declared } => {
                WireWidening::NoPartitionColumn { declared: wire_replace_strategy(declared) }
            }
            freshet_incremental::Widening::NoUniqueKey => WireWidening::NoUniqueKey,
        }),
    }
}

fn wire_replace_strategy(s: freshet_incremental::ReplaceStrategy) -> WireReplaceStrategy {
    use freshet_incremental::ReplaceStrategy as S;
    match s {
        S::InsertOverwrite => WireReplaceStrategy::InsertOverwrite,
        S::DeleteInsert => WireReplaceStrategy::DeleteInsert,
        S::Merge => WireReplaceStrategy::Merge,
        S::ReplaceWhere => WireReplaceStrategy::ReplaceWhere,
        S::FullRefresh => WireReplaceStrategy::FullRefresh,
    }
}

/// One model's upstream dependencies, narrowed to those the plan rebuilds.
///
/// A model absent from the project yields no edges rather than an error: the plan
/// is authoritative about what runs, and a missing definition is the project
/// builder's problem, not a reason to emit a half-formed graph.
fn plan_edges(project: &Project, model: &str, in_plan: &BTreeSet<&str>) -> Vec<String> {
    let Some(m) = project.get(model) else {
        return Vec::new();
    };
    let mut edges: Vec<String> = m
        .depends_on
        .iter()
        .filter(|d| in_plan.contains(d.as_str()))
        .cloned()
        .collect();
    // SQL can select the same upstream twice; the wire carries a set.
    edges.sort();
    edges.dedup();
    edges
}

// ---------------------------------------------------------------------------
// The funnel
// ---------------------------------------------------------------------------

/// Typed entrypoint for in-process callers (CLI/daemon).
pub fn plan(req: PlanRequest) -> anyhow::Result<PlanResponse> {
    let pairs: Vec<(String, String)> = req.models.iter().map(|m| (m.name.clone(), m.sql.clone())).collect();
    let hints = materialization_hints(&req.models)?;
    let project = build_project_with_hints(&pairs, &req.sources, &hints)?;

    // A restatement deliberately returns no snapshot: it rebuilds data, so it must not
    // advance the fingerprints that say which *code* has been built.
    let (plan, next_state, redefined) = if let Some(r) = &req.restate {
        (plan_restatement(&project, &req.incremental, &r.model, r.partitions.clone())?, None, BTreeSet::new())
    } else {
        // Fingerprinted exactly as freshet-api's `BackfillPlanner::plan` does, so a
        // binding's state stays comparable with the facade's.
        let prior = req.prior_state.clone().unwrap_or_default();
        let current = freshet_state::fingerprint_project(&project);
        let redefined = freshet_state::changed_models(&prior, &current);
        let c = plan_code_change(&project, &prior, current, redefined, &req.incremental);
        (c.plan, Some(c.next_state), c.redefined)
    };

    let mut resp = to_wire_with_estimate_and_budget(
        &project,
        &plan,
        &req.incremental,
        &CostModel::default(),
        req.include_cost,
        req.include_queue,
        req.budget_millicu,
    );

    // A plan over budget keeps its `models` — the caller has to see *what* pushed it
    // over to scope it down — but must never keep the commit token.
    //
    // This mirrors `freshet_api::BudgetExceededWithPlan`, which strips the snapshot
    // for the same reason: `next_state` is only valid after a rebuild has actually
    // succeeded. Handing one back for a plan that was *refused* would let a caller
    // commit fingerprints saying "all built" over models nothing ever ran — the
    // silent under-rebuild this planner exists to prevent, arriving through the
    // quota gate of all places.
    //
    // Note this is not reported as `error`. The contract's rule is that `error` is
    // present iff `models` is empty, and a rejected plan is a well-formed answer
    // about real work, not a planning failure. `headroom.exceeded` is the signal,
    // and it is on the response whether or not the caller asked for `cost`.
    let over_budget = resp.headroom.as_ref().is_some_and(|h| h.exceeded);
    resp.next_state = if over_budget { None } else { next_state };

    // Contract v4. A refused plan gets no SQL, for the reason it gets no commit
    // token: the models stay visible so the caller can scope the change down, but
    // nothing on the response should be runnable as-is.
    if let (Some(render), false) = (&req.render, over_budget) {
        let dialect: freshet_render::Dialect = match render.dialect.parse() {
            Ok(d) => d,
            Err(e) => return Ok(err_response("render_error", &e.to_string())),
        };
        let mut opts = freshet_render::RenderOptions::new(dialect);
        opts.schema = render.schema.clone();
        // Empty for a restatement: it rewrites data, never definitions.
        opts.recreate = redefined;
        opts.recreate.extend(render.recreate.iter().cloned());
        if let Err(e) = attach_sql(&mut resp, &project, &plan, &req.incremental, &opts) {
            return Ok(err_response("render_error", &e.to_string()));
        }
    }
    Ok(resp)
}

/// JSON-in/JSON-out wrapper — the lingua franca every binding carries. ALWAYS returns
/// a JSON [`PlanResponse`] (errors are an `error` field, never a thrown panic).
pub fn plan_from_json(input: &str) -> String {
    let resp = match serde_json::from_str::<PlanRequest>(input) {
        Ok(req) => match plan(req) {
            Ok(r) => r,
            // `{:#}` walks anyhow's context chain; `to_string()` prints only the
            // outermost frame. That difference is the whole error message for a user:
            // a bad statement reported "parsing model `m`" and stopped, when the
            // parser underneath had said exactly what was wrong and where.
            Err(e) => err_response("plan_error", &format!("{e:#}")),
        },
        Err(e) => err_response("bad_request", &e.to_string()),
    };
    serde_json::to_string(&resp).unwrap_or_else(|_| {
        r#"{"models":[],"error":{"code":"serialize_error","message":"response serialization failed"}}"#.to_string()
    })
}

/// Build a project from SQL, then apply materialization hints by cloning + overriding
/// each model (freshet-core's `Model` fields are public and `Clone`). This is the
/// minimal mechanism that makes partition scoping reachable for `build_project`-derived
/// projects — without it everything defaults to `Table` and the incremental machinery
/// never engages (the planner core's TODO 5).
pub fn build_project_with_hints(
    models: &[(String, String)],
    sources: &Catalog,
    hints: &BTreeMap<String, Materialization>,
) -> anyhow::Result<Project> {
    let base = build_project(models, sources)?;
    if hints.is_empty() {
        return Ok(base);
    }
    let rebuilt: Vec<Model> = base
        .names()
        .map(|n| {
            let mut m = base.get(n).expect("names() yields known models").clone();
            if let Some(&mat) = hints.get(n) {
                m.materialization = mat;
            }
            m
        })
        .collect();
    Ok(Project::from_models(rebuilt))
}

fn materialization_hints(models: &[WireModel]) -> anyhow::Result<BTreeMap<String, Materialization>> {
    let mut out = BTreeMap::new();
    for m in models {
        if let Some(s) = &m.materialization {
            out.insert(m.name.clone(), parse_materialization(s)?);
        }
    }
    Ok(out)
}

fn parse_materialization(s: &str) -> anyhow::Result<Materialization> {
    match s.to_ascii_lowercase().as_str() {
        "table" => Ok(Materialization::Table),
        "view" => Ok(Materialization::View),
        "incremental" => Ok(Materialization::Incremental),
        other => anyhow::bail!("unknown materialization `{other}` (expected table|view|incremental)"),
    }
}

fn err_response(code: &str, message: &str) -> PlanResponse {
    PlanResponse {
        models: vec![],
        next_state: None,
        error: Some(WireError { code: code.to_string(), message: message.to_string() }),
        cost: None,
        queue: None,
        // No headroom on a planning failure: there is no plan to have priced, so a
        // cap has nothing to say about it. `error` and `headroom` are never both set.
        headroom: None,
    }
}

// ---------------------------------------------------------------------------
// A tiny StateBackend that just holds an injected snapshot.
// ---------------------------------------------------------------------------

/// In-memory backend seeded with a caller-supplied snapshot. Used by stateless
/// bindings (WASM/FFI) that pass prior state IN and read `next_state` back OUT —
/// freshet-state's `MemoryStateBackend` has no public "with snapshot" constructor, so we
/// define this 6-line one. Implements BOTH trait methods (the object-safe
/// [`StateBackend`] requires `save` too; here it just overwrites).
pub struct SnapshotBackend {
    snap: std::sync::Mutex<StateSnapshot>,
}

impl SnapshotBackend {
    pub fn new(snap: StateSnapshot) -> Self {
        Self { snap: std::sync::Mutex::new(snap) }
    }
}

impl StateBackend for SnapshotBackend {
    fn load(&self) -> anyhow::Result<StateSnapshot> {
        let guard = self.snap.lock().map_err(|e| anyhow::anyhow!("snapshot mutex poisoned: {e}"))?;
        Ok(guard.clone())
    }
    fn save(&self, snapshot: &StateSnapshot) -> anyhow::Result<()> {
        let mut guard = self.snap.lock().map_err(|e| anyhow::anyhow!("snapshot mutex poisoned: {e}"))?;
        *guard = snapshot.clone();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(json: &str) -> PlanResponse {
        let s = plan_from_json(json);
        serde_json::from_str::<serde_json::Value>(&s).unwrap();
        plan(serde_json::from_str(json).unwrap()).unwrap()
    }

    #[test]
    fn cold_plan_backfills_everything_in_topo_order() {
        let resp = req(r#"{
            "sources": { "raw": ["a", "b"] },
            "models": [
                {"name": "stg",    "sql": "select a, b from raw"},
                {"name": "mart_a", "sql": "select a as x from stg"}
            ]
        }"#);
        let names: Vec<&str> = resp.models.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, vec!["stg", "mart_a"], "topo order, both cold");
        assert!(resp.next_state.is_some(), "code-change plan returns a snapshot to commit");
    }

    #[test]
    fn restatement_with_incremental_hint_scopes_partitions() {
        let resp = req(r#"{
            "sources": { "raw": ["a"] },
            "models": [
                {"name": "stg",  "sql": "select a from raw",  "materialization": "incremental"},
                {"name": "mart", "sql": "select a from stg",  "materialization": "incremental"}
            ],
            "restate": {"model": "stg", "partitions": ["2026-06-20"]}
        }"#);
        // The hint made stg+mart Incremental, so the restatement stays partition-scoped.
        let mart = resp.models.iter().find(|m| m.name == "mart").expect("mart in plan");
        match &mart.unit {
            WireUnit::Partitions(ps) => assert_eq!(ps, &vec!["2026-06-20".to_string()]),
            WireUnit::FullModel => panic!("incremental downstream should stay partition-scoped"),
        }
        assert!(resp.next_state.is_none(), "restatement does not advance fingerprint state");
    }

    #[test]
    fn bad_json_returns_structured_error_not_panic() {
        let s = plan_from_json("{not valid json");
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["error"]["code"], "bad_request");
        assert!(v["models"].as_array().unwrap().is_empty());
    }

    #[test]
    fn a_parse_failure_says_what_was_wrong_and_where() {
        // The context chain, not just its outermost frame. Before this the whole
        // message a user got for a typo was "parsing model `m`", which names the model
        // and nothing else — useless in a browser where there is no CLI to re-run.
        let s = plan_from_json(r#"{"models":[{"name":"m","sql":"selct a from raw"}]}"#);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        let message = v["error"]["message"].as_str().unwrap();
        assert!(message.contains("parsing model `m`"), "{message}");
        assert!(
            message.len() > "parsing model `m`".len(),
            "the parser's own diagnosis must survive to the wire, got: {message}"
        );
    }

    #[test]
    fn unknown_materialization_is_a_clean_error() {
        let s = plan_from_json(r#"{"models":[{"name":"m","sql":"select 1 as x","materialization":"matview"}]}"#);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["error"]["code"], "plan_error");
    }

    // --- the effort map -------------------------------------------------

    const TWO_TABLES: &str = r#"{
        "sources": { "raw": ["a", "b"] },
        "models": [
            {"name": "stg",    "sql": "select a, b from raw"},
            {"name": "mart_a", "sql": "select a as x from stg"}
        ]
    }"#;

    /// The guarantee `dagron-state` is entitled to: it mirrors this contract rather
    /// than importing it, so a caller that never asked for cost must see exactly the
    /// bytes it saw before the field existed.
    #[test]
    fn cost_and_queue_are_absent_unless_asked_for() {
        let json = plan_from_json(TWO_TABLES);
        assert!(!json.contains("cost"), "cost must not appear unasked: {json}");
        assert!(!json.contains("queue"), "queue must not appear unasked: {json}");

        let resp = req(TWO_TABLES);
        assert!(resp.cost.is_none());
        assert!(resp.queue.is_none());
    }

    #[test]
    fn the_total_is_the_sum_of_the_per_model_costs() {
        let json = TWO_TABLES.replace("\"models\"", "\"include_cost\": true, \"models\"");
        let cost = req(&json).cost.expect("asked for cost");
        let summed: u64 = cost.per_model.iter().map(|m| m.millicu).sum();
        assert_eq!(cost.total_millicu, summed);
        // Two Table models on the default rate card: one whole CU each.
        assert_eq!(cost.total_millicu, 2_000);
        assert!(cost.per_model.iter().all(|m| matches!(m.basis, WireCostBasis::TableFull)));
    }

    #[test]
    fn a_view_is_charged_as_a_view_not_as_a_table() {
        let json = r#"{
            "include_cost": true,
            "sources": { "raw": ["a"] },
            "models": [
                {"name": "stg", "sql": "select a from raw", "materialization": "view"}
            ]
        }"#;
        let cost = req(json).cost.expect("asked for cost");
        assert_eq!(cost.total_millicu, 50, "a view is definition-only");
        assert!(matches!(cost.per_model[0].basis, WireCostBasis::ViewFull));
    }

    // --- the queue ------------------------------------------------------

    /// Three partitions of one model is three items — this is the "how is the data
    /// broken down?" answer, and it has to be per-partition to be worth previewing.
    #[test]
    fn a_partition_scoped_model_expands_to_one_item_per_partition() {
        let json = r#"{
            "include_cost": true,
            "include_queue": true,
            "sources": { "raw": ["a"] },
            "models": [
                {"name": "stg",  "sql": "select a from raw", "materialization": "incremental"},
                {"name": "mart", "sql": "select a from stg", "materialization": "incremental"}
            ],
            "restate": {"model": "stg", "partitions": ["2026-06-20", "2026-06-21", "2026-06-22"]}
        }"#;
        let queue = req(json).queue.expect("asked for queue");

        let stg: Vec<_> = queue.items.iter().filter(|i| i.model == "stg").collect();
        assert_eq!(stg.len(), 3, "three partitions, three items");
        let mut days: Vec<&str> = stg.iter().filter_map(|i| i.partition.as_deref()).collect();
        days.sort_unstable();
        assert_eq!(days, ["2026-06-20", "2026-06-21", "2026-06-22"]);

        // The identity mapping carried the same three partitions downstream.
        assert_eq!(queue.items.iter().filter(|i| i.model == "mart").count(), 3);
    }

    /// The invariant that lets a UI print a per-partition column beside a per-model
    /// total: the shares add up exactly, never approximately.
    #[test]
    fn the_items_of_a_model_sum_to_that_models_cost() {
        let json = r#"{
            "include_cost": true,
            "include_queue": true,
            "sources": { "raw": ["a"] },
            "models": [
                {"name": "stg",  "sql": "select a from raw", "materialization": "incremental"},
                {"name": "mart", "sql": "select a from stg", "materialization": "incremental"}
            ],
            "restate": {"model": "stg", "partitions": ["p1", "p2", "p3"]}
        }"#;
        let resp = req(json);
        let (cost, queue) = (resp.cost.unwrap(), resp.queue.unwrap());

        for per_model in &cost.per_model {
            let from_items: u64 = queue
                .items
                .iter()
                .filter(|i| i.model == per_model.model)
                .map(|i| i.millicu)
                .sum();
            assert_eq!(
                from_items, per_model.millicu,
                "queue shares for `{}` must sum to its plan cost",
                per_model.model
            );
        }
        let all_items: u64 = queue.items.iter().map(|i| i.millicu).sum();
        assert_eq!(all_items, cost.total_millicu);
    }

    /// `seq` is a promise: a runner draining the queue in order never starts work
    /// whose inputs have not been rebuilt.
    #[test]
    fn every_item_is_emitted_after_everything_it_waits_on() {
        let json = r#"{
            "include_queue": true,
            "sources": { "raw": ["a", "b"] },
            "models": [
                {"name": "stg",   "sql": "select a, b from raw"},
                {"name": "mid",   "sql": "select a from stg"},
                {"name": "leaf",  "sql": "select a from mid"}
            ]
        }"#;
        let queue = req(json).queue.expect("asked for queue");
        let last_seq_of = |model: &str| {
            queue.items.iter().filter(|i| i.model == model).map(|i| i.seq).max().unwrap()
        };
        for item in &queue.items {
            for up in &item.depends_on {
                assert!(
                    last_seq_of(up) < item.seq,
                    "`{}` (seq {}) must follow all of `{up}`",
                    item.model,
                    item.seq
                );
            }
        }
        // A three-model chain is three waves of one.
        assert_eq!(queue.waves.len(), 3, "critical path is the chain length");
        assert!(queue.waves.iter().all(|w| w.len() == 1));
    }

    /// Independent models share a wave — the plan's usable parallelism, which is the
    /// number a platform team reads the preview for.
    #[test]
    fn independent_models_share_a_wave() {
        let json = r#"{
            "include_queue": true,
            "sources": { "raw": ["a", "b"] },
            "models": [
                {"name": "stg",     "sql": "select a, b from raw"},
                {"name": "mart_a",  "sql": "select a as x from stg"},
                {"name": "mart_b",  "sql": "select b as y from stg"}
            ]
        }"#;
        let queue = req(json).queue.expect("asked for queue");
        assert_eq!(queue.waves.len(), 2, "stg, then both marts");
        assert_eq!(queue.waves[0].len(), 1, "stg alone");
        assert_eq!(queue.waves[1].len(), 2, "the two marts are independent");
    }

    /// An upstream the planner did *not* select needs no barrier: it is already built.
    #[test]
    fn a_dependency_outside_the_plan_is_not_waited_on() {
        let json = r#"{
            "include_queue": true,
            "sources": { "raw": ["a"] },
            "models": [
                {"name": "stg",  "sql": "select a from raw", "materialization": "incremental"},
                {"name": "mart", "sql": "select a from stg", "materialization": "incremental"}
            ],
            "restate": {"model": "mart", "partitions": ["p1"]}
        }"#;
        let queue = req(json).queue.expect("asked for queue");
        assert_eq!(queue.items.len(), 1, "restating a leaf plans only that leaf");
        assert_eq!(queue.items[0].model, "mart");
        assert!(
            queue.items[0].depends_on.is_empty(),
            "stg is not in the plan, so nothing waits on it"
        );
    }

    #[test]
    fn shares_are_exact_when_the_cost_does_not_divide_evenly() {
        assert_eq!(split_millicu(10, 3), vec![4, 3, 3], "remainder goes to the earliest");
        assert_eq!(split_millicu(10, 3).iter().sum::<u64>(), 10);
        assert_eq!(split_millicu(9, 3), vec![3, 3, 3]);
        assert_eq!(split_millicu(0, 4), vec![0, 0, 0, 0]);
        assert!(split_millicu(5, 0).is_empty(), "no items, no shares, no divide by zero");
    }

    fn edges_of<'a>(resp: &'a PlanResponse, model: &str) -> &'a [String] {
        &resp.models.iter().find(|m| m.name == model).expect("model in plan").depends_on
    }

    #[test]
    fn a_fan_in_carries_both_parents_where_the_reason_names_one() {
        // The defect this field exists to close. `mart` selects from BOTH `a` and
        // `b`; its `reason` can only blame one of them, so an orchestrator built
        // from the attribution alone would start `mart` before the other parent
        // finished. `depends_on` carries the real edge set.
        let resp = req(r#"{
            "sources": { "raw": ["x", "y"] },
            "models": [
                {"name": "a",    "sql": "select x from raw"},
                {"name": "b",    "sql": "select y from raw"},
                {"name": "mart", "sql": "select a.x, b.y from a join b on a.x = b.y"}
            ]
        }"#);

        assert_eq!(edges_of(&resp, "mart"), ["a".to_string(), "b".to_string()]);
        assert!(edges_of(&resp, "a").is_empty(), "a roots the graph");
        assert!(edges_of(&resp, "b").is_empty(), "b roots the graph");

        // And the attribution really does name only one of the two, which is why
        // the edge set had to be carried separately.
        let mart = resp.models.iter().find(|m| m.name == "mart").unwrap();
        if let WireReason::Downstream { because_of, .. } = &mart.reason {
            assert!(
                ["a", "b"].contains(&because_of.as_str()),
                "attribution names a single parent, not the set"
            );
        }
    }

    #[test]
    fn edges_never_point_outside_the_plan() {
        // `stg` is unchanged, so only `mart` rebuilds. `mart` depends on `stg`, but
        // nothing is waiting for a model that is not running — emitting that edge
        // would make the list unusable as a task's depends_on without filtering.
        let cold = req(r#"{
            "sources": { "raw": ["a"] },
            "models": [
                {"name": "stg",  "sql": "select a from raw"},
                {"name": "mart", "sql": "select a from stg"}
            ]
        }"#);
        let state = serde_json::to_string(&cold.next_state.unwrap()).unwrap();

        // Replan with stg untouched and mart's SQL changed.
        let resp = req(&format!(
            r#"{{
                "sources": {{ "raw": ["a"] }},
                "models": [
                    {{"name": "stg",  "sql": "select a from raw"}},
                    {{"name": "mart", "sql": "select a, a as a2 from stg"}}
                ],
                "prior_state": {state}
            }}"#
        ));

        let names: Vec<&str> = resp.models.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, vec!["mart"], "only the changed model rebuilds");
        assert!(
            edges_of(&resp, "mart").is_empty(),
            "stg is not in the plan, so mart waits for nothing: {:?}",
            edges_of(&resp, "mart")
        );
    }

    #[test]
    fn every_edge_appears_earlier_in_the_plan_than_its_dependent() {
        // The invariant a consumer relies on to build a DAG in one pass. It holds
        // because the plan is topologically ordered and edges are plan-restricted.
        let resp = req(r#"{
            "sources": { "raw": ["x", "y"] },
            "models": [
                {"name": "a",    "sql": "select x from raw"},
                {"name": "b",    "sql": "select x from a"},
                {"name": "c",    "sql": "select y from raw"},
                {"name": "mart", "sql": "select b.x, c.y from b join c on b.x = c.y"}
            ]
        }"#);

        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for m in &resp.models {
            for dep in &m.depends_on {
                assert!(
                    seen.contains(dep.as_str()),
                    "{} depends on {}, which has not appeared yet",
                    m.name,
                    dep
                );
            }
            seen.insert(m.name.as_str());
        }
        assert_eq!(edges_of(&resp, "mart"), ["b".to_string(), "c".to_string()]);
    }

    #[test]
    fn depends_on_is_serialized_even_when_empty() {
        // Presence is the version signal: an absent field means a v1 producer and
        // the consumer must fall back; `[]` is an authoritative "waits for nothing".
        let s = plan_from_json(r#"{"sources":{"raw":["a"]},"models":[{"name":"m","sql":"select a from raw"}]}"#);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(
            v["models"][0]["depends_on"],
            serde_json::json!([]),
            "empty must be present, not skipped: {s}"
        );
    }

    /// One plan, one edge set.
    ///
    /// `WirePlanModel::depends_on` and the queue's `WireWorkItem::depends_on` describe the
    /// same graph by two different routes — `plan_edges`, which sorts and deduplicates, and
    /// `upstreams_in_plan`, which does neither — so a consumer building a DAG from either
    /// must get the same answer. They agree today only because their shared input already
    /// is a set: `referenced_models` upstream collects into a `BTreeSet`, so a model's
    /// `depends_on` arrives sorted and unique and both routes are no-ops over it.
    ///
    /// That is an invariant held in another module, by an implementation detail, with
    /// nothing on this side asserting it. This test is that assertion. It fails the day
    /// `referenced_models` returns declaration order instead, which would otherwise ship as
    /// one response contradicting itself.
    #[test]
    fn the_models_and_the_queue_report_the_same_edges() {
        // The fixture needs BOTH properties at once, and getting one without the other makes
        // the test quietly useless.
        //
        // Multiple partitions, so `to_queue` emits several items per model and a comparison
        // that folds them together is caught. AND a model with two in-plan upstreams whose
        // declaration order (`zebra`, `alpha`) differs from sorted order, so a route that
        // stopped normalising is caught. Restating a shared root rather than one branch is
        // what keeps `alpha` in the plan; restate `zebra` directly and `mart` is left with a
        // single upstream, where reversing the list is a no-op and the ordering half of this
        // test silently stops asserting anything.
        let resp = req(r#"{
            "include_queue": true,
            "sources": { "raw": ["id", "x"] },
            "models": [
                {"name": "base",  "sql": "select id, x from raw", "materialization": "incremental"},
                {"name": "zebra", "sql": "select id, x from base", "materialization": "incremental"},
                {"name": "alpha", "sql": "select id, x from base", "materialization": "incremental"},
                {"name": "mart",  "sql": "select z.id, a.x from zebra z join alpha a on z.id = a.id", "materialization": "incremental"}
            ],
            "restate": {"model": "base", "partitions": ["p1", "p2", "p3"]}
        }"#);
        // Guard the fixture itself: both properties must actually hold, or the assertions
        // below pass without exercising what they were written for.
        let q = resp.queue.as_ref().expect("asked for queue");
        assert!(
            q.items.iter().filter(|i| i.model == "mart").count() > 1,
            "fixture must emit several items per model, or the folding case is untested"
        );
        assert_eq!(
            resp.models.iter().find(|m| m.name == "mart").expect("mart in plan").depends_on,
            ["alpha".to_string(), "zebra".to_string()],
            "fixture must give `mart` two in-plan upstreams in non-declaration order"
        );
        let queue = resp.queue.as_ref().expect("asked for queue");
        for m in &resp.models {
            // Per item, not concatenated across them. A partition-scoped model emits one
            // item per partition and each carries the model's whole edge list, so folding
            // them together compares `[zebra, zebra, zebra]` against `[zebra]` and fails on
            // a perfectly correct plan. Comparing them as sets would fix that and throw away
            // the ordering, which is the half this test exists for; comparing each item
            // against the model keeps both.
            let items: Vec<_> = queue.items.iter().filter(|i| i.model == m.name).collect();
            assert!(!items.is_empty(), "`{}` is in the plan but not the queue", m.name);
            for item in items {
                assert_eq!(
                    item.depends_on, m.depends_on,
                    "`{}` (seq {}) has one edge set in the queue and another on the models",
                    m.name, item.seq
                );
            }
        }
    }

    /// Two `table` models, cold: 1 CU each by the default `CostModel`, so 2000 mCU.
    /// Every budget test below is anchored on that so the numbers are checkable by
    /// hand rather than by re-running the estimator.
    const TWO_COLD_TABLES: &str = r#"{
        "sources": { "raw": ["a", "b"] },
        "models": [
            {"name": "stg",    "sql": "select a, b from raw"},
            {"name": "mart_a", "sql": "select a as x from stg"}
        ]
    }"#;

    fn with_budget(millicu: u64) -> PlanResponse {
        let mut v: serde_json::Value = serde_json::from_str(TWO_COLD_TABLES).unwrap();
        v["budget_millicu"] = serde_json::json!(millicu);
        req(&serde_json::to_string(&v).unwrap())
    }

    #[test]
    fn no_budget_means_no_headroom_field_at_all() {
        let resp = req(TWO_COLD_TABLES);
        assert!(resp.headroom.is_none(), "an uncapped plan must not grow a headroom key");
        let json = serde_json::to_string(&resp).unwrap();
        assert!(
            !json.contains("headroom"),
            "the response must stay byte-identical to the pre-budget contract: {json}"
        );
    }

    #[test]
    fn a_plan_inside_budget_reports_what_is_left_and_still_commits() {
        let resp = with_budget(5_000);
        let h = resp.headroom.expect("a budget was set");
        assert_eq!(h.cap_millicu, 5_000);
        assert_eq!(h.used_millicu, 2_000, "two cold table models at 1 CU each");
        assert_eq!(h.remaining_millicu, 3_000);
        assert!(!h.exceeded);
        assert!(
            resp.next_state.is_some(),
            "an admitted plan keeps its commit token — the budget changed nothing about it"
        );
    }

    /// The load-bearing one. A refused plan must keep its models (so the caller can
    /// see what to scope down) and must lose `next_state` — committing a snapshot for
    /// a plan that never ran is the silent under-rebuild this planner exists to stop,
    /// and the quota gate must not be the thing that introduces it.
    #[test]
    fn a_plan_over_budget_keeps_its_models_but_never_the_commit_token() {
        let resp = with_budget(1_500);
        let h = resp.headroom.clone().expect("a budget was set");
        assert!(h.exceeded);
        assert_eq!(h.used_millicu, 2_000);
        assert_eq!(h.remaining_millicu, 0, "headroom saturates at zero, never negative");

        assert_eq!(resp.models.len(), 2, "the caller has to see what pushed it over");
        assert!(
            resp.next_state.is_none(),
            "a refused plan must not carry a commit-ready snapshot"
        );
        assert!(
            resp.error.is_none(),
            "over budget is a well-formed answer about real work, not a planning \
             failure — and `error` is present iff `models` is empty"
        );
    }

    #[test]
    fn a_plan_exactly_on_the_cap_is_admitted() {
        // `freshet_api::gate_against_budget` admits `used <= cap`. Pinned here
        // because an off-by-one on the boundary is invisible until the day a plan
        // lands exactly on it.
        let h = with_budget(2_000).headroom.expect("a budget was set");
        assert!(!h.exceeded, "used == cap is inside the budget");
        assert_eq!(h.remaining_millicu, 0, "no headroom left, but not over");
        assert!(
            with_budget(2_000).next_state.is_some(),
            "admitted at the boundary means the commit token survives"
        );
    }

    #[test]
    fn a_zero_budget_admits_nothing_that_costs_anything() {
        let resp = with_budget(0);
        let h = resp.headroom.expect("a budget was set");
        assert!(h.exceeded, "a 0 CU cap refuses a 2 CU plan");
        assert!(resp.next_state.is_none());
    }

    #[test]
    fn headroom_and_cost_agree_when_both_are_asked_for() {
        // They are computed from one `estimate_compute_units` call; this pins that
        // they cannot drift into two answers for the same question.
        let mut v: serde_json::Value = serde_json::from_str(TWO_COLD_TABLES).unwrap();
        v["budget_millicu"] = serde_json::json!(10_000);
        v["include_cost"] = serde_json::json!(true);
        let resp = req(&serde_json::to_string(&v).unwrap());
        let h = resp.headroom.expect("a budget was set");
        let c = resp.cost.expect("cost was asked for");
        assert_eq!(h.used_millicu, c.total_millicu);
    }

    #[test]
    fn a_budget_alone_is_enough_to_gate_without_asking_for_cost() {
        // A capped deployment must be able to enforce its cap without paying for the
        // effort map. If the gate only ran under `include_cost`, the cap would be
        // bypassable by omitting a flag.
        let resp = with_budget(1_000);
        assert!(resp.cost.is_none(), "cost was not asked for");
        assert!(resp.queue.is_none(), "nor the queue");
        assert!(resp.headroom.is_some_and(|h| h.exceeded), "but the cap still bound");
    }

    #[test]
    fn a_restatement_is_budgeted_too() {
        // Restatements are the expensive shape — the one most likely to blow a cap —
        // and they carry no snapshot in the first place, so the gate has to be
        // visible through `headroom` rather than through a stripped `next_state`.
        let resp = req(r#"{
            "sources": { "raw": ["a"] },
            "models": [
                {"name": "stg",  "sql": "select a from raw",  "materialization": "incremental"},
                {"name": "mart", "sql": "select a from stg",  "materialization": "incremental"}
            ],
            "restate": {"model": "stg", "partitions": ["2026-06-20"]},
            "budget_millicu": 1
        }"#);
        assert!(
            resp.headroom.is_some_and(|h| h.exceeded),
            "a 1 mCU cap must refuse a restatement that costs more"
        );
    }

    // --- contract v4: rendered SQL ------------------------------------------

    fn sql_of<'a>(resp: &'a PlanResponse, model: &str) -> &'a WireSql {
        resp.models.iter().find(|m| m.name == model).and_then(|m| m.sql.as_ref()).expect("sql rendered")
    }

    #[test]
    fn sql_is_absent_unless_a_dialect_is_asked_for() {
        let json = plan_from_json(TWO_TABLES);
        assert!(!json.contains("\"sql\""), "no key without the ask, so v3 consumers see v3 bytes: {json}");
    }

    #[test]
    fn a_cold_plan_renders_every_model_in_create_form() {
        let json = TWO_TABLES.replace("\"models\"", r#""render": {"dialect": "postgres"}, "models""#);
        let resp = req(&json);
        for m in &resp.models {
            let sql = m.sql.as_ref().expect("every model, when asked");
            assert_eq!(sql.dialect, "postgres");
            assert!(sql.statements.iter().any(|s| s.starts_with(&format!("CREATE TABLE {}", m.name))), "{:?}", sql);
        }
    }

    #[test]
    fn only_the_redefined_model_is_recreated_and_its_downstream_keeps_its_relation() {
        let cold = req(TWO_TABLES);
        let state = serde_json::to_string(&cold.next_state.unwrap()).unwrap();
        let resp = req(&format!(
            r#"{{
                "render": {{"dialect": "snowflake"}},
                "sources": {{ "raw": ["a", "b"] }},
                "models": [
                    {{"name": "stg",    "sql": "select a, b from raw where a > 0"}},
                    {{"name": "mart_a", "sql": "select a as x from stg"}}
                ],
                "prior_state": {state}
            }}"#
        ));
        assert_eq!(sql_of(&resp, "stg").statements, ["CREATE OR REPLACE TABLE stg COPY GRANTS AS\nselect a, b from raw where a > 0"]);
        assert!(
            sql_of(&resp, "mart_a").statements[0].starts_with("INSERT OVERWRITE INTO mart_a (x)"),
            "mart_a's SQL did not change, so neither did its columns: replace rows, keep the table"
        );
    }

    #[test]
    fn a_restatement_renders_partition_scoped_writes_and_recreates_nothing() {
        let resp = req(r#"{
            "render": {"dialect": "postgres"},
            "sources": { "raw": ["dt", "a"] },
            "models": [
                {"name": "stg",  "sql": "select dt, a from raw", "materialization": "incremental"}
            ],
            "incremental": {"models": {"stg": {"replace": {"strategy": "insert_overwrite", "partition_column": "dt", "partition_type": "date"}}}},
            "restate": {"model": "stg", "partitions": ["2026-06-20"]}
        }"#);
        let sql = sql_of(&resp, "stg");
        assert!(sql.statements[0].contains("DELETE FROM stg WHERE dt IN (DATE '2026-06-20')"), "{:?}", sql.statements);
        assert!(!sql.statements.iter().any(|s| s.contains("DROP")), "a restatement rewrites data, never definitions");
        assert!(sql.atomic);
    }

    #[test]
    fn an_unrenderable_plan_is_an_error_naming_every_model_and_carries_no_models() {
        // Two undeclared incrementals, restated: neither can be written partition-wise.
        let s = plan_from_json(r#"{
            "render": {"dialect": "bigquery"},
            "sources": { "raw": ["a"] },
            "models": [
                {"name": "stg",  "sql": "select a from raw", "materialization": "incremental"},
                {"name": "mart", "sql": "select a from stg", "materialization": "incremental"}
            ],
            "restate": {"model": "stg", "partitions": ["p1"]}
        }"#);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["error"]["code"], "render_error");
        let msg = v["error"]["message"].as_str().unwrap();
        assert!(msg.contains("`stg`") && msg.contains("`mart`"), "{msg}");
        assert!(v["models"].as_array().unwrap().is_empty(), "error iff no models — never SQL for half a plan");
    }

    #[test]
    fn an_unknown_dialect_is_a_render_error_listing_the_known_ones() {
        let s = plan_from_json(&TWO_TABLES.replace("\"models\"", r#""render": {"dialect": "oracle"}, "models""#));
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["error"]["code"], "render_error");
        assert!(v["error"]["message"].as_str().unwrap().contains("postgres, snowflake, bigquery, databricks"));
    }

    #[test]
    fn a_plan_refused_by_its_budget_carries_no_runnable_sql() {
        let mut v: serde_json::Value = serde_json::from_str(TWO_COLD_TABLES).unwrap();
        v["budget_millicu"] = serde_json::json!(1_000);
        v["render"] = serde_json::json!({"dialect": "postgres"});
        let resp = req(&serde_json::to_string(&v).unwrap());
        assert!(resp.headroom.as_ref().is_some_and(|h| h.exceeded));
        assert!(resp.models.iter().all(|m| m.sql.is_none()), "refused means nothing on it is runnable as-is");
    }

    #[test]
    fn an_explicit_recreate_forces_the_create_form_on_an_unchanged_model() {
        let cold = req(TWO_TABLES);
        let state = serde_json::to_string(&cold.next_state.unwrap()).unwrap();
        // Edit stg so mart_a is in the plan as a downstream, then ask to recreate it anyway.
        let resp = req(&format!(
            r#"{{
                "render": {{"dialect": "bigquery", "recreate": ["mart_a"]}},
                "sources": {{ "raw": ["a", "b"] }},
                "models": [
                    {{"name": "stg",    "sql": "select a, b from raw where a > 1"}},
                    {{"name": "mart_a", "sql": "select a as x from stg"}}
                ],
                "prior_state": {state}
            }}"#
        ));
        assert_eq!(sql_of(&resp, "mart_a").statements, ["CREATE OR REPLACE TABLE mart_a AS\nselect a as x from stg"]);
    }

    // --- M1: grains, windows and the ledger through the funnel -----------------

    #[test]
    fn a_restatement_honours_declared_windows() {
        let resp = req(r#"{
            "sources": { "raw": ["dt", "a"] },
            "models": [
                {"name": "stg",     "sql": "select dt, a from raw", "materialization": "incremental"},
                {"name": "rolling", "sql": "select dt, a from stg", "materialization": "incremental"}
            ],
            "incremental": {"models": {"rolling": {"grain": "Day", "edges": {"stg": {"window": 3}}}}},
            "restate": {"model": "stg", "partitions": ["2026-06-20"]}
        }"#);
        let rolling = resp.models.iter().find(|m| m.name == "rolling").unwrap();
        match &rolling.unit {
            WireUnit::Partitions(ps) => assert_eq!(ps, &["2026-06-20", "2026-06-21", "2026-06-22"]),
            WireUnit::FullModel => panic!("a 3-day window over one day is three days, not the table"),
        }
    }

    #[test]
    fn a_code_change_plan_recovers_what_the_ledger_owes_and_settles_it() {
        let cold = req(r#"{"sources": {"raw": ["dt", "a"]}, "models": [
            {"name": "stg", "sql": "select dt, a from raw", "materialization": "incremental"}]}"#);
        let mut state = cold.next_state.unwrap();
        let mut wm = freshet_incremental::Watermark::new(Grain::Day);
        wm.mark_pending(&BTreeSet::from(["2026-06-20".to_string()]));
        freshet_incremental::write_watermark(&mut state, "stg", &wm);

        // Nothing in the project changed; the owed partition alone makes the plan.
        let resp = req(&format!(
            r#"{{
                "render": {{"dialect": "postgres"}},
                "sources": {{"raw": ["dt", "a"]}},
                "models": [{{"name": "stg", "sql": "select dt, a from raw", "materialization": "incremental"}}],
                "incremental": {{"models": {{"stg": {{"replace": {{"strategy": "insert_overwrite", "partition_column": "dt"}}}}}}}},
                "prior_state": {}
            }}"#,
            serde_json::to_string(&state).unwrap()
        ));
        let stg = resp.models.iter().find(|m| m.name == "stg").expect("the owed partition is planned");
        assert!(matches!(&stg.unit, WireUnit::Partitions(ps) if ps == &["2026-06-20"]));
        let sql = stg.sql.as_ref().unwrap();
        assert!(
            !sql.statements.iter().any(|s| s.contains("DROP")),
            "a recovered restatement is written partition-wise — its definition did not change: {:?}",
            sql.statements
        );
        let settled = resp.next_state.unwrap();
        assert_eq!(freshet_incremental::read_watermark(&settled, "stg").unwrap().owed(100), None, "committing settles it");
    }

    #[test]
    fn a_restatement_also_carries_edges() {
        let resp = req(r#"{
            "sources": { "raw": ["a"] },
            "models": [
                {"name": "stg",  "sql": "select a from raw",  "materialization": "incremental"},
                {"name": "mart", "sql": "select a from stg",  "materialization": "incremental"}
            ],
            "restate": {"model": "stg", "partitions": ["2026-06-20"]}
        }"#);
        assert_eq!(edges_of(&resp, "mart"), ["stg".to_string()], "restatements need ordering too");
    }
}
