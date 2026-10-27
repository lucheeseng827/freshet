//! freshet-api — the embeddable facade.
//!
//! This is the surface an orchestrator embeds. It owns the *lifecycle* that ties
//! the graph (`freshet-core`) to persisted state (`freshet-state`):
//!
//! ```text
//!   1. fingerprint the current project
//!   2. load the last snapshot from the StateBackend
//!   3. diff -> changed models
//!   4. column-aware plan -> minimal, topo-ordered backfill
//!   5. (orchestrator runs the plan)
//!   6. commit() the new snapshot  <- only after the run succeeds
//! ```
//!
//! Steps 1–4 are pure and side-effect-free ([`BackfillPlanner::plan`]); committing
//! state (step 6) is explicit ([`BackfillPlanner::commit`]) so a failed run never
//! advances state and the *next* plan still re-backfills the gap.
//!
//! ## Where dagron fits
//!
//! dagron is "a DAG scheduler, not a runtime" (its `ETL_ARCHITECTURE.md`). This
//! planner is the brain that decides *what the DAG should contain*; dagron decides
//! *what runs when* and runs it. The handoff is a [`BackfillPlan`] — `models` is
//! already topologically ordered, so an orchestrator can map it straight onto a
//! task graph.

use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};

pub use freshet_core::{
    avoided_compute_units, estimate_compute_units, estimate_unpruned, plan_backfill,
    plan_backfill_scoped, plan_backfill_scoped_with, BackfillPlan, BackfillReason, BackfillUnit,
    ChangeSpec, Column, ColumnRef, ComputeMode, ComputeUnits, CostBasis, CostEstimate, CostModel,
    IdentityMapper, Materialization, Model, ModelCost, PartitionChange, PartitionMapper, Project,
};
pub use freshet_state::{
    changed_models, fingerprint_project, FileStateBackend, Fingerprint, MemoryStateBackend,
    StateBackend, StateSnapshot,
};

/// The embeddable planner: a project plus a state backend, with the cost model
/// (`CostModel`) all cost / budget paths share.
///
/// The cost model is configurable via [`BackfillPlanner::with_cost_model`] so a
/// hosted tenant on a custom cost model sees the same numbers in the OSS
/// budget gate that the EE crate meters them against — no drift between the
/// quota check and the bill.
pub struct BackfillPlanner<B: StateBackend> {
    project: Project,
    backend: B,
    cost_model: CostModel,
}

/// A computed plan together with the snapshot to persist once it has run.
#[derive(Debug)]
pub struct PlanResult {
    pub plan: BackfillPlan,
    /// The snapshot to hand to [`BackfillPlanner::commit`] after a successful run.
    pub next_snapshot: StateSnapshot,
    /// Predicted execution effort in compute units, using the default [`CostModel`].
    /// Useful in both compute modes: under [`ComputeMode::Hosted`] it is what a metering
    /// embedder reads, and under [`ComputeMode::Byo`] it feeds the self-set
    /// [`ProcessingBudget`] and is a "how heavy is this rebuild?" preview. It is a
    /// quantity of work, not a currency amount — see [`ProcessingBudget`].
    pub cost_estimate: CostEstimate,
}

/// A cap on per-plan processing units — **a guardrail a team sets for itself**, not
/// a tier gate.
///
/// * `Unlimited` — the default, and what every deployment gets unless it opts in.
/// * `Capped(n)` — refuse a plan that would cost more than `n`. The use is a CI gate
///   that fails a pull request whose migration would rewrite the warehouse, or a
///   blast-radius ceiling on an automated restatement job. Under
///   [`ComputeMode::Byo`] the compute it protects is the operator's own, which is
///   exactly why it is theirs to set.
///
/// It is deliberately **not** a tier gate, though it was once documented as one (see
/// `docs/ARCHITECTURE.md` §6, corrected). Two reasons it cannot be: under
/// [`ComputeMode::Byo`] it rations compute the customer pays their own warehouse for,
/// so it prices nothing; and this is Apache-2.0, so the arm below is source the
/// operator compiles and can delete.
///
/// This crate prices nothing at all. It emits a [`CostEstimate`] and enforces a cap the
/// caller chose; what a compute unit is worth in money is an embedder's decision.
///
/// Enforced inside [`BackfillPlanner::plan_with_budget`]; bypassed (intentionally)
/// by [`BackfillPlanner::plan`] for back-compat and uncapped operation. Reachable
/// from every embedding surface as `freshet_embed::PlanRequest::budget_millicu`, and
/// from the CLI as `freshet plan --budget N`, which also reports the **headroom**
/// left — a cap you only learn about by hitting it is an outage, not a budget.
#[derive(Clone, Copy, Debug, Default)]
pub enum ProcessingBudget {
    #[default]
    Unlimited,
    Capped(ComputeUnits),
}

/// The plan would consume more compute units than the [`ProcessingBudget`]
/// permits. Carries both numbers so the caller can render an actionable
/// message ("plan would use 4.2 CU, cap is 1.0 CU — upgrade or scope smaller").
#[derive(Clone, Copy, Debug)]
pub struct BudgetExceeded {
    pub estimated: ComputeUnits,
    pub cap: ComputeUnits,
}

impl std::fmt::Display for BudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "backfill plan would consume {} mCU but processing budget is capped at {} mCU; \
             reduce scope, restate fewer partitions, or raise the budget",
            self.estimated.0, self.cap.0,
        )
    }
}

impl std::error::Error for BudgetExceeded {}

/// The plan that was rejected, without the [`StateSnapshot`] commit token.
///
/// Distinct from [`PlanResult`] on purpose: a [`PlanResult`] carries a
/// `next_snapshot` that is *only valid after a successful run*. Handing one to
/// `commit()` advances fingerprints without actually backfilling, after which
/// the next `plan()` will think there's nothing to do and silently miss
/// rebuilds. Budget rejection is *not* a successful run, so we strip the
/// snapshot here and only surface the parts a UI needs to explain the
/// rejection.
#[derive(Debug)]
pub struct RejectedPlan {
    pub plan: BackfillPlan,
    pub cost_estimate: CostEstimate,
}

/// Returned by [`BackfillPlanner::plan_with_budget`] (and the restatement
/// counterpart) when the cap is hit. Carries the rejected plan + cost so the
/// UI can show *which* models pushed past the cap, but **never** a
/// commit-ready snapshot — see [`RejectedPlan`].
#[derive(Debug)]
pub struct BudgetExceededWithPlan {
    pub error: BudgetExceeded,
    pub plan: RejectedPlan,
}

impl std::fmt::Display for BudgetExceededWithPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}
impl std::error::Error for BudgetExceededWithPlan {}

/// Single source of truth for budget enforcement: both `plan_with_budget` and
/// `plan_restatement_with_budget` go through here so a caller that set a cap
/// cannot sidestep it by switching entrypoints. Returns `Ok(())` when the
/// plan is within budget, `Err(BudgetExceeded)` when it isn't.
fn gate_against_budget(
    _plan: &BackfillPlan,
    cost: &CostEstimate,
    budget: ProcessingBudget,
) -> std::result::Result<(), BudgetExceeded> {
    match budget {
        ProcessingBudget::Unlimited => Ok(()),
        ProcessingBudget::Capped(cap) => {
            if cost.total <= cap {
                Ok(())
            } else {
                Err(BudgetExceeded {
                    estimated: cost.total,
                    cap,
                })
            }
        }
    }
}

impl<B: StateBackend> BackfillPlanner<B> {
    pub fn new(project: Project, backend: B) -> Self {
        Self {
            project,
            backend,
            cost_model: CostModel::default(),
        }
    }

    /// Override the cost model used by `plan()` and the budget gate. An EE
    /// embedder threads a tenant-specific `CostModel` through here so the OSS
    /// quota check and the embedder's meter agree on what a plan costs.
    pub fn with_cost_model(mut self, cost_model: CostModel) -> Self {
        self.cost_model = cost_model;
        self
    }

    pub fn project(&self) -> &Project {
        &self.project
    }

    pub fn cost_model(&self) -> &CostModel {
        &self.cost_model
    }

    /// Single source of truth for cost estimation — used by `plan()` and
    /// `plan_restatement_with_budget` so neither path can drift away from the
    /// configured cost model.
    fn estimate(&self, plan: &BackfillPlan) -> CostEstimate {
        estimate_compute_units(&self.project, plan, &self.cost_model)
    }

    /// Compute the minimal backfill for the project's current state vs. the last
    /// committed snapshot. Pure: touches the backend only to *read*.
    pub fn plan(&self) -> Result<PlanResult> {
        let current = fingerprint_project(&self.project);
        let previous = self.backend.load()?;
        let changed = changed_models(&previous, &current);

        // For each changed model, the whole of its output columns are considered
        // changed (definition-level change; finer granularity can come from a
        // column-diff pass later without changing this contract).
        let mut changed_cols: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for name in &changed {
            let cols = self
                .project
                .get(name)
                .map(|m| m.output_columns())
                .unwrap_or_default();
            changed_cols.insert(name.clone(), cols);
        }

        let plan = plan_backfill(&self.project, &changed_cols);
        let cost_estimate = self.estimate(&plan);

        let next_snapshot = StateSnapshot {
            fingerprints: current,
            watermarks: previous.watermarks,
        };

        Ok(PlanResult {
            plan,
            next_snapshot,
            cost_estimate,
        })
    }

    /// Plan, then reject the plan if its predicted [`ComputeUnits`] exceed the
    /// [`ProcessingBudget`]. This is the entrypoint a deployment that has opted into
    /// a cap uses: same code, just configured with one.
    ///
    /// Returns `Ok(Err(BudgetExceeded))` to keep the IO/IO-shape (`Result<_>`)
    /// for backend-level failures distinct from budget rejection (the latter is
    /// not an error condition, just a policy outcome). The plan is still
    /// computed before the check so callers can show the user *which* models
    /// they'd have to drop.
    pub fn plan_with_budget(
        &self,
        budget: ProcessingBudget,
    ) -> Result<Result<PlanResult, BudgetExceededWithPlan>> {
        let result = self.plan()?;
        match gate_against_budget(&result.plan, &result.cost_estimate, budget) {
            Ok(()) => Ok(Ok(result)),
            Err(error) => Ok(Err(BudgetExceededWithPlan {
                error,
                plan: RejectedPlan {
                    plan: result.plan,
                    cost_estimate: result.cost_estimate,
                },
            })),
        }
    }

    /// Plan a **data restatement**: rebuild the given partitions of `model` and
    /// propagate the partition-scoped backfill downstream (identity-mapped to
    /// incremental models; non-incremental downstreams fall back to full).
    ///
    /// Unlike [`plan`](Self::plan) this is not driven by a code change, so it does
    /// not read or advance fingerprint state — there is nothing to commit. The
    /// restated partitions are treated as touching *all* of the model's columns
    /// (its data changed); downstream column pruning still applies.
    pub fn plan_restatement(
        &self,
        model: &str,
        partitions: impl IntoIterator<Item = String>,
    ) -> Result<BackfillPlan> {
        let columns = self
            .project
            .get(model)
            .map(|m| m.output_columns())
            .ok_or_else(|| anyhow::anyhow!("unknown model for restatement: {model}"))?;
        let spec = ChangeSpec {
            columns,
            partitions: PartitionChange::Partitions(partitions.into_iter().collect()),
        };
        Ok(plan_backfill_scoped(
            &self.project,
            &BTreeMap::from([(model.to_string(), spec)]),
        ))
    }

    /// Restate, then reject the plan if its predicted [`ComputeUnits`] exceed
    /// the [`ProcessingBudget`]. Same gate as [`plan_with_budget`] — both
    /// entrypoints **must** route through the same helper so a capped embedder
    /// cannot sidestep its own cap by driving rebuilds through restatement
    /// instead of code-change planning.
    pub fn plan_restatement_with_budget(
        &self,
        model: &str,
        partitions: impl IntoIterator<Item = String>,
        budget: ProcessingBudget,
    ) -> Result<std::result::Result<BackfillPlan, BudgetExceededWithPlan>> {
        let plan = self.plan_restatement(model, partitions)?;
        let cost_estimate = self.estimate(&plan);
        match gate_against_budget(&plan, &cost_estimate, budget) {
            Ok(()) => Ok(Ok(plan)),
            Err(error) => Ok(Err(BudgetExceededWithPlan {
                error,
                plan: RejectedPlan {
                    plan,
                    cost_estimate,
                },
            })),
        }
    }

    /// Persist the post-run snapshot. Call only after the plan actually ran.
    pub fn commit(&self, snapshot: &StateSnapshot) -> Result<()> {
        self.backend.save(snapshot)
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
    fn model(name: &str, sql: &str, deps: &[&str], cols: Vec<Column>) -> Model {
        Model {
            name: name.into(),
            sql: sql.into(),
            materialization: Materialization::Table,
            columns: cols,
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn first_run_backfills_everything_then_nothing() {
        let project = Project::from_models([
            model(
                "raw",
                "select 1 a, 2 b",
                &[],
                vec![col("a", &[]), col("b", &[])],
            ),
            model(
                "stg",
                "select a, b from raw",
                &["raw"],
                vec![col("a", &[("raw", "a")]), col("b", &[("raw", "b")])],
            ),
        ]);
        let planner = BackfillPlanner::new(project, MemoryStateBackend::default());

        // Cold state: everything is "changed".
        let r = planner.plan().unwrap();
        assert_eq!(r.plan.models, vec!["raw".to_string(), "stg".to_string()]);
        planner.commit(&r.next_snapshot).unwrap();

        // No edits since commit -> empty plan.
        let r2 = planner.plan().unwrap();
        assert!(r2.plan.is_empty(), "nothing changed, nothing to backfill");
    }

    #[test]
    fn restatement_scopes_partitions() {
        let incr = |name: &str, deps: &[&str], cols: Vec<Column>| Model {
            materialization: Materialization::Incremental,
            ..model(name, "select 1", deps, cols)
        };
        let project = Project::from_models([
            incr("stg", &[], vec![col("a", &[])]),
            incr("mart", &["stg"], vec![col("a", &[("stg", "a")])]),
        ]);
        let planner = BackfillPlanner::new(project, MemoryStateBackend::default());

        let plan = planner
            .plan_restatement("stg", ["2026-06-01".to_string()])
            .unwrap();
        assert_eq!(
            plan.unit["stg"],
            BackfillUnit::Partitions(vec!["2026-06-01".into()])
        );
        assert_eq!(
            plan.unit["mart"],
            BackfillUnit::Partitions(vec!["2026-06-01".into()])
        );

        // Typo'd model fails fast instead of returning a silent empty plan.
        assert!(planner
            .plan_restatement("nope", ["2026-06-01".to_string()])
            .is_err());
    }

    #[test]
    fn plan_result_carries_cost_estimate() {
        let project = Project::from_models([
            model("a", "select 1 as x", &[], vec![col("x", &[])]),
            model(
                "b",
                "select x from a",
                &["a"],
                vec![col("x", &[("a", "x")])],
            ),
        ]);
        let planner = BackfillPlanner::new(project, MemoryStateBackend::default());

        // Cold start backfills both Tables -> 2 CU total.
        let r = planner.plan().unwrap();
        assert_eq!(r.cost_estimate.total, ComputeUnits::from_whole(2));
        assert_eq!(r.cost_estimate.per_model.len(), 2);

        planner.commit(&r.next_snapshot).unwrap();

        // No edits -> empty plan -> 0 CU. Planning is not itself work, so re-running
        // `plan()` never consumes budget.
        let r2 = planner.plan().unwrap();
        assert_eq!(r2.cost_estimate.total, ComputeUnits::zero());
    }

    #[test]
    fn unlimited_budget_never_rejects() {
        let project = Project::from_models([model("raw", "select 1 a", &[], vec![col("a", &[])])]);
        let planner = BackfillPlanner::new(project, MemoryStateBackend::default());
        let ok = planner
            .plan_with_budget(ProcessingBudget::Unlimited)
            .unwrap();
        assert!(ok.is_ok(), "Unlimited budget must never reject");
    }

    #[test]
    fn capped_budget_rejects_oversized_plan_but_returns_the_plan() {
        // A tight self-set cap of 1 CU, but cold start would backfill 3 Table
        // models = 3 CU. plan_with_budget must reject AND surface the plan, so
        // the UI can say "you're over by X — these are the models."
        let project = Project::from_models([
            model("a", "select 1 x", &[], vec![col("x", &[])]),
            model(
                "b",
                "select x from a",
                &["a"],
                vec![col("x", &[("a", "x")])],
            ),
            model(
                "c",
                "select x from b",
                &["b"],
                vec![col("x", &[("b", "x")])],
            ),
        ]);
        let planner = BackfillPlanner::new(project, MemoryStateBackend::default());

        let err = planner
            .plan_with_budget(ProcessingBudget::Capped(ComputeUnits::from_whole(1)))
            .unwrap()
            .unwrap_err();
        assert_eq!(err.error.estimated, ComputeUnits::from_whole(3));
        assert_eq!(err.error.cap, ComputeUnits::from_whole(1));
        assert_eq!(
            err.plan.plan.models.len(),
            3,
            "the plan itself is still returned"
        );

        // A higher cap (3 CU) just clears the same plan.
        let ok = planner
            .plan_with_budget(ProcessingBudget::Capped(ComputeUnits::from_whole(3)))
            .unwrap()
            .unwrap();
        assert_eq!(ok.cost_estimate.total, ComputeUnits::from_whole(3));
    }

    #[test]
    fn rejected_plan_has_no_commit_token() {
        // Type-system guarantee: BudgetExceededWithPlan.plan is RejectedPlan,
        // which does not carry a StateSnapshot. A caller cannot accidentally
        // commit a rejected plan's snapshot — there is none to commit.
        //
        // This test is mostly here so the contract is documented and would
        // fail to compile if someone "helpfully" added next_snapshot back.
        let project = Project::from_models([
            model("a", "select 1 x", &[], vec![col("x", &[])]),
            model(
                "b",
                "select x from a",
                &["a"],
                vec![col("x", &[("a", "x")])],
            ),
        ]);
        let planner = BackfillPlanner::new(project, MemoryStateBackend::default());
        let err = planner
            .plan_with_budget(ProcessingBudget::Capped(ComputeUnits::from_whole(1)))
            .unwrap()
            .unwrap_err();
        // RejectedPlan only exposes plan + cost_estimate.
        let _: &BackfillPlan = &err.plan.plan;
        let _: &CostEstimate = &err.plan.cost_estimate;
    }

    #[test]
    fn restatement_honors_budget_cap() {
        // The headline of CodeRabbit's second finding: an embedder that set a cap
        // must not be able to sidestep it by driving rebuilds through restatement
        // instead of plan_with_budget.
        let incr = |name: &str, deps: &[&str], cols: Vec<Column>| Model {
            materialization: Materialization::Incremental,
            ..model(name, "select 1", deps, cols)
        };
        let project = Project::from_models([
            incr("stg", &[], vec![col("a", &[])]),
            incr("mart", &["stg"], vec![col("a", &[("stg", "a")])]),
        ]);
        let planner = BackfillPlanner::new(project, MemoryStateBackend::default());

        // Default CostModel: incremental_partition = 100 mCU. Restating 5
        // partitions of stg propagates to mart -> 10 partitions total -> 1000
        // mCU = 1 CU. Cap of 0.5 CU (500 mCU) must reject.
        let parts: Vec<String> = (1..=5).map(|d| format!("2026-06-0{d}")).collect();
        let err = planner
            .plan_restatement_with_budget(
                "stg",
                parts.clone(),
                ProcessingBudget::Capped(ComputeUnits(500)),
            )
            .unwrap()
            .unwrap_err();
        assert_eq!(err.error.cap, ComputeUnits(500));
        assert!(
            err.error.estimated > ComputeUnits(500),
            "restatement estimate must exceed the cap",
        );
        // The plan itself is still surfaced so the UI can show which partitions
        // pushed past the cap.
        assert!(!err.plan.plan.models.is_empty());

        // A generous cap (5 CU) clears the same restatement.
        let ok = planner
            .plan_restatement_with_budget(
                "stg",
                parts,
                ProcessingBudget::Capped(ComputeUnits::from_whole(5)),
            )
            .unwrap()
            .unwrap();
        assert!(ok.models.contains(&"mart".to_string()));
    }

    #[test]
    fn custom_cost_model_drives_estimate_and_budget_gate() {
        // The injectable seam: a tenant on a custom cost model must see its
        // own pricing in both the cost_estimate AND the budget cap, not the
        // OSS default. Cranking table_full to 10 CU means a single-Table cold
        // start costs 10 CU and is rejected by a 5 CU cap that would have
        // cleared under the default model.
        let project = Project::from_models([model("raw", "select 1 a", &[], vec![col("a", &[])])]);
        let pricey = CostModel {
            table_full: ComputeUnits::from_whole(10),
            ..CostModel::default()
        };
        let planner =
            BackfillPlanner::new(project, MemoryStateBackend::default()).with_cost_model(pricey);

        // Sanity: the configured model is what the planner reports.
        assert_eq!(
            planner.cost_model().table_full,
            ComputeUnits::from_whole(10)
        );

        // Plan reports the inflated cost.
        let r = planner.plan().unwrap();
        assert_eq!(r.cost_estimate.total, ComputeUnits::from_whole(10));

        // A 5 CU cap that would have passed under the default rate (1 CU)
        // must now reject — proving the gate uses the injected model.
        let err = planner
            .plan_with_budget(ProcessingBudget::Capped(ComputeUnits::from_whole(5)))
            .unwrap()
            .unwrap_err();
        assert_eq!(err.error.estimated, ComputeUnits::from_whole(10));
        assert_eq!(err.error.cap, ComputeUnits::from_whole(5));
    }

    #[test]
    fn restatement_with_unlimited_budget_equals_plain_restatement() {
        // Round-trip: an unlimited cap must never alter what plan_restatement
        // would have produced — same models, same units.
        let incr = |name: &str, deps: &[&str], cols: Vec<Column>| Model {
            materialization: Materialization::Incremental,
            ..model(name, "select 1", deps, cols)
        };
        let project = Project::from_models([
            incr("stg", &[], vec![col("a", &[])]),
            incr("mart", &["stg"], vec![col("a", &[("stg", "a")])]),
        ]);
        let planner = BackfillPlanner::new(project, MemoryStateBackend::default());
        let plain = planner.plan_restatement("stg", ["p1".to_string()]).unwrap();
        let gated = planner
            .plan_restatement_with_budget("stg", ["p1".to_string()], ProcessingBudget::Unlimited)
            .unwrap()
            .unwrap();
        assert_eq!(plain.models, gated.models);
        assert_eq!(plain.unit, gated.unit);
    }
}
