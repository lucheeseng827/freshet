//! Measured cost: what a team's own runs of each model cost, folded back into a plan's
//! estimate — opt-in, kept in a file the team owns, and never sent anywhere.
//!
//! The planner's [`ComputeUnits`] are a rate card: one CU is one full rebuild of one
//! `table` model, whatever it holds. That ranks two plans of one project; it does not
//! say what either will cost. A team that records what its runs cost — seconds of
//! warehouse time, credits, bytes scanned, in whatever unit it bills itself in — gets
//! an estimate in that unit instead, model by model:
//!
//! * a model with runs of its own is estimated at their **median** (a whole rebuild,
//!   or per partition times the partitions planned);
//! * a model without is **extrapolated** at the project's measured rate per CU — the
//!   median, over every measured model, of what one of its CU cost — times the CU the
//!   rate card gives it;
//! * with no runs recorded at all there is nothing to extrapolate from, and the
//!   estimate stays a rate card.
//!
//! Every figure says how much of it was measured, because an estimate that is mostly
//! extrapolated is a rate card with a different label, and the reader should know.
//! The same method prices the unpruned baseline, so the rebuild work avoided comes
//! out in the team's unit too, and summing it across plans is theirs to do.
//!
//! The record is `costs.json`, beside the project's `sources.json`:
//!
//! ```json
//! {"unit": "seconds", "models": {"stg_orders": {"full": [12.4, 11.8], "partition": [0.8]}}}
//! ```
//!
//! `full` holds what whole rebuilds cost, `partition` what one partition of a scoped
//! rebuild cost (a run of N partitions is recorded as its cost over N), each the last
//! [`KEEP`] runs, oldest first.

use std::collections::BTreeMap;

use freshet_core::{
    estimate_compute_units, estimate_unpruned, BackfillPlan, BackfillUnit, ComputeUnits, CostModel, Project,
};
use serde::{Deserialize, Serialize};

/// How many runs of each kind a model keeps: enough for a median to shrug off an
/// outlier, few enough that the estimate follows a model whose cost has moved.
pub const KEEP: usize = 20;

/// A team's record of what its runs cost: `costs.json`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Costs {
    /// The unit every value is in: `seconds`, `credits`, `bytes`, … Freshet never
    /// converts between units, so one file holds one.
    pub unit: String,
    #[serde(default)]
    pub models: BTreeMap<String, ModelRuns>,
}

/// One model's recorded runs.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelRuns {
    /// What whole rebuilds cost, oldest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub full: Vec<f64>,
    /// What one partition of a partition-scoped rebuild cost, oldest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition: Vec<f64>,
}

impl Costs {
    /// Record one run of `model` that cost `value`: a whole rebuild, or, with
    /// `partitions`, a scoped one of that many partitions.
    pub fn record(&mut self, model: &str, value: f64, partitions: Option<usize>) -> anyhow::Result<()> {
        if !value.is_finite() || value < 0.0 {
            anyhow::bail!("a run's cost is a finite number >= 0, not {value}");
        }
        let runs = self.models.entry(model.to_string()).or_default();
        let kept = match partitions {
            Some(0) => anyhow::bail!("a scoped run covers at least one partition"),
            Some(n) => {
                runs.partition.push(value / n as f64);
                &mut runs.partition
            }
            None => {
                runs.full.push(value);
                &mut runs.full
            }
        };
        if kept.len() > KEEP {
            kept.drain(..kept.len() - KEEP);
        }
        Ok(())
    }
}

/// Where a model's figure came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// The model's own recorded runs.
    Measured,
    /// The project's measured rate per CU, times the CU the rate card gives it.
    Extrapolated,
}

/// One planned model's measured estimate.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ModelEstimate {
    pub model: String,
    pub value: f64,
    pub source: Source,
}

/// A plan priced in the team's own unit.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MeasuredEstimate {
    pub unit: String,
    /// The whole plan.
    pub total: f64,
    /// How much of `total` came from models' own runs; the rest is extrapolated.
    pub measured: f64,
    /// The rate unmeasured models were extrapolated at, per whole CU.
    pub per_cu: f64,
    /// Rebuilding every model downstream of the change whole, priced the same way.
    pub unpruned: f64,
    pub per_model: Vec<ModelEstimate>,
}

impl MeasuredEstimate {
    /// The rebuild work the plan avoids, in the team's unit: never below zero.
    pub fn avoided(&self) -> f64 {
        (self.unpruned - self.total).max(0.0)
    }

    /// The share of `total` that was measured rather than extrapolated, 0 to 1.
    pub fn measured_share(&self) -> f64 {
        if self.total > 0.0 {
            self.measured / self.total
        } else {
            1.0
        }
    }
}

/// Price `plan` from `costs`, or `None` when no run of any model is recorded: there is
/// then nothing measured to price it from, and the rate card is the estimate.
pub fn estimate(project: &Project, plan: &BackfillPlan, costs: &Costs) -> Option<MeasuredEstimate> {
    let rate = CostModel::default();
    let per_cu = per_cu(project, costs, &rate)?;
    let priced = |plan: &BackfillPlan| -> (f64, f64, Vec<ModelEstimate>) {
        let cu = estimate_compute_units(project, plan, &rate);
        let (mut total, mut measured) = (0.0, 0.0);
        let mut per_model = Vec::with_capacity(cu.per_model.len());
        for m in &cu.per_model {
            let runs = costs.models.get(&m.model);
            let own = match plan.unit.get(&m.model) {
                Some(BackfillUnit::Partitions(ps)) => {
                    runs.and_then(|r| median(&r.partition)).map(|p| p * ps.len() as f64)
                }
                _ => runs.and_then(|r| median(&r.full)),
            };
            let (value, source) = match own {
                Some(v) => (v, Source::Measured),
                None => (whole(m.units) * per_cu, Source::Extrapolated),
            };
            total += value;
            if source == Source::Measured {
                measured += value;
            }
            per_model.push(ModelEstimate { model: m.model.clone(), value, source });
        }
        (total, measured, per_model)
    };
    let (total, measured, per_model) = priced(plan);
    let unpruned_plan = BackfillPlan {
        models: estimate_unpruned(project, plan, &rate).per_model.into_iter().map(|m| m.model).collect(),
        reasons: BTreeMap::new(),
        unit: BTreeMap::new(),
    };
    let (unpruned, _, _) = priced(&unpruned_plan);
    Some(MeasuredEstimate { unit: costs.unit.clone(), total, measured, per_cu, unpruned, per_model })
}

/// The median, over every model with recorded runs, of what one CU of it cost. A
/// whole rebuild is weighed at its model's full CU, a partition at the rate card's
/// per-partition CU.
fn per_cu(project: &Project, costs: &Costs, rate: &CostModel) -> Option<f64> {
    let mut ratios = Vec::new();
    for (name, runs) in &costs.models {
        // A model the project no longer has says nothing about this project's rate.
        if project.get(name).is_none() {
            continue;
        }
        let whole_plan = BackfillPlan {
            models: vec![name.clone()],
            reasons: BTreeMap::new(),
            unit: BTreeMap::new(),
        };
        let full_cu = whole(estimate_compute_units(project, &whole_plan, rate).total);
        if let (Some(v), true) = (median(&runs.full), full_cu > 0.0) {
            ratios.push(v / full_cu);
        }
        let partition_cu = whole(rate.incremental_partition);
        if let (Some(v), true) = (median(&runs.partition), partition_cu > 0.0) {
            ratios.push(v / partition_cu);
        }
    }
    median(&ratios)
}

fn whole(units: ComputeUnits) -> f64 {
    units.0 as f64 / 1000.0
}

fn median(xs: &[f64]) -> Option<f64> {
    if xs.is_empty() {
        return None;
    }
    let mut sorted = xs.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mid = sorted.len() / 2;
    Some(if sorted.len() % 2 == 1 { sorted[mid] } else { (sorted[mid - 1] + sorted[mid]) / 2.0 })
}

#[cfg(test)]
mod tests {
    use super::*;
    use freshet_core::{plan_backfill, Materialization, Model};

    /// raw -> stg -> {a, b}; `big` beside them, a table nobody planned.
    fn project() -> Project {
        let mut built = freshet_sql::build_project(
            &[
                ("stg".into(), "select x, y from raw".into()),
                ("a".into(), "select x from stg".into()),
                ("b".into(), "select y from stg".into()),
                ("big".into(), "select x from raw".into()),
            ],
            &[("raw".to_string(), vec!["x".to_string(), "y".to_string()])].into(),
        )
        .unwrap();
        // `b` a view: a twentieth of a CU on the rate card.
        let models: Vec<Model> = built
            .names()
            .map(|n| {
                let mut m = built.get(n).unwrap().clone();
                if n == "b" {
                    m.materialization = Materialization::View;
                }
                m
            })
            .collect();
        built = Project::from_models(models);
        built
    }

    fn whole_change(p: &Project, model: &str) -> BackfillPlan {
        let cols = p.get(model).unwrap().output_columns();
        plan_backfill(p, &BTreeMap::from([(model.to_string(), cols)]))
    }

    #[test]
    fn no_runs_recorded_is_no_measured_estimate() {
        let p = project();
        assert_eq!(estimate(&p, &whole_change(&p, "stg"), &Costs { unit: "s".into(), ..Costs::default() }), None);
    }

    #[test]
    fn a_measured_model_is_its_median_and_the_rest_extrapolate_at_the_measured_rate() {
        let p = project();
        let mut costs = Costs { unit: "seconds".into(), ..Costs::default() };
        for v in [10.0, 12.0, 100.0] {
            costs.record("stg", v, None).unwrap(); // median 12: an outlier does not move it
        }
        costs.record("big", 4.0, None).unwrap();
        let e = estimate(&p, &whole_change(&p, "stg"), &costs).unwrap();
        // Rate per CU: the median of stg's 12/1 and big's 4/1 — 8 seconds a CU.
        assert_eq!(e.per_cu, 8.0);
        let by = |m: &str| e.per_model.iter().find(|x| x.model == m).unwrap().clone();
        assert_eq!((by("stg").value, by("stg").source), (12.0, Source::Measured));
        assert_eq!((by("a").value, by("a").source), (8.0, Source::Extrapolated), "a table: 1 CU");
        assert_eq!((by("b").value, by("b").source), (0.4, Source::Extrapolated), "a view: 0.05 CU");
        assert_eq!(e.total, 20.4);
        assert_eq!(e.measured, 12.0);
        assert!((e.measured_share() - 12.0 / 20.4).abs() < 1e-12);
        // Nothing pruned here: every model downstream of stg is in the plan.
        assert_eq!((e.unpruned, e.avoided()), (20.4, 0.0));
    }

    #[test]
    fn a_pruned_plan_avoids_the_unpruned_work_in_the_same_unit() {
        let p = project();
        let mut costs = Costs { unit: "credits".into(), ..Costs::default() };
        costs.record("stg", 2.0, None).unwrap();
        costs.record("a", 6.0, None).unwrap();
        costs.record("b", 1.0, None).unwrap();
        // Only stg's `y` changed: `a` reads `x` and is pruned.
        let plan = plan_backfill(&p, &BTreeMap::from([("stg".to_string(), ["y".to_string()].into())]));
        let e = estimate(&p, &plan, &costs).unwrap();
        assert_eq!(e.per_model.iter().map(|m| m.model.as_str()).collect::<Vec<_>>(), ["stg", "b"]);
        assert_eq!((e.total, e.measured, e.unpruned), (3.0, 3.0, 9.0));
        assert_eq!(e.avoided(), 6.0, "a's six credits, not rebuilt");
    }

    #[test]
    fn a_partition_run_is_kept_per_partition_and_scales_with_the_partitions_planned() {
        let mut costs = Costs { unit: "seconds".into(), ..Costs::default() };
        costs.record("events", 9.0, Some(3)).unwrap();
        assert_eq!(costs.models["events"].partition, [3.0]);
        assert!(costs.record("events", 1.0, Some(0)).is_err());
        assert!(costs.record("events", f64::NAN, None).is_err());
        assert!(costs.record("events", -1.0, None).is_err());
    }

    #[test]
    fn only_the_last_runs_are_kept() {
        let mut costs = Costs { unit: "s".into(), ..Costs::default() };
        for v in 0..(KEEP + 5) {
            costs.record("m", v as f64, None).unwrap();
        }
        assert_eq!(costs.models["m"].full.len(), KEEP);
        assert_eq!(costs.models["m"].full[0], 5.0, "the oldest went first");
    }

    #[test]
    fn the_record_round_trips_through_its_json() {
        let raw = r#"{"unit":"seconds","models":{"stg":{"full":[12.4,11.8],"partition":[0.8]}}}"#;
        let costs: Costs = serde_json::from_str(raw).unwrap();
        assert_eq!(serde_json::to_string(&costs).unwrap(), raw);
    }
}
