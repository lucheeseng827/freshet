//! Per-partition **replace ops**: what a rebuild does to the target relation.
//!
//! ## The gap this fills
//!
//! A plan says *"rebuild `events` for partitions `[2026-06-20, 2026-06-21]`"*. It
//! does not say how those rows get into the table, and the answer is not obvious:
//! overwrite those two partitions, delete the rows matching them and insert, merge
//! on a key, or replace everything satisfying a predicate. Pick wrong and you
//! either duplicate rows or leave stale ones behind — both silent, both data bugs.
//!
//! Today that decision lives in whatever command the operator wired up, which means
//! it is invisible to the planner and therefore unvalidated. This module makes it a
//! declared, checkable property of a model.
//!
//! ## What "abstract" means here, precisely
//!
//! A [`ReplaceOp`] is a *description of the write*, not a way to perform one. There
//! is no SQL in this module, no dialect, and no engine name — `INSERT OVERWRITE`,
//! `MERGE INTO`, `REPLACE WHERE` and `DELETE`+`INSERT` are how four different
//! warehouses spell these ideas, and rendering into any of them is the engine
//! adapter's job, which is roadmap. What lands here is the vocabulary, the facts a
//! renderer needs, and — the part that is actually load-bearing — the rules for
//! when a declaration *cannot* be honoured.
//!
//! ## Widening, not failing
//!
//! This crate's invariant is that over-rebuild is safe and under-rebuild is a data
//! bug, and resolution obeys it without exception: every unhonourable declaration
//! **widens to a full refresh of the whole relation**, and none is ever narrowed or
//! quietly dropped. A `merge` with no key does not merge on nothing; it rebuilds
//! everything.
//!
//! Widening is expensive, so it is never silent either — a widened op carries the
//! [`Widening`] that caused it, in the same shape as the planner's own
//! `BackfillReason`, so a CLI or a review comment can say *why* a model it expected
//! to touch two partitions is about to rewrite the table.
//!
//! ## Undeclared means undeclared
//!
//! [`resolve`] returns `None` for a model with no [`ReplaceSpec`], rather than
//! guessing a default. Warehouses disagree about what the default should be —
//! `merge` on some adapters, `insert_overwrite` on others — so choosing one here
//! would be this crate picking an engine, which is exactly what it must not do. The
//! practical consequence is that adopting this abstraction changes nothing for a
//! project that has not opted in.

use std::collections::BTreeMap;

use freshet_core::{BackfillPlan, BackfillUnit, Materialization, Model, Project};
use serde::{Deserialize, Serialize};

use crate::{IncrementalConfig, Partition};

/// How a rebuild's rows reach the target relation.
///
/// Four partition-wise strategies plus the fallback. The names are the ones the
/// warehouses and dbt already use, so an operator does not have to learn a private
/// vocabulary — but they name *semantics* here, not statements.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplaceStrategy {
    /// Replace whole partitions of the target wholesale. Needs a partition column.
    InsertOverwrite,
    /// Delete the rows belonging to the target partitions, then insert the new ones.
    /// Needs a partition column. Distinct from [`Self::InsertOverwrite`] because on
    /// an engine with no partition primitive the delete is a predicate, not a drop.
    DeleteInsert,
    /// Match existing rows on a unique key and update or insert. Needs
    /// `unique_key`; the partition set bounds which source rows are considered, not
    /// how they are matched.
    Merge,
    /// Replace everything satisfying a predicate over the partition column — the
    /// atomic form of [`Self::DeleteInsert`] where an engine offers it. Needs a
    /// partition column.
    ReplaceWhere,
    /// Rebuild the whole relation. Always available, always correct, and always the
    /// most expensive — which is why it is where every widening lands.
    FullRefresh,
}

impl ReplaceStrategy {
    /// Whether this strategy is meaningless without a partition column.
    ///
    /// [`Self::Merge`] is absent deliberately: it matches on a key, so it can scope
    /// a rebuild without one.
    fn needs_partition_column(self) -> bool {
        matches!(self, Self::InsertOverwrite | Self::DeleteInsert | Self::ReplaceWhere)
    }
}

/// How a partition key is spelled as a SQL literal when a write is scoped to it.
///
/// A fact about the partition *column*, like [`ReplaceSpec::partition_column`]
/// itself, and just as unknowable from the plan: the key `2026-06-20` is a string
/// here, and whether the column it is compared against holds a `DATE`, a `STRING`
/// or an `INT` is something only the person who modelled the table knows.
///
/// Semantics only, like everything in this module — it names the column's kind,
/// not a spelling. A renderer turns [`Self::Date`] into `DATE '2026-06-20'`, and
/// refuses a key that is not one, rather than letting a warehouse coerce it into
/// something that matches different rows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionType {
    /// A quoted string literal — the default, because every supported warehouse
    /// coerces a string literal when it is compared with a date.
    #[default]
    String,
    /// A calendar date, `YYYY-MM-DD`.
    Date,
    /// A whole number, for integer-bucketed partitions.
    Int,
}

/// A model's declared replacement intent. Lives in the incremental config sidecar
/// next to `grain` and `edges`, because it is the same kind of fact: something only
/// the person who modelled the table knows.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaceSpec {
    pub strategy: Option<ReplaceStrategy>,
    /// The column whose values are partition keys. Required by every strategy
    /// except `merge` and `full_refresh`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_column: Option<String>,
    /// The columns `merge` matches on. Ignored by the other strategies.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unique_key: Vec<String>,
    /// What kind of values `partition_column` holds. Absent means
    /// [`PartitionType::String`]. Read only by a renderer; resolution ignores it,
    /// so it can never widen or narrow what a plan asked for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_type: Option<PartitionType>,
}

/// What the write actually covers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplaceTarget {
    /// The entire relation.
    Whole,
    /// These partition keys, sorted and deduplicated.
    Partitions(Vec<Partition>),
}

impl ReplaceTarget {
    /// How many partitions this covers, or `None` for the whole relation.
    ///
    /// Used by the tests to state the safety property in the one form that is
    /// checkable: `None` is unbounded and therefore never narrower than any `Some`.
    pub fn partition_count(&self) -> Option<usize> {
        match self {
            Self::Whole => None,
            Self::Partitions(ps) => Some(ps.len()),
        }
    }
}

/// Why an op is broader than what was declared.
///
/// Each variant names a fact about the model or the plan, never a judgement — the
/// point is that an operator reading it can tell whether to fix the config or
/// accept the cost.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "widened_because")]
pub enum Widening {
    /// The model is not `incremental`, so it has no partitions to replace in place.
    /// A view has no rows at all; a table is rewritten wholesale by definition.
    NotIncremental { materialization: String },
    /// The plan gave nothing to scope the write to — either it rebuilds this model
    /// in full, or it selected the model with an empty partition set.
    ///
    /// Both collapse here because the safe answer and the operator's remedy are the
    /// same, and because the alternative for the empty case is worse than it looks:
    /// a `ReplaceTarget::Partitions([])` is a write that touches nothing, which is
    /// the narrowest target this module could possibly emit, on a model the plan
    /// says must be rebuilt. Under-rebuild is the bug class this crate exists to
    /// prevent, so an empty set widens like every other thing it cannot honour.
    ///
    /// Note this is about the *plan*, not the config: the same model resolves to a
    /// partition-wise op on a plan that scopes it.
    WholeModelInPlan,
    /// The strategy needs a partition column and the config names none. Guessing
    /// one from the SQL would be this crate parsing queries, which is
    /// `freshet-sql`'s job and still would not tell us which column is the
    /// partition key.
    NoPartitionColumn { declared: ReplaceStrategy },
    /// `merge` with nothing to match on. A merge with an empty key either matches
    /// every row or none, depending on the engine — both are wrong, and neither
    /// fails loudly.
    NoUniqueKey,
}

/// A resolved replacement: everything a renderer needs, and nothing about how to
/// render it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaceOp {
    /// The strategy actually in force — the declared one, or `FullRefresh` when
    /// [`Self::widened`] says the declaration could not be honoured.
    pub strategy: ReplaceStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_column: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unique_key: Vec<String>,
    pub target: ReplaceTarget,
    /// `None` means the declaration was honoured exactly as written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub widened: Option<Widening>,
    /// The declared [`PartitionType`], carried through for a renderer. `None` on a
    /// widened op, which has no partitions to spell.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_type: Option<PartitionType>,
}

impl ReplaceOp {
    /// The widened result every unhonourable declaration collapses to.
    fn widen(reason: Widening) -> Self {
        Self {
            strategy: ReplaceStrategy::FullRefresh,
            partition_column: None,
            unique_key: Vec::new(),
            target: ReplaceTarget::Whole,
            widened: Some(reason),
            partition_type: None,
        }
    }

    /// Whether this op rewrites more than the plan's partitions asked for.
    pub fn is_widened(&self) -> bool {
        self.widened.is_some()
    }
}

/// Resolve one model's replace op against the unit the plan gave it.
///
/// `None` when the model declares no strategy — see the module docs on why that is
/// not a defaulting opportunity.
pub fn resolve(model: &Model, spec: Option<&ReplaceSpec>, unit: &BackfillUnit) -> Option<ReplaceOp> {
    let strategy = spec?.strategy?;

    // A declared full refresh is honoured, not widened: the operator asked for the
    // expensive thing, so there is no surprise to report.
    if strategy == ReplaceStrategy::FullRefresh {
        return Some(ReplaceOp {
            strategy,
            partition_column: None,
            unique_key: Vec::new(),
            target: ReplaceTarget::Whole,
            widened: None,
            partition_type: None,
        });
    }

    if model.materialization != Materialization::Incremental {
        return Some(ReplaceOp::widen(Widening::NotIncremental {
            materialization: materialization_name(model.materialization).to_string(),
        }));
    }

    let BackfillUnit::Partitions(partitions) = unit else {
        return Some(ReplaceOp::widen(Widening::WholeModelInPlan));
    };
    if partitions.is_empty() {
        return Some(ReplaceOp::widen(Widening::WholeModelInPlan));
    }

    let spec = spec.expect("checked above");
    if strategy.needs_partition_column() && spec.partition_column.is_none() {
        return Some(ReplaceOp::widen(Widening::NoPartitionColumn { declared: strategy }));
    }
    if strategy == ReplaceStrategy::Merge && spec.unique_key.is_empty() {
        return Some(ReplaceOp::widen(Widening::NoUniqueKey));
    }

    let mut keys: Vec<Partition> = partitions.clone();
    keys.sort();
    keys.dedup();

    Some(ReplaceOp {
        strategy,
        partition_column: spec.partition_column.clone(),
        unique_key: spec.unique_key.clone(),
        target: ReplaceTarget::Partitions(keys),
        widened: None,
        partition_type: spec.partition_type,
    })
}

/// Resolve every model in a plan, skipping the ones that declare nothing.
///
/// Keyed by model name rather than positional, so a caller cannot accidentally pair
/// an op with the wrong model when the plan is reordered or filtered.
pub fn resolve_plan(
    project: &Project,
    cfg: &IncrementalConfig,
    plan: &BackfillPlan,
) -> BTreeMap<String, ReplaceOp> {
    let mut out = BTreeMap::new();
    for name in &plan.models {
        let Some(model) = project.get(name) else { continue };
        let spec = cfg.models.get(name).and_then(|m| m.replace.as_ref());
        let unit = plan.unit.get(name).cloned().unwrap_or(BackfillUnit::FullModel);
        if let Some(op) = resolve(model, spec, &unit) {
            out.insert(name.clone(), op);
        }
    }
    out
}

fn materialization_name(m: Materialization) -> &'static str {
    match m {
        Materialization::Table => "table",
        Materialization::View => "view",
        Materialization::Incremental => "incremental",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freshet_core::{Column, Model};

    fn model(name: &str, materialization: Materialization) -> Model {
        Model {
            name: name.to_string(),
            sql: format!("select 1 as x from raw_{name}"),
            materialization,
            columns: vec![Column { name: "x".into(), upstream: Vec::new() }],
            depends_on: Vec::new(),
        }
    }

    fn spec(strategy: ReplaceStrategy) -> ReplaceSpec {
        ReplaceSpec { strategy: Some(strategy), ..Default::default() }
    }

    fn partitions(keys: &[&str]) -> BackfillUnit {
        BackfillUnit::Partitions(keys.iter().map(|s| s.to_string()).collect())
    }

    /// A plan over `(model, unit)` pairs, in the order given. Every model gets the
    /// same reason: `resolve_plan` reads units and never reasons, so varying them
    /// would only be decoration.
    fn plan_of(entries: &[(&str, Option<BackfillUnit>)]) -> BackfillPlan {
        BackfillPlan {
            models: entries.iter().map(|(n, _)| n.to_string()).collect(),
            reasons: entries
                .iter()
                .map(|(n, _)| (n.to_string(), freshet_core::BackfillReason::DirectlyChanged))
                .collect(),
            unit: entries
                .iter()
                .filter_map(|(n, u)| u.clone().map(|u| (n.to_string(), u)))
                .collect(),
        }
    }

    #[test]
    fn a_model_that_declares_nothing_resolves_to_nothing() {
        let m = model("events", Materialization::Incremental);
        assert_eq!(resolve(&m, None, &partitions(&["2026-06-20"])), None);
        assert_eq!(
            resolve(&m, Some(&ReplaceSpec::default()), &partitions(&["2026-06-20"])),
            None,
            "a spec present but with no strategy is still no declaration — adopting \
             this abstraction must change nothing for a project that has not opted in"
        );
    }

    #[test]
    fn a_well_formed_declaration_is_honoured_verbatim() {
        let m = model("events", Materialization::Incremental);
        let s = ReplaceSpec {
            strategy: Some(ReplaceStrategy::InsertOverwrite),
            partition_column: Some("dt".into()),
            unique_key: Vec::new(),
            partition_type: None,
        };
        let op = resolve(&m, Some(&s), &partitions(&["2026-06-21", "2026-06-20"])).unwrap();

        assert_eq!(op.strategy, ReplaceStrategy::InsertOverwrite);
        assert_eq!(op.partition_column.as_deref(), Some("dt"));
        assert_eq!(op.target, ReplaceTarget::Partitions(vec!["2026-06-20".into(), "2026-06-21".into()]));
        assert!(!op.is_widened());
    }

    #[test]
    fn partition_keys_come_out_sorted_and_deduplicated() {
        let m = model("events", Materialization::Incremental);
        let s = ReplaceSpec {
            strategy: Some(ReplaceStrategy::ReplaceWhere),
            partition_column: Some("dt".into()),
            unique_key: Vec::new(),
            partition_type: None,
        };
        let unit = partitions(&["2026-06-21", "2026-06-20", "2026-06-21"]);
        let op = resolve(&m, Some(&s), &unit).unwrap();
        assert_eq!(
            op.target,
            ReplaceTarget::Partitions(vec!["2026-06-20".into(), "2026-06-21".into()]),
            "a renderer should not have to defend against a repeated key"
        );
    }

    #[test]
    fn a_declared_full_refresh_is_not_a_widening() {
        let m = model("dim", Materialization::Table);
        let op = resolve(&m, Some(&spec(ReplaceStrategy::FullRefresh)), &BackfillUnit::FullModel).unwrap();
        assert_eq!(op.target, ReplaceTarget::Whole);
        assert!(!op.is_widened(), "the operator asked for it; there is no surprise to report");
    }

    #[test]
    fn a_view_or_a_table_cannot_be_replaced_partition_wise() {
        for (materialization, name) in
            [(Materialization::View, "view"), (Materialization::Table, "table")]
        {
            let m = model("thing", materialization);
            let s = ReplaceSpec {
                strategy: Some(ReplaceStrategy::InsertOverwrite),
                partition_column: Some("dt".into()),
                unique_key: Vec::new(),
                partition_type: None,
            };
            let op = resolve(&m, Some(&s), &partitions(&["2026-06-20"])).unwrap();

            assert_eq!(op.strategy, ReplaceStrategy::FullRefresh);
            assert_eq!(op.target, ReplaceTarget::Whole);
            assert_eq!(
                op.widened,
                Some(Widening::NotIncremental { materialization: name.to_string() }),
                "and it should say which materialization, so the operator knows \
                 whether the fix is the config or the model"
            );
        }
    }

    #[test]
    fn a_full_model_plan_widens_even_a_perfect_declaration() {
        let m = model("events", Materialization::Incremental);
        let s = ReplaceSpec {
            strategy: Some(ReplaceStrategy::InsertOverwrite),
            partition_column: Some("dt".into()),
            unique_key: Vec::new(),
            partition_type: None,
        };
        let op = resolve(&m, Some(&s), &BackfillUnit::FullModel).unwrap();
        assert_eq!(op.widened, Some(Widening::WholeModelInPlan));
        assert_eq!(
            op.target,
            ReplaceTarget::Whole,
            "the config is fine; the plan simply gave no partitions to scope to, and \
             overwriting 'all partitions' is not something this crate can enumerate"
        );
    }

    #[test]
    fn a_partition_wise_strategy_without_a_partition_column_widens() {
        let m = model("events", Materialization::Incremental);
        for strategy in
            [ReplaceStrategy::InsertOverwrite, ReplaceStrategy::DeleteInsert, ReplaceStrategy::ReplaceWhere]
        {
            let op = resolve(&m, Some(&spec(strategy)), &partitions(&["2026-06-20"])).unwrap();
            assert_eq!(
                op.widened,
                Some(Widening::NoPartitionColumn { declared: strategy }),
                "{strategy:?} needs a column to scope by"
            );
            assert_eq!(op.target, ReplaceTarget::Whole);
        }
    }

    #[test]
    fn merge_needs_no_partition_column_but_does_need_a_key() {
        let m = model("events", Materialization::Incremental);

        let keyless = resolve(&m, Some(&spec(ReplaceStrategy::Merge)), &partitions(&["2026-06-20"])).unwrap();
        assert_eq!(
            keyless.widened,
            Some(Widening::NoUniqueKey),
            "an empty key matches every row or none depending on the engine — both \
             wrong, neither loud"
        );

        let keyed = ReplaceSpec {
            strategy: Some(ReplaceStrategy::Merge),
            partition_column: None,
            unique_key: vec!["id".into()],
            partition_type: None,
        };
        let op = resolve(&m, Some(&keyed), &partitions(&["2026-06-20"])).unwrap();
        assert!(
            !op.is_widened(),
            "merge matches on a key, so it can scope a rebuild with no partition column"
        );
        assert_eq!(op.unique_key, ["id"]);
    }

    /// The safety property, stated over every combination this module can produce.
    ///
    /// A resolved op must never cover *less* than the plan asked for. `Whole` is
    /// unbounded and so is always safe; a partition list is safe only if it holds
    /// every key the plan named. Nothing here may narrow, because a narrowed write
    /// leaves stale rows behind and no test downstream would ever catch it.
    #[test]
    fn no_resolution_ever_narrows_what_the_plan_asked_for() {
        let strategies = [
            ReplaceStrategy::InsertOverwrite,
            ReplaceStrategy::DeleteInsert,
            ReplaceStrategy::Merge,
            ReplaceStrategy::ReplaceWhere,
            ReplaceStrategy::FullRefresh,
        ];
        let materializations =
            [Materialization::Table, Materialization::View, Materialization::Incremental];
        let columns = [None, Some("dt".to_string())];
        let keys: [Vec<String>; 2] = [Vec::new(), vec!["id".to_string()]];
        let units = [BackfillUnit::FullModel, partitions(&["2026-06-20", "2026-06-21"])];

        let mut checked = 0;
        for strategy in strategies {
            for materialization in materializations {
                for partition_column in &columns {
                    for unique_key in &keys {
                        for unit in &units {
                            let m = model("events", materialization);
                            let s = ReplaceSpec {
                                strategy: Some(strategy),
                                partition_column: partition_column.clone(),
                                unique_key: unique_key.clone(),
                                partition_type: None,
                            };
                            let op = resolve(&m, Some(&s), unit).unwrap();
                            checked += 1;

                            let asked = match unit {
                                BackfillUnit::FullModel => None,
                                BackfillUnit::Partitions(p) => Some(p.clone()),
                            };
                            match (&op.target, asked) {
                                // Unbounded: covers anything.
                                (ReplaceTarget::Whole, _) => {}
                                // A partition list against a full-model plan would
                                // be a narrowing, and must never be produced.
                                (ReplaceTarget::Partitions(got), None) => panic!(
                                    "{strategy:?}/{materialization:?} narrowed a full-model \
                                     rebuild to {got:?}"
                                ),
                                (ReplaceTarget::Partitions(got), Some(want)) => {
                                    for key in want {
                                        assert!(
                                            got.contains(&key),
                                            "{strategy:?}/{materialization:?} dropped partition \
                                             {key} — a stale-row data bug no downstream test \
                                             would catch"
                                        );
                                    }
                                }
                            }

                            // And a widened op is always the one safe answer.
                            if op.is_widened() {
                                assert_eq!(op.strategy, ReplaceStrategy::FullRefresh);
                                assert_eq!(op.target, ReplaceTarget::Whole);
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(checked, 120, "every combination should have been exercised");
    }

    #[test]
    fn resolving_a_plan_covers_only_the_models_that_declared_something() {
        use crate::ModelPartitionConfig;

        let project = Project::from_models([
            model("events", Materialization::Incremental),
            model("daily_rollup", Materialization::Incremental),
            model("legacy_view", Materialization::View),
        ]);

        let cfg = IncrementalConfig {
            models: BTreeMap::from([
                (
                    "events".to_string(),
                    ModelPartitionConfig {
                        replace: Some(ReplaceSpec {
                            strategy: Some(ReplaceStrategy::InsertOverwrite),
                            partition_column: Some("dt".into()),
                            unique_key: Vec::new(),
                            partition_type: None,
                        }),
                        ..Default::default()
                    },
                ),
                (
                    "legacy_view".to_string(),
                    ModelPartitionConfig {
                        replace: Some(spec(ReplaceStrategy::Merge)),
                        ..Default::default()
                    },
                ),
                // Configured for grain only, with no replace declaration.
                (
                    "daily_rollup".to_string(),
                    ModelPartitionConfig { grain: Some(crate::Grain::Day), ..Default::default() },
                ),
            ]),
        };

        let plan = plan_of(&[
            ("events", Some(partitions(&["2026-06-20"]))),
            ("daily_rollup", Some(partitions(&["2026-06-20"]))),
            ("legacy_view", Some(BackfillUnit::FullModel)),
        ]);

        let ops = resolve_plan(&project, &cfg, &plan);

        assert_eq!(
            ops.keys().collect::<Vec<_>>(),
            ["events", "legacy_view"],
            "daily_rollup is in the plan and in the config, but declares no replace \
             strategy — it must not acquire one by being nearby"
        );
        assert!(!ops["events"].is_widened());
        assert_eq!(
            ops["legacy_view"].widened,
            Some(Widening::NotIncremental { materialization: "view".into() })
        );
    }

    #[test]
    fn a_model_missing_from_the_project_is_skipped_not_guessed() {
        let project = Project::from_models([model("events", Materialization::Incremental)]);
        let cfg = IncrementalConfig::default();
        let plan = plan_of(&[("events", None), ("ghost", None)]);

        // A plan naming a model the project does not have is already a bug upstream;
        // this must not compound it by inventing an op for a relation it cannot see.
        assert!(resolve_plan(&project, &cfg, &plan).is_empty());
    }

    #[test]
    fn a_plan_with_no_recorded_unit_is_treated_as_a_full_rebuild() {
        let project = Project::from_models([model("events", Materialization::Incremental)]);
        let cfg = IncrementalConfig {
            models: BTreeMap::from([(
                "events".to_string(),
                crate::ModelPartitionConfig {
                    replace: Some(ReplaceSpec {
                        strategy: Some(ReplaceStrategy::InsertOverwrite),
                        partition_column: Some("dt".into()),
                        unique_key: Vec::new(),
                        partition_type: None,
                    }),
                    ..Default::default()
                },
            )]),
        };
        // No unit recorded for the model at all.
        let plan = plan_of(&[("events", None)]);

        let ops = resolve_plan(&project, &cfg, &plan);
        assert_eq!(
            ops["events"].widened,
            Some(Widening::WholeModelInPlan),
            "an absent unit must read as \"everything\", never as \"no partitions\" — \
             the latter would silently write nothing"
        );
    }

    #[test]
    fn an_empty_partition_set_widens_rather_than_writing_nothing() {
        let m = model("events", Materialization::Incremental);
        let s = ReplaceSpec {
            strategy: Some(ReplaceStrategy::InsertOverwrite),
            partition_column: Some("dt".into()),
            unique_key: Vec::new(),
            partition_type: None,
        };
        let op = resolve(&m, Some(&s), &BackfillUnit::Partitions(Vec::new())).unwrap();

        assert_eq!(
            op.target,
            ReplaceTarget::Whole,
            "a model the plan selected, scoped to no partitions, is contradictory. \
             Emitting Partitions([]) would be a write that touches nothing — the \
             narrowest target this module can produce, on a model that must be \
             rebuilt. The non-narrowing property test cannot catch it, because an \
             empty ask has nothing to drop."
        );
        assert_eq!(op.strategy, ReplaceStrategy::FullRefresh);
        assert_eq!(op.widened, Some(Widening::WholeModelInPlan));
    }

    #[test]
    fn no_resolution_ever_emits_a_target_that_writes_nothing() {
        // The stronger form of the safety property: not just "drops no partition the
        // plan named", but "never produces an empty write".
        for strategy in [
            ReplaceStrategy::InsertOverwrite,
            ReplaceStrategy::DeleteInsert,
            ReplaceStrategy::Merge,
            ReplaceStrategy::ReplaceWhere,
            ReplaceStrategy::FullRefresh,
        ] {
            for unit in [BackfillUnit::FullModel, BackfillUnit::Partitions(Vec::new())] {
                let m = model("events", Materialization::Incremental);
                let s = ReplaceSpec {
                    strategy: Some(strategy),
                    partition_column: Some("dt".into()),
                    unique_key: vec!["id".into()],
                    partition_type: None,
                };
                let op = resolve(&m, Some(&s), &unit).unwrap();
                assert_ne!(
                    op.target.partition_count(),
                    Some(0),
                    "{strategy:?} on {unit:?} produced a write covering no partitions"
                );
            }
        }
    }

    #[test]
    fn a_config_entry_may_declare_only_replace() {
        // Regression: every ModelPartitionConfig field must be independently
        // optional. Without a struct-level serde default, serde demands each key
        // and a replace-only entry fails with "missing field `edges`" — which is
        // exactly the config a project adopting replace ops writes first.
        let cfg: IncrementalConfig = serde_json::from_str(
            r#"{"models":{"events":{"replace":{"strategy":"insert_overwrite","partition_column":"dt"}}}}"#,
        )
        .expect("a replace-only entry must parse");
        let m = &cfg.models["events"];
        assert!(m.grain.is_none());
        assert!(m.edges.is_empty());
        assert_eq!(m.replace.as_ref().unwrap().strategy, Some(ReplaceStrategy::InsertOverwrite));

        // And the other direction, plus an empty edge policy taking its default.
        let cfg: IncrementalConfig =
            serde_json::from_str(r#"{"models":{"mart":{"grain":"Day","edges":{"raw":{}}}}}"#)
                .expect("a grain-only entry must parse");
        assert!(cfg.models["mart"].replace.is_none());
        assert_eq!(cfg.models["mart"].edges["raw"].window, 1);

        assert!(serde_json::from_str::<IncrementalConfig>("{}").is_ok());
    }

    #[test]
    fn a_spec_round_trips_through_its_config_json() {
        let json = r#"{"strategy":"insert_overwrite","partition_column":"dt"}"#;
        let parsed: ReplaceSpec = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.strategy, Some(ReplaceStrategy::InsertOverwrite));
        assert_eq!(parsed.partition_column.as_deref(), Some("dt"));
        assert_eq!(serde_json::to_string(&parsed).unwrap(), json);
    }

    #[test]
    fn a_widening_serializes_with_its_reason_named() {
        let op = ReplaceOp::widen(Widening::NoUniqueKey);
        let json = serde_json::to_value(&op).unwrap();
        assert_eq!(json["strategy"], "full_refresh");
        assert_eq!(json["target"], "whole");
        assert_eq!(json["widened"]["widened_because"], "no_unique_key");
    }
}
