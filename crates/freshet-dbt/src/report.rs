//! The explain report: what Freshet would rebuild for the change between two manifests,
//! set against what dbt did (the last run) and would do (`state:modified+`).
//!
//! Every number here is a count of models, and every rule that decides one leans
//! towards Freshet rebuilding *more*: model-level lineage for anything uncertain, every
//! column of a model changed unless a narrower set is proven, a net saving that
//! subtracts anything Freshet would build that the baseline did not. The report can
//! understate the saving; it is built so it cannot overstate it.

use std::collections::{BTreeSet, VecDeque};
use std::fmt::Write as _;

use freshet_core::{plan_backfill, BackfillReason, Materialization};
use serde::Serialize;

use crate::artifacts::{Manifest, Node, RunResults};
use crate::diff::{self, Diff};
use crate::project::{has_compiled_sql, DbtProject, Lineage, SqlBasis};

/// Bumped when a field's meaning changes.
pub const REPORT_VERSION: u32 = 1;

/// Where the artifacts were read from, for the report's own labels.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Artifacts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_results: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_manifest: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExplainReport {
    pub report_version: u32,
    pub artifacts: Artifacts,
    pub graph: GraphFacts,
    pub last_run: LastRun,
    /// Models whose lineage is model-level, and why. Each one can only make Freshet
    /// rebuild more.
    pub model_level: Vec<ModelLevel>,
    /// Present only when a baseline manifest was given: without one there is no change
    /// to plan, and no pruning number.
    pub comparison: Option<Comparison>,
    /// What the numbers assume, in plain words.
    pub notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct GraphFacts {
    /// Models in the current manifest.
    pub models: usize,
    /// Models dbt builds: all but the ephemeral ones.
    pub runnable: usize,
    /// Inlined into their consumers as CTEs; never built on their own.
    pub ephemeral: usize,
    pub column_lineage: usize,
    pub model_lineage: usize,
    /// Models the current manifest has compiled SQL for.
    pub compiled_sql: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct ModelLevel {
    pub model: String,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct LastRun {
    /// `run_results`: counted from `target/run_results.json`. `every_model`: there was
    /// no usable run recorded, so every model counts — what a plain `dbt run` builds.
    pub source: String,
    /// The dbt command that wrote `run_results.json`, when it says.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Models it built that the current manifest has (ephemeral ones excluded).
    pub models: usize,
    /// Model results for unique_ids the current manifest no longer has; not counted.
    pub not_in_manifest: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct Comparison {
    pub compared_on: ComparedOn,
    /// Directly changed models, new ones included.
    pub changed: Vec<ChangedModel>,
    /// Models the baseline had and the current manifest does not.
    pub removed: Vec<String>,
    /// Freshet's plan, in dependency order (ephemeral models left out).
    pub freshet: Vec<Rebuild>,
    /// Absent when nothing changed: a saving against an empty plan is not a saving.
    pub vs_last_run: Option<Baseline>,
    pub vs_state_modified_plus: Option<Baseline>,
}

/// How many models each side was read from compiled SQL, and how many from raw SQL
/// (compiled SQL is used only when both manifests have it).
#[derive(Clone, Debug, Serialize)]
pub struct ComparedOn {
    pub compiled_sql: usize,
    pub raw_sql: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChangedModel {
    pub model: String,
    pub because: Vec<String>,
    /// The output columns that changed; `null` means every one.
    pub columns: Option<Vec<String>>,
    /// Why a SQL-only change still counts every column.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub all_columns_because: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Rebuild {
    pub model: String,
    pub reason: Reason,
    pub materialized: String,
    /// `column` or `model`: how precisely this model's inputs were traced.
    pub lineage: String,
}

/// The same shape freshet-embed puts on the wire for a plan's reason.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    DirectlyChanged,
    Downstream { because_of: String, via_columns: Vec<String> },
}

/// Freshet's plan against one baseline set of models.
#[derive(Clone, Debug, Serialize)]
pub struct Baseline {
    pub label: String,
    /// Models the baseline builds.
    pub models: usize,
    /// Of those, the ones Freshet rebuilds too.
    pub freshet_rebuilds: usize,
    /// Models the baseline builds and Freshet does not.
    pub pruned: Vec<String>,
    /// Models Freshet rebuilds that the baseline did not build.
    pub freshet_extra: Vec<String>,
    /// Net: `models - (freshet_rebuilds + freshet_extra)`. Negative when Freshet would
    /// build more than the baseline did.
    pub avoided: i64,
    /// `avoided / models`, as a percentage rounded to one decimal.
    pub avoided_pct: f64,
}

/// Build the report from artifacts already in memory.
pub fn explain(
    current: &Manifest,
    state: Option<&Manifest>,
    run_results: Option<&RunResults>,
    artifacts: Artifacts,
) -> ExplainReport {
    // With a baseline, one basis per model for both sides: compiled SQL only where both
    // manifests have it, so the two fingerprints are of comparable text.
    let (cur, prev) = match state {
        Some(prev_m) => (
            DbtProject::build_with(current, &|n| shared_basis(n, prev_m)),
            Some(DbtProject::build_with(prev_m, &|n| shared_basis(n, current))),
        ),
        None => (DbtProject::build(current), None),
    };

    let runnable: BTreeSet<String> =
        cur.models.values().filter(|m| !m.is_ephemeral()).map(|m| m.name.clone()).collect();
    let graph = GraphFacts {
        models: cur.models.len(),
        runnable: runnable.len(),
        ephemeral: cur.models.len() - runnable.len(),
        column_lineage: cur.models.values().filter(|m| m.lineage == Lineage::Columns).count(),
        model_lineage: cur.models.values().filter(|m| m.lineage != Lineage::Columns).count(),
        compiled_sql: cur.models.values().filter(|m| m.has_compiled_sql).count(),
    };
    let model_level: Vec<ModelLevel> = cur
        .models
        .values()
        .filter_map(|m| match &m.lineage {
            Lineage::ModelLevel { reason } => Some(ModelLevel { model: m.name.clone(), reason: reason.clone() }),
            Lineage::Columns => None,
        })
        .collect();
    let (last_run, last_run_models, run_note) = last_run(&cur, &runnable, run_results);

    let mut notes = Vec::new();
    let comparison = match (&prev, state) {
        (Some(prev), Some(prev_m)) => {
            // Only a comparison has a last-run number for the note to qualify.
            notes.extend(run_note);
            Some(compare(&cur, prev, current, prev_m, &runnable, &last_run, &last_run_models, &mut notes))
        }
        _ => {
            notes.push(
                "No baseline manifest (--state), so there is no change to plan and no pruning number. \
                 Point --state at a directory holding the manifest.json to compare against — dbt's own \
                 --state convention, e.g. your production run's artifacts."
                    .to_string(),
            );
            None
        }
    };
    if !model_level.is_empty() {
        notes.push(format!(
            "{} model(s) have model-level lineage: each rebuilds whenever anything it reads changes. \
             That only ever adds rebuilds, so it can make the saving smaller, never larger.",
            model_level.len()
        ));
    }
    if graph.ephemeral > 0 {
        notes.push(format!(
            "{} ephemeral model(s) are inlined into the models that read them and never built on their \
             own; they are left out of every count.",
            graph.ephemeral
        ));
    }
    if last_run.not_in_manifest > 0 {
        notes.push(format!(
            "{} model result(s) in run_results.json are for models the current manifest does not have; \
             they are not counted.",
            last_run.not_in_manifest
        ));
    }

    ExplainReport {
        report_version: REPORT_VERSION,
        artifacts,
        graph,
        last_run,
        model_level,
        comparison,
        notes,
    }
}

/// Compiled SQL when this node has it and its counterpart in `other` (if any) does too.
fn shared_basis(node: &Node, other: &Manifest) -> SqlBasis {
    let counterpart_compiled = other.nodes.get(&node.unique_id).is_none_or(has_compiled_sql);
    if has_compiled_sql(node) && counterpart_compiled {
        SqlBasis::Compiled
    } else {
        SqlBasis::Raw
    }
}

/// What the last dbt invocation built, as planner names; and a note when that is not
/// what `run_results.json` literally says.
fn last_run(
    cur: &DbtProject,
    runnable: &BTreeSet<String>,
    run_results: Option<&RunResults>,
) -> (LastRun, BTreeSet<String>, Option<String>) {
    let every = |command: Option<String>, note: String| {
        let facts = LastRun { source: "every_model".into(), command, models: runnable.len(), not_in_manifest: 0 };
        (facts, runnable.clone(), Some(note))
    };
    let Some(rr) = run_results else {
        return every(
            None,
            "No target/run_results.json: the last-run comparison counts every model, which is what a \
             plain `dbt run` builds."
                .into(),
        );
    };
    let command = rr.command().map(str::to_string);
    if let Some(cmd) = command.as_deref().filter(|c| !matches!(*c, "run" | "build" | "retry")) {
        return every(
            command.clone(),
            format!(
                "target/run_results.json was written by `dbt {cmd}`, which builds no model: the last-run \
                 comparison counts every model, which is what a plain `dbt run` builds."
            ),
        );
    }
    let mut built = BTreeSet::new();
    let mut not_in_manifest = 0;
    for r in &rr.results {
        // Attempted builds: a failed one still ran. Skipped ones did not.
        if !matches!(r.status.as_str(), "success" | "error" | "partial success") {
            continue;
        }
        match cur.name_of(&r.unique_id) {
            Some(name) if runnable.contains(name) => {
                built.insert(name.to_string());
            }
            Some(_) => {}
            None if r.unique_id.starts_with("model.") => not_in_manifest += 1,
            None => {}
        }
    }
    if built.is_empty() && not_in_manifest == 0 {
        return every(
            command,
            "target/run_results.json records no model build: the last-run comparison counts every \
             model, which is what a plain `dbt run` builds."
                .into(),
        );
    }
    let facts = LastRun { source: "run_results".into(), command, models: built.len(), not_in_manifest };
    (facts, built, None)
}

#[allow(clippy::too_many_arguments)]
fn compare(
    cur: &DbtProject,
    prev: &DbtProject,
    cur_m: &Manifest,
    prev_m: &Manifest,
    runnable: &BTreeSet<String>,
    last_run: &LastRun,
    last_run_models: &BTreeSet<String>,
    notes: &mut Vec<String>,
) -> Comparison {
    let Diff { changed, removed } = diff::diff(cur, prev, cur_m, prev_m);

    // Freshet's plan: every changed model with its changed columns (all of them unless
    // a narrower set was proven), through the planner core as-is.
    let seeds = changed
        .iter()
        .map(|c| {
            let cols = c.columns.clone().unwrap_or_else(|| cur.model(&c.model).output_columns());
            (c.model.clone(), cols)
        })
        .collect();
    let plan = plan_backfill(&cur.project, &seeds);
    let freshet: Vec<Rebuild> = plan
        .models
        .iter()
        .filter(|m| runnable.contains(*m))
        .map(|name| {
            let model = &cur.models[name];
            let reason = match plan.reasons.get(name) {
                Some(BackfillReason::Downstream { because_of, via_columns }) => {
                    Reason::Downstream { because_of: because_of.clone(), via_columns: via_columns.clone() }
                }
                Some(BackfillReason::DirectlyChanged) | None => Reason::DirectlyChanged,
            };
            let lineage = if model.lineage == Lineage::Columns { "column" } else { "model" };
            Rebuild { model: name.clone(), reason, materialized: model.materialized.clone(), lineage: lineage.into() }
        })
        .collect();
    let freshet_set: BTreeSet<String> = freshet.iter().map(|r| r.model.clone()).collect();

    // state:modified+ as Freshet approximates it: the changed models and every
    // descendant along the manifest's edges.
    let mut modified_plus: BTreeSet<String> = changed.iter().map(|c| c.model.clone()).collect();
    let mut queue: VecDeque<String> = modified_plus.iter().cloned().collect();
    while let Some(name) = queue.pop_front() {
        for d in cur.project.downstream_of(&name) {
            if modified_plus.insert(d.clone()) {
                queue.push_back(d.clone());
            }
        }
    }
    modified_plus.retain(|m| runnable.contains(m));

    let (vs_last_run, vs_state_modified_plus) = if changed.is_empty() {
        notes.push(
            "Nothing changed between the two manifests, so Freshet plans no rebuild and there is \
             nothing to compare."
                .to_string(),
        );
        (None, None)
    } else {
        let last_label = match (last_run.source.as_str(), last_run.command.as_deref()) {
            ("run_results", Some(cmd)) => format!("the last `dbt {cmd}`"),
            ("run_results", None) => "the last dbt run".to_string(),
            _ => "every model (a plain `dbt run`)".to_string(),
        };
        (
            Some(baseline(last_label, last_run_models, &freshet_set)),
            Some(baseline("state:modified+ (Freshet's approximation)".to_string(), &modified_plus, &freshet_set)),
        )
    };

    let compared_on = ComparedOn {
        compiled_sql: cur.models.values().filter(|m| m.basis == SqlBasis::Compiled).count(),
        raw_sql: cur.models.values().filter(|m| m.basis == SqlBasis::Raw).count(),
    };

    if vs_last_run.is_some() {
        if last_run.source == "run_results" {
            notes.push(
                "The last-run comparison assumes that run is the one that built this change. A \
                 scheduled run that only picked up new data is not a rebuild Freshet plans."
                    .to_string(),
            );
        }
        notes.push(
            "state:modified+ here is Freshet's approximation: the models Freshet found changed plus \
             every descendant. dbt decides \"modified\" from its own checksums and config \
             comparisons, so its selection can differ."
                .to_string(),
        );
        notes.push("Counts are models, not cost: a view and a table each count as one rebuild.".to_string());
    }
    if compared_on.raw_sql > 0 {
        notes.push(format!(
            "{} model(s) were compared on raw SQL because a manifest has no compiled SQL for them. A \
             change that only shows once Jinja renders (a var, a target-dependent branch) is invisible \
             to that comparison: run `dbt compile` for both manifests to close the gap.",
            compared_on.raw_sql
        ));
    }
    let rendered_only = changed.iter().filter(|c| c.because.iter().any(|b| b == diff::RENDERED_ONLY)).count();
    if rendered_only > 0 {
        notes.push(format!(
            "{rendered_only} model(s) changed only in their compiled SQL — the source file is the same, \
             so rendering differed: an incremental model's is_incremental() branch (compiled only where \
             its table exists), a var, or the target. They count as changed, which can only make the \
             saving smaller; compile both manifests against the same target to avoid it."
        ));
    }
    if freshet.iter().any(|r| materialization_is_incremental(&r.materialized)) {
        notes.push(
            "An incremental model a code change reaches is rebuilt in full (a full refresh): its \
             existing rows were computed by the old logic. `dbt run` would process only new rows."
                .to_string(),
        );
    }

    Comparison {
        compared_on,
        changed: changed
            .into_iter()
            .map(|c| ChangedModel {
                model: c.model,
                because: c.because,
                columns: c.columns.map(|cols| cols.into_iter().collect()),
                all_columns_because: c.all_columns_because,
            })
            .collect(),
        removed,
        freshet,
        vs_last_run,
        vs_state_modified_plus,
    }
}

fn materialization_is_incremental(materialized: &str) -> bool {
    crate::project::materialization(materialized) == Materialization::Incremental
}

fn baseline(label: String, base: &BTreeSet<String>, freshet: &BTreeSet<String>) -> Baseline {
    let pruned: Vec<String> = base.difference(freshet).cloned().collect();
    let freshet_extra: Vec<String> = freshet.difference(base).cloned().collect();
    let avoided = pruned.len() as i64 - freshet_extra.len() as i64;
    let avoided_pct = if base.is_empty() { 0.0 } else { (avoided as f64 / base.len() as f64 * 1000.0).round() / 10.0 };
    Baseline {
        label,
        models: base.len(),
        freshet_rebuilds: base.len() - pruned.len(),
        pruned,
        freshet_extra,
        avoided,
        avoided_pct,
    }
}

// ---------------------------------------------------------------- rendering --

impl ExplainReport {
    /// The report as a person reads it in a terminal.
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        let names = self
            .model_level
            .iter()
            .map(|m| m.model.len())
            .chain(self.comparison.iter().flat_map(|c| {
                c.changed.iter().map(|m| m.model.len()).chain(c.freshet.iter().map(|r| r.model.len()))
            }))
            .max()
            .unwrap_or(0);
        let w = names.max(22) + 2;
        let g = &self.graph;

        let _ = writeln!(
            out,
            "freshet explain: {} dbt model(s), {} built by dbt{}",
            g.models,
            g.runnable,
            if g.ephemeral > 0 { format!(" ({} ephemeral)", g.ephemeral) } else { String::new() }
        );
        let _ = writeln!(
            out,
            "  lineage: column-level for {}, model-level for {} · compiled SQL for {} of {}",
            g.column_lineage, g.model_lineage, g.compiled_sql, g.models
        );
        let run = &self.last_run;
        let _ = match (run.source.as_str(), run.command.as_deref()) {
            ("run_results", Some(cmd)) => writeln!(out, "  last run: `dbt {cmd}` built {} model(s)", run.models),
            ("run_results", None) => writeln!(out, "  last run: built {} model(s)", run.models),
            _ if self.comparison.is_some() => {
                writeln!(out, "  last run: not recorded — counting every model ({}), as `dbt run` would", run.models)
            }
            _ => writeln!(out, "  last run: not recorded"),
        };

        match &self.comparison {
            None => {
                let _ = writeln!(out, "\nno --state manifest: nothing to compare, so no pruning number.");
            }
            Some(c) => self.render_comparison(&mut out, c, w),
        }

        if !self.model_level.is_empty() {
            let _ = writeln!(
                out,
                "\nmodel-level lineage ({}) — rebuilt whenever anything they read changes:",
                self.model_level.len()
            );
            for m in &self.model_level {
                let _ = writeln!(out, "    {:<w$}{}", m.model, m.reason);
            }
        }
        if !self.notes.is_empty() {
            let _ = writeln!(out, "\nnotes:");
            for n in &self.notes {
                let _ = writeln!(out, "  - {n}");
            }
        }
        out
    }

    fn render_comparison(&self, out: &mut String, c: &Comparison, w: usize) {
        let _ = writeln!(
            out,
            "\ncompared with the --state manifest ({} model(s) on compiled SQL, {} on raw SQL):",
            c.compared_on.compiled_sql, c.compared_on.raw_sql
        );
        if c.changed.is_empty() {
            let _ = writeln!(out, "  nothing changed.");
        } else {
            let _ = writeln!(out, "  changed ({}):", c.changed.len());
            for m in &c.changed {
                let cols = match &m.columns {
                    Some(cols) => format!(" — only [{}]", cols.join(", ")),
                    None => String::new(),
                };
                let _ = writeln!(out, "    {:<w$}{}{cols}", m.model, m.because.join(", "));
            }
        }
        if !c.removed.is_empty() {
            let _ = writeln!(out, "  removed ({}): {}", c.removed.len(), c.removed.join(", "));
        }
        let Some(dbt) = &c.vs_last_run else {
            let _ = writeln!(out, "\nFreshet would rebuild nothing.");
            return;
        };
        let _ = writeln!(out, "\nFreshet would rebuild {} model(s):", c.freshet.len());
        for r in &c.freshet {
            let why = match &r.reason {
                Reason::DirectlyChanged => "directly changed".to_string(),
                Reason::Downstream { because_of, via_columns } => {
                    format!("downstream of {because_of} via [{}]", via_columns.join(", "))
                }
            };
            let tag = if r.lineage == "model" { "  (model-level lineage)" } else { "" };
            let _ = writeln!(out, "    {:<w$}{why}{tag}", r.model);
        }
        let _ = writeln!(out);
        for b in [Some(dbt), c.vs_state_modified_plus.as_ref()].into_iter().flatten() {
            let saving = if b.avoided > 0 {
                format!("{} fewer — {:.1}% avoided", b.avoided, b.avoided_pct)
            } else if b.avoided == 0 {
                "no fewer".to_string()
            } else {
                format!("{} more", -b.avoided)
            };
            let _ = writeln!(out, "  vs {} ({} model(s)): {saving}", b.label, b.models);
            if !b.pruned.is_empty() {
                let _ = writeln!(out, "      it builds, Freshet does not: {}", b.pruned.join(", "));
            }
            if !b.freshet_extra.is_empty() {
                let _ = writeln!(out, "      Freshet builds, it did not: {}", b.freshet_extra.join(", "));
            }
        }
    }
}
