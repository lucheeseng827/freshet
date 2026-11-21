//! Plan quality across lineage densities: of the models a column edit could reach
//! through `depends_on`, how many Freshet's column lineage rebuilds, and how many it
//! must.
//!
//! Speed is measured elsewhere; this measures the answer. It needs no timer and no
//! quiet machine, so it is exact and runs as an ordinary test. Three counts per edit:
//!
//! * **closure** — the edited model and everything downstream of it: what a planner
//!   without column lineage rebuilds (dbt's `state:modified+`).
//! * **truth** — the models whose output actually changes, from the generator's own
//!   [`ModelWiring`]: a column changes when the column it selects changed, and every
//!   column of a join changes when a join key (`c0`) did. Nothing here reads the SQL,
//!   so the planner's lineage is checked against something it did not derive.
//! * **planned** — what [`freshet_core::plan_backfill`] rebuilds for the edit, over
//!   the project `freshet_sql::build_project` derived from the SQL.
//!
//! `planned` must contain `truth` — a model missed is a model left stale, the one
//! error the planner exists not to make — and every model it adds beyond `truth` is
//! rebuild work the lineage could not rule out. Freshet over-approximates on purpose
//! in one place: a column covers its source's row set through the source's first
//! column, so an edit to `c0` reaches every reader, even one that selects nothing
//! derived from it. The `key` rows show what that costs.
//!
//! Every generated model reading every column of one parent — the topology the speed
//! comparisons use — is the sweep's last `fan_in = 1` point: there, a column edit
//! reaches nearly the whole closure, and column lineage has little to prune.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};

use crate::export::downstream_closure;
use crate::{generate_wired, GenSpec, ModelWiring};

/// The topology every sweep point varies the density of: 1,024 models.
pub fn sweep_topology() -> GenSpec {
    GenSpec { layers: 16, width: 64, cols: 8, seed: 0x5151_5151, ..GenSpec::default() }
}

/// The `(fan_in, reads)` points swept: one, two and four parents, and from one column
/// of each up to all a model can select.
pub fn sweep_points() -> Vec<(usize, usize)> {
    let cols = sweep_topology().cols;
    [1, 2, 4]
        .into_iter()
        .flat_map(|fan_in| [1, 2, 4, 8].into_iter().filter(move |r| r * fan_in <= cols).map(move |r| (fan_in, r)))
        .collect()
}

/// How many layer-0 models each point edits, one at a time; the counts are summed.
pub const EDITS_PER_POINT: usize = 8;

/// Which column of an edited layer-0 model changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EditKind {
    /// `c0`: the column every join is keyed on, and the one Freshet's row-set rule
    /// carries into every reader.
    Key,
    /// The last column: a plain value, which decides no row anywhere.
    Value,
}

impl EditKind {
    fn column(self, cols: usize) -> usize {
        match self {
            EditKind::Key => 0,
            EditKind::Value => cols - 1,
        }
    }
}

/// One density point and one kind of edit, summed over [`EDITS_PER_POINT`] edits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepPoint {
    pub fan_in: usize,
    pub reads: usize,
    pub edit: EditKind,
    pub closure: usize,
    pub truth: usize,
    pub planned: usize,
    /// Models in `truth` the plan left out. Anything but zero is a wrong plan.
    pub missed: usize,
}

/// The committed sweep: `results/quality-sweep.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sweep {
    pub topology: GenSpec,
    pub edits_per_point: usize,
    pub points: Vec<SweepPoint>,
}

/// Run every point of the sweep.
pub fn sweep() -> Result<Sweep> {
    let base = sweep_topology();
    let mut points = Vec::new();
    for (fan_in, reads) in sweep_points() {
        let spec = GenSpec { fan_in, reads: Some(reads), ..base };
        points.extend(measure(&spec, EDITS_PER_POINT)?);
    }
    Ok(Sweep { topology: base, edits_per_point: EDITS_PER_POINT, points })
}

/// Both kinds of edit at one density, over the first `edits` layer-0 models.
pub fn measure(spec: &GenSpec, edits: usize) -> Result<Vec<SweepPoint>> {
    let (g, wiring) = generate_wired(spec);
    let project = freshet_sql::build_project(&g.models, &g.sources).context("the generated project does not build")?;
    let (fan_in, reads) = spec.density();
    let cols = spec.cols.max(1);
    let mut out = Vec::new();
    for edit in [EditKind::Key, EditKind::Value] {
        let column = edit.column(cols);
        let mut point = SweepPoint { fan_in, reads, edit, closure: 0, truth: 0, planned: 0, missed: 0 };
        for i in 0..edits.min(spec.width.max(1)) {
            let model = format!("m_0_{i}");
            let truth = truth(&wiring, &model, column);
            let changed = BTreeMap::from([(model.clone(), BTreeSet::from([format!("c{column}")]))]);
            let planned: BTreeSet<String> = freshet_core::plan_backfill(&project, &changed).models.into_iter().collect();
            let closure = downstream_closure(&project, &model);
            ensure!(planned.is_subset(&closure), "{model}: the plan reaches outside the model's own closure");
            point.closure += closure.len();
            point.truth += truth.len();
            point.planned += planned.len();
            point.missed += truth.difference(&planned).count();
        }
        out.push(point);
    }
    Ok(out)
}

/// The models whose output changes when `column` of `model` changes, from the
/// wiring alone: a value change reaches the columns that select it, and a change to a
/// join key changes which rows a join makes, so every column of that join.
pub fn truth(wiring: &[ModelWiring], model: &str, column: usize) -> BTreeSet<String> {
    let mut changed: BTreeMap<&str, BTreeSet<usize>> = BTreeMap::from([(model, BTreeSet::from([column]))]);
    // Generation order is topological: every parent comes before its readers.
    for w in wiring {
        if w.name == model || w.parents.is_empty() {
            continue;
        }
        let of = |p: usize| changed.get(w.parents[p].as_str());
        let rows_changed = w.parents.len() > 1 && (0..w.parents.len()).any(|p| of(p).is_some_and(|c| c.contains(&0)));
        let columns: BTreeSet<usize> = (0..w.columns.len())
            .filter(|&j| {
                let (p, c) = w.columns[j];
                rows_changed || of(p).is_some_and(|set| set.contains(&c))
            })
            .collect();
        if !columns.is_empty() {
            changed.insert(&w.name, columns);
        }
    }
    changed.into_keys().map(str::to_string).collect()
}

/// The sweep as Markdown, for BENCHMARKS.md: printed from the committed file by
/// code, never transcribed.
pub fn table(s: &Sweep) -> String {
    let t = &s.topology;
    let mut out = format!(
        "{} models ({} layers × {}, {} columns, seed {:#x}); each row sums {} edits, one layer-0 model at a time.\n\n",
        t.model_count(),
        t.layers,
        t.width,
        t.cols,
        t.seed,
        s.edits_per_point
    );
    out.push_str("| parents | columns read per parent | edited column | closure | changed (truth) | planned | planned / closure | truth / planned | missed |\n");
    out.push_str("|--:|--:|---|--:|--:|--:|--:|--:|--:|\n");
    for p in &s.points {
        let edit = match p.edit {
            EditKind::Key => "`c0` (key)",
            EditKind::Value => "last (value)",
        };
        out.push_str(&format!(
            "| {} | {} | {edit} | {} | {} | {} | {} | {} | {} |\n",
            p.fan_in,
            p.reads,
            p.closure,
            p.truth,
            p.planned,
            ratio(p.planned, p.closure),
            ratio(p.truth, p.planned),
            p.missed
        ));
    }
    out
}

fn ratio(a: usize, b: usize) -> String {
    if b == 0 {
        "—".into()
    } else {
        format!("{:.2}", a as f64 / b as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wired(name: &str, parents: &[&str], columns: &[(usize, usize)]) -> ModelWiring {
        ModelWiring {
            name: name.into(),
            parents: parents.iter().map(|p| p.to_string()).collect(),
            columns: columns.to_vec(),
        }
    }

    #[test]
    fn the_truth_follows_values_and_join_keys_and_nothing_else() {
        let wiring = [
            wired("a", &[], &[(0, 0), (0, 1), (0, 2)]),
            wired("x", &[], &[(0, 0), (0, 1), (0, 2)]),
            // Selects a.c2 only: a value edit to a.c1 does not reach it.
            wired("b", &["a"], &[(0, 2), (0, 2), (0, 2)]),
            // Selects a.c1.
            wired("c", &["a"], &[(0, 1), (0, 0), (0, 0)]),
            // Joins a and x on c0, selecting only x's columns: a key edit to `a`
            // changes its rows all the same.
            wired("d", &["a", "x"], &[(1, 1), (1, 2), (1, 0)]),
            // Reads c's c1 (a.c0): reached by a key edit, through c, by value.
            wired("e", &["c"], &[(0, 1), (0, 1), (0, 1)]),
        ];
        let names = |s: BTreeSet<String>| s.into_iter().collect::<Vec<_>>();
        assert_eq!(names(truth(&wiring, "a", 1)), ["a", "c"]);
        assert_eq!(names(truth(&wiring, "a", 2)), ["a", "b"]);
        assert_eq!(names(truth(&wiring, "a", 0)), ["a", "c", "d", "e"]);
    }

    /// Pins the committed `results/quality-sweep.json` to the code. A plan that
    /// misses a model fails as unsound; one that grew fails as a regression; one that
    /// shrank fails too, so the published numbers are regenerated rather than left to
    /// understate the planner.
    #[test]
    fn the_committed_quality_sweep_is_what_the_planner_does_today() {
        let fresh = sweep().unwrap();
        for p in &fresh.points {
            assert_eq!(
                p.missed, 0,
                "UNSOUND: {} parents, {} columns each, {:?} edit: the plan leaves out {} models the edit changes",
                p.fan_in, p.reads, p.edit, p.missed
            );
        }
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../results/quality-sweep.json");
        let regenerate = "regenerate it: cargo run -p freshet-bench --bin freshet-bench-quality > results/quality-sweep.json";
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}; {regenerate}", path.display()));
        let committed: Sweep = serde_json::from_str(&raw).unwrap();
        assert_eq!(committed.topology.model_count(), fresh.topology.model_count(), "{regenerate}");
        assert_eq!(committed.points.len(), fresh.points.len(), "the grid changed; {regenerate}");
        for (was, now) in committed.points.iter().zip(&fresh.points) {
            assert_eq!((was.fan_in, was.reads, was.edit), (now.fan_in, now.reads, now.edit), "{regenerate}");
            assert!(
                now.planned <= was.planned,
                "REGRESSION: {} parents, {} columns each, {:?} edit: {} models planned, {} before ({} needed)",
                now.fan_in, now.reads, now.edit, now.planned, was.planned, now.truth
            );
            assert_eq!(was, now, "the plan improved, or the generator changed; {regenerate}");
        }
    }
}
