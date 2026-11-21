//! freshet-bench — synthetic project generators + a reproducible benchmark manifest.
//!
//! Substantiates the problem statement's perf **Signal** (dbt-core's super-linear
//! parse; SQLMesh's 9×–136× state-op claims; SDF/Fusion's "10k models in <600ms")
//! with numbers we actually *measure*. The library half — deterministic generators +
//! the [`BenchManifest`] — builds under `cargo build`; the criterion suite lives in
//! `benches/` (`harness = false`) so **criterion never enters the default/release
//! dependency graph**.
//!
//! ## Method (kept honest)
//!
//! * Fixtures are generated **once, outside** the timed region and `black_box`'d.
//! * The generator is **deterministic** (seeded) so a run reproduces.
//! * dbt/SQLMesh comparison numbers are produced **out-of-band** on the *same* exported
//!   topology and recorded in the manifest, labeled — never presented as produced by
//!   this crate, never reusing anyone's extrapolated multipliers.
//!
//! ## The out-of-band comparison
//!
//! [`export`] writes one [`GeneratedProject`] as a dbt project, a SQLMesh project and
//! a `freshet` project dir; `scripts/compare.sh` times each tool's own command over
//! them as whole processes, and `freshet-bench-manifest` folds the raw repetitions
//! into [`BenchManifest`]s. Freshet's CLI is timed by the same harness as the others,
//! so its row there is a process wall time like theirs — comparable to them, and *not*
//! comparable to this crate's criterion rows, which time library calls in-process.

pub mod export;
pub mod gate;
pub mod quality;

use std::collections::BTreeMap;

use freshet_sql::Catalog;
use serde::{Deserialize, Serialize};

/// Knobs for a synthetic layered-DAG project.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenSpec {
    /// Number of dependency layers (depth). Layer 0 reads the source table.
    pub layers: usize,
    /// Models per layer (width).
    pub width: usize,
    /// Output columns per model (drives lineage density / fingerprint cost).
    pub cols: usize,
    /// Deterministic seed (which parent each model picks).
    pub seed: u64,
    /// How many models of the previous layer each later model joins, on `c0`. One,
    /// the default, is a plain `select … from parent`.
    #[serde(default = "one", skip_serializing_if = "is_one")]
    pub fan_in: usize,
    /// How many of each parent's columns a model selects, at most `cols / fan_in`;
    /// `None`, the default, is all of them. Below that, column lineage has something
    /// to prune: a change to a column no reader selects reaches no reader.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reads: Option<usize>,
}

fn one() -> usize {
    1
}

fn is_one(n: &usize) -> bool {
    *n == 1
}

impl Default for GenSpec {
    fn default() -> Self {
        Self { layers: 8, width: 16, cols: 8, seed: 0x5151_5151, fan_in: 1, reads: None }
    }
}

impl GenSpec {
    /// Total model count this spec produces. Mirrors `generate()`'s `.max(1)`
    /// clamping so the capacity hint / size labels stay honest for zero-sized specs.
    pub fn model_count(&self) -> usize {
        self.layers.max(1) * self.width.max(1)
    }

    /// `fan_in` and `reads` as generated: at least one parent and no more than a
    /// layer has, and at least one column per parent and no more than `cols` split
    /// between them.
    pub fn density(&self) -> (usize, usize) {
        let cols = self.cols.max(1);
        let fan_in = self.fan_in.clamp(1, self.width.max(1));
        let reads = self.reads.unwrap_or(cols).clamp(1, (cols / fan_in).max(1));
        (fan_in, reads)
    }

    /// Every model reads every column of one parent: the topology every committed
    /// result before the density sweep was measured on.
    fn reads_everything(&self) -> bool {
        self.density() == (1, self.cols.max(1))
    }
}

/// A generated project: `(name, sql)` model definitions plus the seed source catalog.
#[derive(Clone, Debug)]
pub struct GeneratedProject {
    pub models: Vec<(String, String)>,
    pub sources: Catalog,
}

/// Generate a deterministic layered DAG. Layer 0 models select from a single `src`
/// table; each later model selects its columns from one parent in the previous layer
/// (chosen by the seeded PRNG), yielding real `depends_on` + per-column lineage.
///
/// With `fan_in` or `reads` set, each later model instead joins `fan_in` distinct
/// parents on `c0` and selects `reads` columns of each, output `c{j}` being the
/// `j`-th of them round-robin.
pub fn generate(spec: &GenSpec) -> GeneratedProject {
    generate_wired(spec).0
}

/// What each generated model reads, as the generator chose it: the ground truth
/// [`quality`] checks the planner's lineage against, written down rather than parsed
/// back out of the SQL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelWiring {
    pub name: String,
    /// The models read, in join order; empty for layer 0, which reads `src`. More
    /// than one means they are joined on `c0`.
    pub parents: Vec<String>,
    /// For each output column `c{j}`, `(index into parents, the parent's column)`.
    /// Layer 0's `c{j}` is `src.c{j}`, recorded as `(0, j)`.
    pub columns: Vec<(usize, usize)>,
}

/// [`generate`], with each model's [`ModelWiring`], in generation (topological) order.
pub fn generate_wired(spec: &GenSpec) -> (GeneratedProject, Vec<ModelWiring>) {
    let cols = spec.cols.max(1);
    let width = spec.width.max(1);
    let col_list = (0..cols).map(|c| format!("c{c}")).collect::<Vec<_>>().join(", ");
    let (fan_in, reads) = spec.density();

    let mut sources = Catalog::new();
    sources.insert("src".to_string(), (0..cols).map(|c| format!("c{c}")).collect());

    let mut rng = Lcg::new(spec.seed);
    let mut models = Vec::with_capacity(spec.model_count());
    let mut wiring = Vec::with_capacity(spec.model_count());
    for layer in 0..spec.layers.max(1) {
        for i in 0..width {
            let name = format!("m_{layer}_{i}");
            let (sql, parents, columns) = if layer == 0 {
                (format!("select {col_list} from src"), Vec::new(), (0..cols).map(|j| (0, j)).collect())
            } else if spec.reads_everything() {
                // Exactly the draws and the SQL of every result committed before the
                // density knobs existed, so their topologies are unchanged.
                let parent = format!("m_{}_{}", layer - 1, rng.next_below(width));
                (format!("select {col_list} from {parent}"), vec![parent], (0..cols).map(|j| (0, j)).collect())
            } else {
                let parents: Vec<String> = distinct(&mut rng, fan_in, width)
                    .into_iter()
                    .map(|p| format!("m_{}_{p}", layer - 1))
                    .collect();
                let chosen: Vec<Vec<usize>> = parents.iter().map(|_| distinct(&mut rng, reads, cols)).collect();
                let inputs: Vec<(usize, usize)> =
                    (0..reads).flat_map(|r| chosen.iter().enumerate().map(move |(p, c)| (p, c[r]))).collect();
                let columns: Vec<(usize, usize)> = (0..cols).map(|j| inputs[j % inputs.len()]).collect();
                (joined_select(&parents, &columns), parents, columns)
            };
            models.push((name.clone(), sql));
            wiring.push(ModelWiring { name, parents, columns });
        }
    }
    (GeneratedProject { models, sources }, wiring)
}

/// `k` distinct numbers below `n`, in the order drawn (a partial Fisher–Yates).
fn distinct(rng: &mut Lcg, k: usize, n: usize) -> Vec<usize> {
    let mut pool: Vec<usize> = (0..n).collect();
    for i in 0..k.min(n) {
        let j = i + rng.next_below(n - i);
        pool.swap(i, j);
    }
    pool.truncate(k.min(n));
    pool
}

/// `select … as c{j}` over one parent, or over several joined on `c0`.
fn joined_select(parents: &[String], columns: &[(usize, usize)]) -> String {
    let one = parents.len() == 1;
    let items: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(j, (p, c))| if one { format!("c{c} as c{j}") } else { format!("p{p}.c{c} as c{j}") })
        .collect();
    if one {
        return format!("select {} from {}", items.join(", "), parents[0]);
    }
    let mut from = format!("{} as p0", parents[0]);
    for (p, parent) in parents.iter().enumerate().skip(1) {
        from.push_str(&format!(" join {parent} as p{p} on p0.c0 = p{p}.c0"));
    }
    format!("select {} from {from}", items.join(", "))
}

/// Reproducibility envelope for a benchmark run. Timings are filled by the harness /
/// CI; this crate only fixes what was measured and on what.
///
/// Every field added after the first four is `#[serde(default)]`, so a manifest
/// written before they existed still reads.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchManifest {
    pub spec: GenSpec,
    pub provenance: Provenance,
    /// Out-of-band process wall times, labeled — dbt's and SQLMesh's, and Freshet's
    /// own CLI when the same harness times it. One list for all tools because what
    /// makes two rows comparable is that one method produced both.
    #[serde(default)]
    pub external: Vec<ExternalDatum>,
    /// The one-model edit that the "edit" operations were timed against, and the
    /// answer every tool should reach for it. `None` when the run timed no edit.
    #[serde(default)]
    pub edit: Option<EditSpec>,
    /// Operations the harness meant to time and did not, with the reason. Recorded
    /// rather than dropped: a table that silently omits a tool reads as if the tool
    /// had been measured and had nothing to report.
    #[serde(default)]
    pub skipped: Vec<SkippedDatum>,
}

/// Environment provenance (commit/rustc/cpu/criterion version) — stamped by CI, not by
/// this dependency-light crate (no clock/rng access here).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Provenance {
    pub commit: String,
    pub rustc: String,
    pub cpu: String,
    /// criterion's version for a criterion run; empty for a run that timed whole
    /// processes (`scripts/compare.sh`), which uses no criterion at all.
    pub criterion: String,
    /// Logical cores visible to the run. Recorded because some of the timed tools
    /// parallelize and some do not: a wall time means nothing without the core count
    /// it was allowed to spread across.
    #[serde(default)]
    pub cores: usize,
    /// `uname -srm`-style kernel/arch string.
    #[serde(default)]
    pub os: String,
    /// Whether the worktree had uncommitted changes when the run started. A `true`
    /// here means `commit` does not fully describe the code that was measured.
    #[serde(default)]
    pub dirty: bool,
    /// How the measured Rust binaries were built (cargo profile and its settings).
    #[serde(default)]
    pub build: String,
    /// Version of every external tool, interpreter and library the run invoked.
    #[serde(default)]
    pub tools: BTreeMap<String, String>,
}

/// One externally-measured comparison point, explicitly out-of-band.
///
/// `wall_ms` is the **median** of `samples_ms` and `mad_ms` its median absolute
/// deviation; both are derived, and [`ExternalDatum::is_consistent`] rechecks them
/// against the raw samples, so a hand edit to a published number cannot drift from
/// the repetitions it claims to summarize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExternalDatum {
    pub tool: String,       // "dbt-core 1.8" | "sqlmesh 0.x"
    pub op: String,         // "parse" | "state-diff" | ...
    pub wall_ms: f64,
    /// What the command does, what it does not, and what it is (not) comparable to.
    pub note: String,       // "different machine; see results/specs.json"
    /// The exact command timed, as run from the project directory.
    #[serde(default)]
    pub command: String,
    /// The untimed step run before every repetition to establish its precondition
    /// (e.g. deleting a cache for a "cold" row). Empty when there is none.
    #[serde(default)]
    pub prepare: String,
    /// Every timed repetition's wall time, in run order. Warm-up runs are excluded
    /// here and kept in `warmup_ms`.
    #[serde(default)]
    pub samples_ms: Vec<f64>,
    /// User + system CPU time of each repetition, children included, index-aligned
    /// with `samples_ms`. CPU above wall means the tool used more than one core.
    #[serde(default)]
    pub cpu_ms: Vec<f64>,
    /// Untimed-for-the-median warm-up runs (OS page cache, bytecode caches), kept so
    /// nothing that ran is invisible.
    #[serde(default)]
    pub warmup_ms: Vec<f64>,
    /// CPU time everything *else* on the machine used during each repetition
    /// (system-wide busy time minus the repetition's own CPU), index-aligned with
    /// `samples_ms`; empty when the host could not report it. Recorded because a
    /// shared machine's other tenants compete for the same cores: near zero means the
    /// repetition had the machine to itself, and a large value is a reason to distrust
    /// that repetition rather than something to average away. Tick-granular, so it
    /// can dip slightly below zero.
    #[serde(default)]
    pub other_cpu_ms: Vec<f64>,
    /// Hypervisor steal time during each repetition, index-aligned with `samples_ms`;
    /// empty when not measured.
    #[serde(default)]
    pub steal_ms: Vec<f64>,
    /// Median absolute deviation of `samples_ms`.
    #[serde(default)]
    pub mad_ms: f64,
    /// What the command reported (plan size, model count, its own internal timer), so
    /// a reader can check that the tools answered the same question the same way.
    #[serde(default)]
    pub observed: String,
}

impl ExternalDatum {
    /// Set `wall_ms` and `mad_ms` from `samples_ms`.
    pub fn summarize(mut self) -> Self {
        self.wall_ms = median(&self.samples_ms);
        self.mad_ms = median_abs_deviation(&self.samples_ms);
        self
    }

    /// Whether the derived fields are exactly what the raw samples give, and every
    /// per-repetition series lines up with the samples it annotates.
    pub fn is_consistent(&self) -> bool {
        let n = self.samples_ms.len();
        let aligned_or_absent = |v: &[f64]| v.is_empty() || v.len() == n;
        n > 0
            && self.cpu_ms.len() == n
            && aligned_or_absent(&self.other_cpu_ms)
            && aligned_or_absent(&self.steal_ms)
            && self.wall_ms == median(&self.samples_ms)
            && self.mad_ms == median_abs_deviation(&self.samples_ms)
    }
}

/// An operation the harness meant to time and could not (tool not installed, a
/// precondition that failed), with the reason.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SkippedDatum {
    pub tool: String,
    pub op: String,
    pub reason: String,
}

/// The one-model edit a comparison times, and its expected answer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EditSpec {
    /// The model given [`export::edit_model`]'s semantic edit.
    pub model: String,
    /// `model` plus its transitive dependents ([`export::downstream_closure`]): how
    /// many models a correct plan for this edit rebuilds, in every tool.
    pub closure: usize,
}

/// Median; the mean of the middle two for an even count. `NaN` for no samples, which
/// [`ExternalDatum::is_consistent`] rejects.
pub fn median(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    let mut sorted = xs.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    }
}

/// Median absolute deviation from the median — the spread statistic §M5 asks for,
/// because unlike a standard deviation one slow outlier repetition cannot inflate it.
pub fn median_abs_deviation(xs: &[f64]) -> f64 {
    let m = median(xs);
    median(&xs.iter().map(|x| (x - m).abs()).collect::<Vec<_>>())
}

/// A small, fast, deterministic PRNG (numerical-recipes LCG). No `rand` dep, no global
/// state — pure determinism for reproducible fixtures.
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed ^ 0x9E37_79B9_7F4A_7C15 }
    }
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.state
    }
    fn next_below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Convenience: bucketed size ladder for sweeps.
///
/// Rungs are 128 / 512 / 2048 / 5120 / **10240** models.
///
/// The top rung exists because the ladder previously stopped at 5120 while
/// the exit target is "**10k models < 600ms parse**".
/// A target above the largest rung cannot pass or fail — it can only be
/// extrapolated to, which is exactly what that milestone's own honesty rules forbid
/// ("published multipliers are measured ratios, never round assertions"). 10240
/// makes the 10k target a measurement.
///
/// The docstring also used to claim "roughly 1e2 .. 1e5". It was 1e2..5e3; the 1e5
/// end was never generated. The range is now stated as the rungs themselves rather
/// than as an order-of-magnitude claim, because that is the form that cannot drift
/// from what the function returns. A 1e5 rung is deliberately *not* added here: at
/// ~50 µs/model for `build_project` a single iteration is near 5 s, which belongs in
/// the weekly job, not in a suite anyone runs before pushing. It is timed there as
/// whole processes (`scripts/compare.sh --sizes 80x128,625x160`) and held to
/// [`gate::Rules::PUBLISHED`].
pub fn size_ladder() -> Vec<GenSpec> {
    [(8, 16), (16, 32), (32, 64), (40, 128), (80, 128)]
        .into_iter()
        .map(|(layers, width)| GenSpec { layers, width, cols: 8, seed: 0x5151_5151, ..GenSpec::default() })
        .collect()
}

/// Build the `BenchManifest`'s default empty external set (CI fills it).
pub fn empty_externals() -> BTreeMap<String, Vec<ExternalDatum>> {
    BTreeMap::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_projects_are_acyclic_and_parseable() {
        for spec in size_ladder().into_iter().take(2) {
            let g = generate(&spec);
            let project = freshet_sql::build_project(&g.models, &g.sources).expect("generated SQL parses");
            assert_eq!(project.len(), spec.model_count());
            assert!(project.topo_order().is_ok(), "generator must never emit a cycle");
        }
    }

    #[test]
    fn generation_is_deterministic() {
        let spec = GenSpec::default();
        assert_eq!(generate(&spec).models, generate(&spec).models);
        let sparse = GenSpec { fan_in: 3, reads: Some(2), ..spec };
        assert_eq!(generate(&sparse).models, generate(&sparse).models);
    }

    /// The committed results were measured on the topology the generator made before
    /// `fan_in` and `reads` existed. Pinned by its SQL, not by a rerun of itself.
    #[test]
    fn the_default_topology_is_the_one_the_committed_results_measured() {
        let g = generate(&GenSpec { layers: 3, width: 4, cols: 2, seed: 0x5151_5151, ..GenSpec::default() });
        let sql: Vec<&str> = g.models.iter().map(|(_, s)| s.as_str()).collect();
        assert_eq!(&sql[..4], ["select c0, c1 from src"; 4]);
        assert!(sql[4..].iter().all(|s| s.starts_with("select c0, c1 from m_")), "{sql:?}");
        let same = generate(&GenSpec { layers: 3, width: 4, cols: 2, seed: 0x5151_5151, reads: Some(2), ..GenSpec::default() });
        assert_eq!(g.models, same.models, "reading every column is the default topology");
        // The manifest JSON of a default spec is what it was, too.
        let json = serde_json::to_string(&GenSpec::default()).unwrap();
        assert_eq!(json, r#"{"layers":8,"width":16,"cols":8,"seed":1364283729}"#);
        let old: GenSpec = serde_json::from_str(&json).unwrap();
        assert_eq!((old.fan_in, old.reads), (1, None));
    }

    #[test]
    fn a_sparse_model_joins_its_parents_and_reads_what_its_wiring_says() {
        let spec = GenSpec { layers: 2, width: 4, cols: 4, seed: 9, fan_in: 2, reads: Some(1) };
        let (g, wiring) = generate_wired(&spec);
        let (name, sql) = &g.models[4];
        let w = &wiring[4];
        assert_eq!(&w.name, name);
        assert_eq!(w.parents.len(), 2);
        assert_ne!(w.parents[0], w.parents[1], "distinct parents");
        // reads = 1 per parent, round-robin over the two: c0, c2 from p0; c1, c3 from p1.
        assert_eq!(w.columns[0].0, 0);
        assert_eq!(w.columns[1].0, 1);
        assert_eq!(w.columns[0], w.columns[2]);
        let expected_from = format!("from {} as p0 join {} as p1 on p0.c0 = p1.c0", w.parents[0], w.parents[1]);
        assert!(sql.ends_with(&expected_from), "{sql}");
        assert!(sql.starts_with(&format!("select p0.c{} as c0, p1.c{} as c1", w.columns[0].1, w.columns[1].1)), "{sql}");
        let project = freshet_sql::build_project(&g.models, &g.sources).unwrap();
        let mut deps = project.get(name).unwrap().depends_on.clone();
        deps.sort();
        let mut parents = w.parents.clone();
        parents.sort();
        assert_eq!(deps, parents);
    }

    #[test]
    fn density_is_clamped_to_what_a_model_can_read() {
        let spec = |fan_in, reads| GenSpec { layers: 2, width: 3, cols: 8, seed: 1, fan_in, reads };
        assert_eq!(spec(1, None).density(), (1, 8));
        assert_eq!(spec(0, Some(0)).density(), (1, 1));
        assert_eq!(spec(9, None).density(), (3, 2), "no more parents than a layer has");
        assert_eq!(spec(2, Some(8)).density(), (2, 4), "cols split between the parents");
    }

    #[test]
    fn median_and_mad_follow_their_definitions() {
        assert_eq!(median(&[5.0, 1.0, 3.0]), 3.0);
        assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]), 2.5, "even count: mean of the middle two");
        assert!(median(&[]).is_nan());
        // |x - 3| = [2, 0, 0, 1, 97] -> median 1. One outlier does not move it.
        assert_eq!(median_abs_deviation(&[1.0, 3.0, 3.0, 4.0, 100.0]), 1.0);
    }

    #[test]
    fn a_datum_whose_median_was_edited_by_hand_is_inconsistent() {
        let d = ExternalDatum {
            tool: "t".into(),
            op: "o".into(),
            wall_ms: 0.0,
            note: "n".into(),
            command: "c".into(),
            prepare: String::new(),
            samples_ms: vec![10.0, 12.0, 11.0],
            cpu_ms: vec![9.0, 11.0, 10.0],
            warmup_ms: vec![30.0],
            other_cpu_ms: vec![0.0, 40.0, -10.0],
            steal_ms: Vec::new(),
            mad_ms: 0.0,
            observed: String::new(),
        }
        .summarize();
        assert_eq!((d.wall_ms, d.mad_ms), (11.0, 1.0));
        assert!(d.is_consistent(), "an unmeasured series (empty) is allowed");
        let mut rounded = d.clone();
        rounded.wall_ms = 10.0;
        assert!(!rounded.is_consistent());
        let mut unpaired = d.clone();
        unpaired.cpu_ms.pop();
        assert!(!unpaired.is_consistent(), "every repetition needs its CPU time");
        let mut short = d;
        short.other_cpu_ms.pop();
        assert!(!short.is_consistent(), "a measured series must cover every repetition");
    }

    #[test]
    fn a_manifest_from_before_the_comparison_fields_still_reads() {
        let old = r#"{"spec":{"layers":1,"width":1,"cols":1,"seed":0},
            "provenance":{"commit":"c","rustc":"r","cpu":"x","criterion":"0.5"},
            "external":[{"tool":"t","op":"o","wall_ms":1.0,"note":"n"}]}"#;
        let m: BenchManifest = serde_json::from_str(old).unwrap();
        assert!(m.skipped.is_empty() && m.edit.is_none() && m.external[0].samples_ms.is_empty());
    }

    /// Every committed `results/compare-*.json` must deserialize as the manifest
    /// schema and carry medians that are exactly what its raw repetitions give.
    ///
    /// This is the guard against the drift BENCHMARKS.md already paid for once (a
    /// table hand-transcribed from criterion output): a published number that no
    /// longer matches its samples fails the build instead of shipping.
    #[test]
    fn committed_comparison_results_are_self_consistent() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../results");
        let Ok(entries) = std::fs::read_dir(&dir) else { return };
        for entry in entries {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if !(name.starts_with("compare-") && name.ends_with(".json")) {
                continue;
            }
            let runs: Vec<BenchManifest> = serde_json::from_slice(&std::fs::read(&path).unwrap())
                .unwrap_or_else(|e| panic!("{name}: not a list of BenchManifest: {e}"));
            assert!(!runs.is_empty(), "{name}: no runs");
            for run in &runs {
                let p = &run.provenance;
                assert!(!p.commit.is_empty() && !p.cpu.is_empty() && p.cores > 0, "{name}: provenance incomplete");
                for d in &run.external {
                    assert!(d.is_consistent(), "{name}: {} / {}: median/MAD disagree with the samples", d.tool, d.op);
                    assert!(!d.command.is_empty() && !d.note.is_empty(), "{name}: {} / {}: unlabeled", d.tool, d.op);
                }
            }
        }
    }
}
