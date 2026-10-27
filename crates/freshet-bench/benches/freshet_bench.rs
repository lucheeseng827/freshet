//! Criterion suite over the planner core's hot paths. `harness = false` (see Cargo.toml), so
//! this binary is built ONLY by `cargo bench`, never by `cargo build`.
//!
//! Run: `cargo bench -p freshet-bench`
//!
//! Measures, on a deterministic generated project:
//!   * parse + lineage  (freshet_sql::build_project)
//!   * fingerprint      (freshet_state::fingerprint_project)
//!   * changed_models   (the state diff)
//!   * plan_backfill    (single-column change -> minimal blast radius)
//!
//! Fixtures are built ONCE outside the timed region and black_box'd.

use std::collections::{BTreeMap, BTreeSet};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use freshet_api::{BackfillPlanner, MemoryStateBackend, StateBackend};
use freshet_bench::{generate, size_ladder};
use freshet_core::plan_backfill;
use freshet_sql::build_project;
use freshet_state::{changed_models, fingerprint_project, StateSnapshot};

fn bench_hot_paths(c: &mut Criterion) {
    for spec in size_ladder() {
        let g = generate(&spec);
        let n = spec.model_count();

        // parse + lineage
        c.bench_with_input(BenchmarkId::new("build_project", n), &g, |b, g| {
            b.iter(|| build_project(black_box(&g.models), black_box(&g.sources)).unwrap());
        });

        // Pre-build the project for the state/plan benches.
        let project = build_project(&g.models, &g.sources).unwrap();

        c.bench_with_input(BenchmarkId::new("fingerprint_project", n), &project, |b, p| {
            b.iter(|| fingerprint_project(black_box(p)));
        });

        let fps = fingerprint_project(&project);
        let prev = StateSnapshot { fingerprints: fps.clone(), watermarks: BTreeMap::new() };
        c.bench_with_input(BenchmarkId::new("changed_models", n), &(&prev, &fps), |b, (prev, fps)| {
            b.iter(|| changed_models(black_box(prev), black_box(fps)));
        });

        // Single-column change on one layer-0 model -> the column-pruned blast radius.
        let mut changed: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        changed.insert("m_0_0".to_string(), BTreeSet::from(["c0".to_string()]));
        c.bench_with_input(BenchmarkId::new("plan_backfill_1col", n), &(&project, &changed), |b, (p, ch)| {
            b.iter(|| plan_backfill(black_box(p), black_box(ch)));
        });
    }
}

/// The **end-to-end warm replan** — `BackfillPlanner::plan()` against a project that
/// is already parsed.
///
/// This exists because BENCHMARKS.md previously reported a warm replan as the *sum*
/// of the three separately-timed stages above. That sum is not a measurement: it
/// double-counts nothing but it also proves nothing about the call a daemon actually
/// makes, and this page's whole premise is that nothing on it is extrapolated. So the
/// call is timed directly.
///
/// The project and the seeded backend are built once, outside the timed region; only
/// `plan()` is measured. That is exactly `freshet-serve`'s shape: a warm
/// `Arc<Project>` held across requests, re-fingerprinted on every plan.
///
/// Two cases, because they are the two a CI job hits:
///
/// * **nochange** — the snapshot matches the project, so the plan is empty. This is
///   what `freshet plan` costs on the overwhelming majority of commits, and the one
///   that must stay cheap enough to run on every push.
/// * **1model** — one layer-0 model's fingerprint differs, so that model and its
///   column-aware downstream rebuild. Note this is coarser than the
///   `plan_backfill_1col` row above: `plan()` diffs whole-model fingerprints, so it
///   cannot express "only column `c0` changed" — it marks the model changed and uses
///   all of its output columns. The two rows are therefore not comparable, and the
///   lower-level one is the optimistic bound.
fn bench_warm_replan(c: &mut Criterion) {
    for spec in size_ladder() {
        let g = generate(&spec);
        let n = spec.model_count();
        let project = build_project(&g.models, &g.sources).unwrap();
        let fps = fingerprint_project(&project);

        // Committed state identical to the project: nothing to rebuild.
        let clean = MemoryStateBackend::default();
        clean
            .save(&StateSnapshot { fingerprints: fps.clone(), watermarks: BTreeMap::new() })
            .unwrap();
        let planner_clean = BackfillPlanner::new(project.clone(), clean);
        c.bench_with_input(BenchmarkId::new("plan_warm_nochange", n), &planner_clean, |b, p| {
            b.iter(|| black_box(p).plan().unwrap());
        });

        // One model edited, as `plan()` sees it: its stored fingerprint no longer
        // matches. Perturbed rather than removed — a missing entry is the "never
        // built" case, which is a different code path.
        let mut edited = fps.clone();
        if let Some(v) = edited.get_mut("m_0_0") {
            *v ^= 0x9E37_79B9_7F4A_7C15;
        }
        let dirty = MemoryStateBackend::default();
        dirty
            .save(&StateSnapshot { fingerprints: edited, watermarks: BTreeMap::new() })
            .unwrap();
        let planner_dirty = BackfillPlanner::new(project, dirty);
        c.bench_with_input(BenchmarkId::new("plan_warm_1model", n), &planner_dirty, |b, p| {
            b.iter(|| black_box(p).plan().unwrap());
        });
    }
}

criterion_group!(benches, bench_hot_paths, bench_warm_replan);
criterion_main!(benches);
