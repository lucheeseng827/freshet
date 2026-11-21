//! The weekly gate: what a `scripts/compare.sh --only freshet` run must show.
//!
//! Three rules, each read from the run's own [`BenchManifest`]s, so the gate judges
//! exactly what is published and needs no stored number from another machine:
//!
//! * **budget** — Freshet's CLI plans a one-model edit at the largest rung, which must
//!   have at least [`Rules::budget_models`] models, within [`Rules::budget_ms`]. This
//!   is the published budget, and the first `freshet plan` a CI job runs.
//! * **scaling** — for every Freshet operation timed at both, the time per model at
//!   the largest rung is at most [`Rules::max_scaling`] times that at the smallest.
//!   Both rungs ran in one job on one runner, so the runner's speed cancels out of the
//!   ratio, and what is left is how the work grows with the project: a regression to
//!   super-linear parsing or planning fails here on any machine.
//! * **answers** — no operation's answer was marked a mismatch: a plan that rebuilds
//!   the wrong number of models is not a measurement of anything.
//!
//! The plan's *quality* — which models it rebuilds, against the truth — is not timed
//! and needs no scheduled runner: [`crate::quality`] checks it on every build.

use std::fmt;

use anyhow::{ensure, Result};

use crate::BenchManifest;

/// The rules a run is held to.
#[derive(Clone, Debug, PartialEq)]
pub struct Rules {
    pub budget_ms: f64,
    pub budget_models: usize,
    pub max_scaling: f64,
}

impl Rules {
    /// The published budget: 100,000 models planned in 10 s, and no operation more
    /// than 1.5× as expensive per model at the largest rung as at the smallest.
    pub const PUBLISHED: Rules = Rules { budget_ms: 10_000.0, budget_models: 100_000, max_scaling: 1.5 };
}

/// One rule applied to one thing, and whether it held.
#[derive(Clone, Debug, PartialEq)]
pub struct Finding {
    pub ok: bool,
    pub what: String,
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}  {}", if self.ok { "ok  " } else { "FAIL" }, self.what)
    }
}

/// The CLI planning a one-model edit against committed state: what the budget is for.
fn is_edit_plan(tool: &str, op: &str) -> bool {
    is_cli(tool) && op.starts_with("DIFF: plan vs committed state,") && op.contains(" edited ")
}

/// Freshet's own CLI, not its daemon (timed through curl) and not another tool.
fn is_cli(tool: &str) -> bool {
    tool.starts_with("freshet ") && !tool.contains("daemon")
}

fn is_freshet(tool: &str) -> bool {
    tool.starts_with("freshet ")
}

/// Apply `rules` to the runs of one `compare.sh` manifest, one finding per check.
/// An error means the runs cannot be judged at all (too few rungs, no budget rung,
/// no edit plan timed), which the gate treats as a failure.
pub fn check(runs: &[BenchManifest], rules: &Rules) -> Result<Vec<Finding>> {
    ensure!(runs.len() >= 2, "the scaling rule needs at least two rungs, got {}", runs.len());
    let size = |r: &BenchManifest| r.spec.model_count();
    let small = runs.iter().min_by_key(|r| size(r)).expect("non-empty");
    let large = runs.iter().max_by_key(|r| size(r)).expect("non-empty");
    let (n_small, n_large) = (size(small), size(large));
    ensure!(n_large > n_small, "every rung has {n_large} models; scaling needs two sizes");
    ensure!(
        n_large >= rules.budget_models,
        "the budget is for {} models, and the largest rung measured has {n_large}",
        rules.budget_models
    );

    let mut findings = Vec::new();
    let edit = large
        .external
        .iter()
        .find(|d| is_edit_plan(&d.tool, &d.op))
        .ok_or_else(|| anyhow::anyhow!("no `freshet plan` of an edit was timed at {n_large} models"))?;
    findings.push(Finding {
        ok: edit.wall_ms <= rules.budget_ms,
        what: format!(
            "budget: `{}` at {n_large} models took {:.0} ms (median of {}, MAD {:.0} ms); budget {:.0} ms",
            edit.command,
            edit.wall_ms,
            edit.samples_ms.len(),
            edit.mad_ms,
            rules.budget_ms
        ),
    });

    for d in large.external.iter().filter(|d| is_freshet(&d.tool)) {
        // The edit's model is in the op's label (`m_0_0 edited`), the same at both rungs.
        let Some(s) = small.external.iter().find(|s| s.tool == d.tool && s.op == d.op) else {
            continue;
        };
        let per_large = d.wall_ms / n_large as f64;
        let per_small = s.wall_ms / n_small as f64;
        let scaling = per_large / per_small;
        findings.push(Finding {
            ok: scaling <= rules.max_scaling,
            what: format!(
                "scaling: {} — {:.1} µs/model at {n_large}, {:.1} µs/model at {n_small}: {scaling:.2}× (at most {:.2}×)",
                d.op,
                per_large * 1000.0,
                per_small * 1000.0,
                rules.max_scaling
            ),
        });
    }

    for run in runs {
        for d in run.external.iter().filter(|d| d.observed.starts_with("MISMATCH")) {
            findings.push(Finding {
                ok: false,
                what: format!("answer: {} / {} at {} models: {}", d.tool, d.op, size(run), d.observed),
            });
        }
    }
    let mismatches = findings.iter().filter(|f| f.what.starts_with("answer:")).count();
    if mismatches == 0 {
        findings.push(Finding { ok: true, what: "answers: every operation reported the answer it should".into() });
    }
    Ok(findings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExternalDatum, GenSpec, Provenance};

    fn datum(tool: &str, op: &str, wall_ms: f64, observed: &str) -> ExternalDatum {
        ExternalDatum {
            tool: tool.into(),
            op: op.into(),
            wall_ms,
            note: "n".into(),
            command: "freshet plan --project . --state ../../freshet-state.json".into(),
            prepare: String::new(),
            samples_ms: vec![wall_ms; 3],
            cpu_ms: vec![wall_ms; 3],
            warmup_ms: Vec::new(),
            other_cpu_ms: Vec::new(),
            steal_ms: Vec::new(),
            mad_ms: 0.0,
            observed: observed.into(),
        }
    }

    const EDIT: &str = "DIFF: plan vs committed state, m_0_0 edited (k)";
    const LOAD: &str = "LOAD: graph (parse + column lineage + topo order)";

    fn run(layers: usize, width: usize, edit_ms: f64, load_ms: f64) -> BenchManifest {
        BenchManifest {
            spec: GenSpec { layers, width, ..GenSpec::default() },
            provenance: Provenance::default(),
            external: vec![
                datum("freshet 0.1.0", LOAD, load_ms, "n models in topological order"),
                datum("freshet 0.1.0", EDIT, edit_ms, "80 models in plan"),
                datum("freshet 0.1.0 (freshet-serve daemon)", "WARM: daemon replan, nothing changed (0)", load_ms / 2.0, "0"),
            ],
            edit: None,
            skipped: Vec::new(),
        }
    }

    fn failed(findings: &[Finding]) -> Vec<String> {
        findings.iter().filter(|f| !f.ok).map(|f| f.what.clone()).collect()
    }

    #[test]
    fn a_run_within_budget_and_linear_passes() {
        // 10,240 and 100,000 models, ~80 µs/model at both.
        let runs = [run(80, 128, 820.0, 560.0), run(625, 160, 8_100.0, 5_600.0)];
        let findings = check(&runs, &Rules::PUBLISHED).unwrap();
        assert!(failed(&findings).is_empty(), "{findings:#?}");
        // budget + three scaling rows + the answers row.
        assert_eq!(findings.len(), 5, "{findings:#?}");
        assert!(findings[0].what.starts_with("budget: `freshet plan"), "{}", findings[0]);
    }

    #[test]
    fn over_budget_fails_the_budget_rule_alone() {
        let runs = [run(80, 128, 1_100.0, 560.0), run(625, 160, 10_500.0, 5_600.0)];
        let failures = failed(&check(&runs, &Rules::PUBLISHED).unwrap()).join("\n");
        assert!(failures.starts_with("budget:") && !failures.contains("scaling:"), "{failures}");
    }

    #[test]
    fn super_linear_growth_fails_the_scaling_rule_on_a_fast_machine_too() {
        // Well inside the budget, but the load (and the daemon's replan, half of it
        // in this fixture) costs 2× per model at the top rung; the edit plan does not.
        let runs = [run(80, 128, 300.0, 200.0), run(625, 160, 2_900.0, 3_906.0)];
        let failures = failed(&check(&runs, &Rules::PUBLISHED).unwrap());
        assert_eq!(failures.len(), 2, "{failures:#?}");
        assert!(failures[0].starts_with("scaling: LOAD") && failures[0].contains("2.00×"), "{}", failures[0]);
        assert!(failures[1].starts_with("scaling: WARM") && failures[1].contains("2.00×"), "{}", failures[1]);
    }

    #[test]
    fn a_mismatched_answer_fails() {
        let mut runs = [run(80, 128, 820.0, 560.0), run(625, 160, 8_100.0, 5_600.0)];
        runs[1].external[1].observed = "MISMATCH (expected 250): 249 models in plan".into();
        let failures = failed(&check(&runs, &Rules::PUBLISHED).unwrap());
        assert_eq!(failures.len(), 1);
        assert!(failures[0].contains("MISMATCH (expected 250)"), "{}", failures[0]);
    }

    #[test]
    fn runs_that_cannot_be_judged_are_errors() {
        let one = [run(625, 160, 8_100.0, 5_600.0)];
        assert!(check(&one, &Rules::PUBLISHED).is_err());
        let too_small = [run(16, 64, 77.0, 54.0), run(80, 128, 820.0, 560.0)];
        let e = check(&too_small, &Rules::PUBLISHED).unwrap_err().to_string();
        assert!(e.contains("largest rung measured has 10240"), "{e}");
        let mut no_edit = [run(80, 128, 820.0, 560.0), run(625, 160, 8_100.0, 5_600.0)];
        no_edit[1].external.remove(1);
        assert!(check(&no_edit, &Rules::PUBLISHED).unwrap_err().to_string().contains("no `freshet plan` of an edit"));
    }

    fn committed(name: &str) -> Vec<BenchManifest> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../results").join(name);
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }

    /// The published 100k results, re-derived from their committed files: what
    /// BENCHMARKS.md says of each run is what makes it a reading of the file.
    #[test]
    fn the_committed_100k_runs_read_as_published() {
        // Run C: met by 24 ms.
        let c = check(&committed("compare-2026-10-27-freshet-100k.json"), &Rules::PUBLISHED).unwrap();
        assert!(failed(&c).is_empty(), "{c:#?}");
        assert!(c[0].what.contains("at 100000 models took 9976 ms"), "{}", c[0]);
        // Keeping every parsed tree past the build: the budget and three scaling rows fail.
        let kept = failed(&check(&committed("compare-2026-10-27-freshet-100k-kept-trees.json"), &Rules::PUBLISHED).unwrap());
        assert_eq!(kept.len(), 4, "{kept:#?}");
        assert!(kept[0].contains("took 11837 ms"), "{}", kept[0]);
        // Run D: each tree hashed and freed at hand-over.
        let d = check(&committed("compare-2026-10-27-freshet-100k-run-d.json"), &Rules::PUBLISHED).unwrap();
        assert!(failed(&d).is_empty(), "{d:#?}");
        assert!(d[0].what.contains("at 100000 models took 8588 ms"), "{}", d[0]);
        // Run E: the daemon's fingerprint cache. Its daemon rows are the result; its
        // CLI rows ran on a slower host (the binaries alternated on one project are
        // equal) and miss the budget and three scaling rows, as published.
        let e_runs = committed("compare-2026-10-27-freshet-100k-run-e.json");
        let e = check(&e_runs, &Rules::PUBLISHED).unwrap();
        let e_failed = failed(&e);
        assert_eq!(e_failed.len(), 4, "{e_failed:#?}");
        assert!(e_failed[0].contains("took 10690 ms"), "{}", e_failed[0]);
        let daemon = |runs: &[BenchManifest], models: usize| -> f64 {
            let run = runs.iter().find(|r| r.spec.model_count() == models).unwrap();
            run.external.iter().find(|x| x.tool.contains("daemon")).unwrap().wall_ms
        };
        let d_runs = committed("compare-2026-10-27-freshet-100k-run-d.json");
        assert_eq!((daemon(&d_runs, 100_000), daemon(&e_runs, 100_000)), (3548.0, 394.0));
        assert_eq!((daemon(&d_runs, 10_240), daemon(&e_runs, 10_240)), (362.0, 46.0));
    }

    /// The committed comparison runs predate the 100k rung, but their two rungs can be
    /// held to the scaling rule today: the rule is not set tighter than the code.
    #[test]
    fn the_committed_freshet_run_scales_within_the_rule() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../results/compare-2026-09-23-freshet.json");
        let runs: Vec<BenchManifest> = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let rules = Rules { budget_models: 10_240, ..Rules::PUBLISHED };
        let findings = check(&runs, &rules).unwrap();
        assert!(failed(&findings).is_empty(), "{findings:#?}");
    }
}
