//! freshet-dbt — point Freshet at an existing dbt project and show what it would have
//! pruned.
//!
//! The adoption wedge (the planner core's product roadmap, Phase 1): a team
//! already running dbt gets a number about *their* project — "of the models your last
//! run built, this change needed these" — without adopting anything. Nothing here runs
//! dbt, parses Jinja beyond `ref()`/`source()`, or touches a warehouse: it reads the
//! artifacts dbt already wrote.
//!
//! * [`artifacts`] — the slice of `manifest.json` / `run_results.json` Freshet reads,
//!   tolerant of every field it does not.
//! * [`DbtProject`] — a manifest as a planner [`Project`](freshet_core::Project): edges
//!   from the manifest, column lineage from each model's SQL, model-level lineage
//!   wherever that lineage cannot be trusted.
//! * [`explain`] / [`explain_against`] — the comparison: changed models between a
//!   `--state` manifest and the current one, Freshet's plan for them, and that plan set
//!   against the last run and `state:modified+`.
//!
//! ## The one invariant
//!
//! Over-rebuild is safe; under-rebuild is a data bug; so a report that says Freshet
//! would skip a model must be right about that model. Every doubt resolves towards
//! rebuilding — an unreadable model, a relation name two relations share, a filter on a
//! column whose source cannot be traced — and the report says which models that
//! happened to, because each one makes the saving smaller than it might really be.

pub mod artifacts;
mod diff;
mod jinja;
mod project;
mod report;
mod sqlscan;

use std::path::{Path, PathBuf};

use anyhow::{bail, Result};

pub use artifacts::{Manifest, RunResults};
pub use project::{materialization, DbtModel, DbtProject, Lineage, SqlBasis, UNKNOWN_COLUMNS};
pub use report::{
    explain, Artifacts, Baseline, ChangedModel, ComparedOn, Comparison, ExplainReport, GraphFacts, LastRun,
    ModelLevel, Reason, Rebuild, REPORT_VERSION,
};

/// Read a dbt project's artifacts and build the report.
///
/// * `project_dir` — the dbt project root: reads `target/manifest.json` and, when it is
///   there, `target/run_results.json`. A `target` directory itself is accepted too.
/// * `state_dir` — dbt's `--state` convention: a directory holding the `manifest.json`
///   to compare against (production artifacts, the main branch's build). A path to the
///   manifest file itself is accepted too. Without it the report has graph facts and no
///   pruning number.
pub fn explain_against(project_dir: &Path, state_dir: Option<&Path>) -> Result<ExplainReport> {
    let target = target_dir(project_dir)?;
    let manifest_path = target.join("manifest.json");
    let manifest = Manifest::from_path(&manifest_path)?;

    let run_results_path = target.join("run_results.json");
    let run_results = if run_results_path.is_file() { Some(RunResults::from_path(&run_results_path)?) } else { None };

    let state_path = state_dir.map(state_manifest).transpose()?;
    let state = state_path.as_deref().map(Manifest::from_path).transpose()?;

    let artifacts = Artifacts {
        manifest: Some(manifest_path.display().to_string()),
        run_results: run_results.as_ref().map(|_| run_results_path.display().to_string()),
        state_manifest: state_path.as_ref().map(|p| p.display().to_string()),
    };
    Ok(explain(&manifest, state.as_ref(), run_results.as_ref(), artifacts))
}

/// The directory holding `manifest.json`: `<dir>/target`, or `<dir>` itself when it is
/// already a target directory.
fn target_dir(project_dir: &Path) -> Result<PathBuf> {
    let target = project_dir.join("target");
    if target.join("manifest.json").is_file() {
        return Ok(target);
    }
    if project_dir.join("manifest.json").is_file() {
        return Ok(project_dir.to_path_buf());
    }
    if !project_dir.is_dir() {
        bail!("{} is not a directory (expected the root of a dbt project)", project_dir.display());
    }
    bail!(
        "no dbt manifest at {} — run `dbt parse` or `dbt compile` in {} first \
         (`dbt compile` also lets Freshet read the SQL of models that use Jinja)",
        target.join("manifest.json").display(),
        project_dir.display()
    )
}

fn state_manifest(state: &Path) -> Result<PathBuf> {
    if state.is_file() {
        return Ok(state.to_path_buf());
    }
    let manifest = state.join("manifest.json");
    if manifest.is_file() {
        return Ok(manifest);
    }
    bail!(
        "no manifest.json in --state {} (dbt's --state convention: a directory holding the \
         manifest.json of the run to compare against, e.g. production artifacts)",
        state.display()
    )
}
