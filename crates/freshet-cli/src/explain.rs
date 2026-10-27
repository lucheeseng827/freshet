//! `freshet explain --against <dbt-project-dir>` — what Freshet would have pruned from a
//! dbt project's last run.
//!
//! ```text
//! freshet explain --against ./jaffle_shop                      # graph facts only
//! freshet explain --against ./jaffle_shop --state ./prod-run   # the pruning number
//! freshet explain --against ./jaffle_shop --state ./prod-run --json
//! ```
//!
//! Read-only and offline: it reads the artifacts dbt already wrote
//! (`target/manifest.json`, `target/run_results.json`, and the `--state` directory's
//! `manifest.json`) and never runs dbt, opens a socket, or touches a warehouse. All of
//! the logic lives in `freshet-dbt`; this file is argument parsing and printing, so the
//! report a binding or a hosted page renders is the one the CLI prints.

use std::path::PathBuf;

use anyhow::{Context, Result};

#[derive(clap::Args)]
pub struct ExplainArgs {
    /// The dbt project to read: its `target/manifest.json` (from `dbt parse`,
    /// `dbt compile`, `dbt run` or `dbt build`) and `target/run_results.json` when there
    /// is one. A `target` directory itself works too.
    #[arg(long, value_name = "DIR")]
    against: PathBuf,
    /// The manifest to compare against, dbt's own `--state` convention: a directory
    /// holding the `manifest.json` of the run you are measuring from (production
    /// artifacts, the main branch's build). Without it there is no change to plan and
    /// no pruning number.
    #[arg(long, value_name = "DIR")]
    state: Option<PathBuf>,
    /// Print the report as JSON instead of text.
    #[arg(long)]
    json: bool,
}

pub fn run(args: &ExplainArgs) -> Result<()> {
    let report = freshet_dbt::explain_against(&args.against, args.state.as_deref())?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report).context("serializing the report")?);
    } else {
        print!("{}", report.render_text());
    }
    Ok(())
}
