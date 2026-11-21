//! freshet-bench-quality — the plan-quality sweep across lineage densities.
//!
//! ```text
//! freshet-bench-quality > results/quality-sweep.json
//! freshet-bench-quality --table results/quality-sweep.json   # Markdown, for humans
//! ```
//!
//! What each count means is in [`freshet_bench::quality`]. The committed file is
//! pinned to the code by a test, so it is regenerated whenever the planner's answer
//! changes, and a plan that misses a model fails before anything is written.

use std::process::ExitCode;

use anyhow::{bail, Context, Result};
use freshet_bench::quality::{sweep, table, Sweep};

const USAGE: &str = "usage: freshet-bench-quality > results/quality-sweep.json\n       \
                     freshet-bench-quality --table results/quality-sweep.json";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let run = || -> Result<()> {
        match args.as_slice() {
            [] => {
                let s = sweep()?;
                if let Some(p) = s.points.iter().find(|p| p.missed > 0) {
                    bail!("UNSOUND: {} parents, {} columns each, {:?} edit: {} models missed", p.fan_in, p.reads, p.edit, p.missed);
                }
                println!("{}", serde_json::to_string_pretty(&s)?);
            }
            [flag, file] if flag == "--table" => {
                let raw = std::fs::read_to_string(file).with_context(|| format!("reading {file}"))?;
                let s: Sweep = serde_json::from_str(&raw).with_context(|| format!("parsing {file}"))?;
                print!("{}", table(&s));
            }
            _ => bail!("{USAGE}"),
        }
        Ok(())
    };
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}
