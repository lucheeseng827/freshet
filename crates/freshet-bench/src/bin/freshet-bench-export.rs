//! freshet-bench-export — write one generated topology as dbt / SQLMesh / Freshet
//! projects, for the out-of-band comparison in `scripts/compare.sh`.
//!
//! ```text
//! freshet-bench-export --out DIR [--layers N] [--width N] [--cols N] [--seed N|0xHEX]
//!                      [--format dbt|sqlmesh|freshet|all] [--edit MODEL]
//! ```
//!
//! Each format is written to `DIR/<format>` (`DIR/dbt`, `DIR/sqlmesh`, `DIR/freshet`)
//! even when only one is asked for, so a script never has to know which layout it got.
//! Unset knobs take `GenSpec::default()`'s values. `--edit MODEL` applies the
//! comparison's one-model edit before exporting (see `export::edit_model`). A JSON
//! `ExportSummary` goes to stdout.
//!
//! Arguments are parsed by hand: six flags do not justify putting an argument parser
//! into a crate whose library half is deliberately dependency-light.

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{bail, ensure, Context, Result};
use freshet_bench::export::{export, Format};
use freshet_bench::GenSpec;

const USAGE: &str = "usage: freshet-bench-export --out DIR [--layers N] [--width N] [--cols N] \
[--seed N|0xHEX] [--format dbt|sqlmesh|freshet|all] [--edit MODEL]";

struct Args {
    spec: GenSpec,
    out: PathBuf,
    formats: Vec<Format>,
    edit: Option<String>,
}

fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(Some(args)) => args,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("error: {e:#}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> Result<()> {
    let summary = export(&args.spec, &args.formats, args.edit.as_deref(), &args.out)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

/// `Ok(None)` means `--help` was asked for.
fn parse_args(mut argv: impl Iterator<Item = String>) -> Result<Option<Args>> {
    let mut spec = GenSpec::default();
    let mut out = None;
    let mut formats = Format::ALL.to_vec();
    let mut edit = None;
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().with_context(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--layers" => spec.layers = count(&flag, &value()?)?,
            "--width" => spec.width = count(&flag, &value()?)?,
            "--cols" => spec.cols = count(&flag, &value()?)?,
            "--seed" => spec.seed = seed(&value()?)?,
            "--out" => out = Some(PathBuf::from(value()?)),
            "--format" => {
                formats = match value()?.as_str() {
                    "all" => Format::ALL.to_vec(),
                    one => vec![one.parse()?],
                }
            }
            "--edit" => edit = Some(value()?),
            "-h" | "--help" => return Ok(None),
            other => bail!("unknown argument `{other}`"),
        }
    }
    let out = out.context("--out DIR is required")?;
    Ok(Some(Args { spec, out, formats, edit }))
}

/// A layer/width/column count. Zero is refused rather than silently clamped to one
/// the way `generate` would: a size typed as 0 and measured as 1 is a mislabeled row.
fn count(flag: &str, raw: &str) -> Result<usize> {
    let n: usize = raw.parse().with_context(|| format!("{flag} expects a positive integer, got `{raw}`"))?;
    ensure!(n > 0, "{flag} must be at least 1");
    Ok(n)
}

/// Decimal or `0x` hex, `_` separators allowed — the spelling `GenSpec::default()`
/// and BENCHMARKS.md use (`0x5151_5151`).
fn seed(raw: &str) -> Result<u64> {
    let digits = raw.replace('_', "");
    let parsed = match digits.strip_prefix("0x").or_else(|| digits.strip_prefix("0X")) {
        Some(hex) => u64::from_str_radix(hex, 16),
        None => digits.parse(),
    };
    parsed.with_context(|| format!("--seed expects a decimal or 0x-hex u64, got `{raw}`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Option<Args>> {
        parse_args(args.iter().map(|s| s.to_string()))
    }

    #[test]
    fn flags_override_the_default_spec() {
        let a = parse(&["--out", "d", "--layers", "3", "--width", "4", "--cols", "2", "--seed", "0x5151_5151"])
            .unwrap()
            .unwrap();
        assert_eq!((a.spec.layers, a.spec.width, a.spec.cols, a.spec.seed), (3, 4, 2, 0x5151_5151));
        assert_eq!(a.formats, Format::ALL.to_vec());
        let a = parse(&["--out", "d", "--format", "sqlmesh", "--edit", "m_0_0", "--seed", "42"]).unwrap().unwrap();
        assert_eq!((a.formats, a.edit.as_deref(), a.spec.seed), (vec![Format::Sqlmesh], Some("m_0_0"), 42));
    }

    #[test]
    fn bad_input_is_refused_by_name() {
        for (args, needle) in [
            (&["--layers", "2"][..], "--out"),
            (&["--out", "d", "--layers", "0"][..], "at least 1"),
            (&["--out", "d", "--width"][..], "needs a value"),
            (&["--out", "d", "--format", "parquet"][..], "unknown format"),
            (&["--out", "d", "--seed", "0xZZ"][..], "--seed"),
            (&["--out", "d", "--bogus"][..], "unknown argument"),
        ] {
            let err = parse(args).err().map(|e| format!("{e:#}")).unwrap_or_default();
            assert!(err.contains(needle), "{args:?}: {err}");
        }
        assert!(parse(&["--help"]).unwrap().is_none());
    }
}
