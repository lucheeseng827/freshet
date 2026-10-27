//! freshet-bench-manifest — fold `scripts/compare.sh`'s raw timing files into
//! `BenchManifest` JSON.
//!
//! ```text
//! freshet-bench-manifest RUN_DIR [RUN_DIR ...] > compare.json
//! freshet-bench-manifest --table compare.json      # Markdown tables, for humans
//! ```
//!
//! Prints a JSON array with one `BenchManifest` per `RUN_DIR` (one per project size).
//! `--table` renders a written manifest as Markdown, so a table quoted in a doc or a
//! PR comes out of the committed file by code, not by hand transcription.
//!
//! The shell script only *measures*: it appends raw repetitions to tab-separated files
//! and never computes a statistic. Medians, MADs and the schema live here, in tested
//! Rust against the `BenchManifest` type itself, so the published file cannot drift
//! from either the samples or the struct. Each `RUN_DIR` holds (`⇥` is a tab):
//!
//! * `export.json` — the `ExportSummary` of the base project (gives the spec).
//! * `export-edit.json` — *optional*; the edited project's summary (gives the edit).
//! * `provenance.tsv` — `key⇥value`; keys `commit dirty rustc cpu cores os build`,
//!   optionally `criterion`, and `tool:<name>⇥<version>` per tool.
//! * `ops.tsv` — `id⇥tool⇥op⇥command⇥prepare⇥note`, one per timed operation, in run
//!   order.
//! * `samples.tsv` — `id⇥warmup|sample⇥real_s⇥user_s⇥sys_s⇥other_ms⇥steal_ms`, one
//!   per repetition: the first three as bash's `time` prints them under
//!   `TIMEFORMAT='%3R %3U %3S'`, then the CPU the rest of the machine used and the
//!   hypervisor steal during the repetition, in whole ms, or `-` if not measured.
//! * `observed.tsv` — *optional*; `id⇥what the command reported`.
//! * `skipped.tsv` — *optional*; `tool⇥op⇥reason`.
//!
//! Anything malformed is an error, never a silently dropped row: an operation with no
//! samples must be in `skipped.tsv`, not merely absent.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::ExitCode;

use anyhow::{bail, ensure, Context, Result};
use freshet_bench::export::ExportSummary;
use freshet_bench::{BenchManifest, ExternalDatum, Provenance, SkippedDatum};

const USAGE: &str = "usage: freshet-bench-manifest RUN_DIR [RUN_DIR ...] > compare.json\n       \
                     freshet-bench-manifest --table compare.json";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args.iter().any(|a| a == "-h" || a == "--help") {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }
    let run = || -> Result<()> {
        match args.as_slice() {
            [flag, file] if flag == "--table" => {
                let runs: Vec<BenchManifest> = read_json(Path::new(file))?;
                print!("{}", table(&runs));
            }
            [flag, ..] if flag == "--table" => bail!("--table takes exactly one manifest file\n{USAGE}"),
            dirs => {
                let manifests = dirs.iter().map(|d| manifest(Path::new(d))).collect::<Result<Vec<_>>>()?;
                println!("{}", serde_json::to_string_pretty(&manifests)?);
            }
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

fn manifest(dir: &Path) -> Result<BenchManifest> {
    let base: ExportSummary = read_json(&dir.join("export.json"))?;
    let edit = match optional(&dir.join("export-edit.json"))? {
        Some(raw) => {
            let edited: ExportSummary = serde_json::from_str(&raw).context("parsing export-edit.json")?;
            ensure!(
                edited.spec.layers == base.spec.layers
                    && edited.spec.width == base.spec.width
                    && edited.spec.cols == base.spec.cols
                    && edited.spec.seed == base.spec.seed,
                "export-edit.json was generated from a different spec than export.json"
            );
            edited.edit
        }
        None => None,
    };

    let provenance = provenance(&read(&dir.join("provenance.tsv"))?)?;
    let skipped = match optional(&dir.join("skipped.tsv"))? {
        Some(raw) => rows(&raw, 3, "skipped.tsv")?
            .into_iter()
            .map(|r| SkippedDatum { tool: r[0].into(), op: r[1].into(), reason: r[2].into() })
            .collect(),
        None => Vec::new(),
    };
    let observed = optional(&dir.join("observed.tsv"))?.unwrap_or_default();
    let external = data(&read(&dir.join("ops.tsv"))?, &read(&dir.join("samples.tsv"))?, &observed)?;

    Ok(BenchManifest { spec: base.spec, provenance, external, edit, skipped })
}

/// Join ops, samples and observations into summarized data, in `ops.tsv` order.
fn data(ops: &str, samples: &str, observed: &str) -> Result<Vec<ExternalDatum>> {
    let mut order = Vec::new();
    let mut by_id: BTreeMap<String, ExternalDatum> = BTreeMap::new();
    for r in rows(ops, 6, "ops.tsv")? {
        let datum = ExternalDatum {
            tool: r[1].into(),
            op: r[2].into(),
            wall_ms: f64::NAN,
            note: r[5].into(),
            command: r[3].into(),
            prepare: r[4].into(),
            samples_ms: Vec::new(),
            cpu_ms: Vec::new(),
            warmup_ms: Vec::new(),
            other_cpu_ms: Vec::new(),
            steal_ms: Vec::new(),
            mad_ms: f64::NAN,
            observed: String::new(),
        };
        ensure!(!datum.command.is_empty() && !datum.note.is_empty(), "ops.tsv: `{}` is unlabeled", r[0]);
        ensure!(by_id.insert(r[0].to_string(), datum).is_none(), "ops.tsv: duplicate id `{}`", r[0]);
        order.push(r[0].to_string());
    }

    // Repetitions whose contention could not be measured, per op. All or none: a
    // series with holes could not be lined up with the samples it annotates.
    let mut unmeasured: BTreeMap<String, usize> = BTreeMap::new();
    for r in rows(samples, 7, "samples.tsv")? {
        let d = by_id.get_mut(r[0]).with_context(|| format!("samples.tsv: unknown op id `{}`", r[0]))?;
        let wall = seconds_to_ms(r[2])?;
        let cpu = seconds_to_ms(r[3])? + seconds_to_ms(r[4])?;
        match r[1] {
            "sample" => {
                d.samples_ms.push(wall);
                d.cpu_ms.push(cpu);
                match (whole_ms(r[5])?, whole_ms(r[6])?) {
                    (Some(other), Some(steal)) => {
                        d.other_cpu_ms.push(other);
                        d.steal_ms.push(steal);
                    }
                    (None, None) => *unmeasured.entry(r[0].to_string()).or_default() += 1,
                    _ => bail!("samples.tsv: `{}` measured one contention column but not the other", r[0]),
                }
            }
            "warmup" => d.warmup_ms.push(wall),
            other => bail!("samples.tsv: kind must be `warmup` or `sample`, got `{other}`"),
        }
    }
    for (id, missing) in unmeasured {
        let d = &by_id[&id];
        ensure!(
            d.other_cpu_ms.is_empty(),
            "samples.tsv: `{id}` has contention for {} repetition(s) and not for {missing}",
            d.other_cpu_ms.len()
        );
    }

    for r in rows(observed, 2, "observed.tsv")? {
        let d = by_id.get_mut(r[0]).with_context(|| format!("observed.tsv: unknown op id `{}`", r[0]))?;
        d.observed = r[1].into();
    }

    order
        .into_iter()
        .map(|id| {
            let d = by_id.remove(&id).expect("every ordered id was inserted");
            ensure!(
                !d.samples_ms.is_empty(),
                "op `{id}` ({} / {}) has no timed samples; record it in skipped.tsv instead",
                d.tool,
                d.op
            );
            Ok(d.summarize())
        })
        .collect()
}

fn provenance(raw: &str) -> Result<Provenance> {
    let mut p = Provenance::default();
    for r in rows(raw, 2, "provenance.tsv")? {
        let (key, value) = (r[0], r[1].to_string());
        match key {
            "commit" => p.commit = value,
            "dirty" => p.dirty = value.parse().context("provenance.tsv: dirty must be true|false")?,
            "rustc" => p.rustc = value,
            "cpu" => p.cpu = value,
            "cores" => p.cores = value.parse().context("provenance.tsv: cores must be an integer")?,
            "os" => p.os = value,
            "build" => p.build = value,
            "criterion" => p.criterion = value,
            _ => match key.strip_prefix("tool:") {
                Some(tool) => {
                    p.tools.insert(tool.to_string(), value);
                }
                None => bail!("provenance.tsv: unknown key `{key}`"),
            },
        }
    }
    ensure!(
        !p.commit.is_empty() && !p.rustc.is_empty() && !p.cpu.is_empty() && p.cores > 0,
        "provenance.tsv must name the commit, rustc, cpu and core count"
    );
    Ok(p)
}

/// One Markdown section per run: the machine, the edit, then every datum with its
/// median, MAD, range, CPU/wall ratio, the worst contention any of its repetitions
/// saw, and what the tool reported; then every skip.
///
/// Times print in seconds with three decimals — exact, since the samples are whole
/// milliseconds — so a 0.1 s row and a 100 s row read on one scale. Contention prints
/// as the cores other processes kept busy on average during the worst repetition,
/// because "300 ms of someone else's CPU" means nothing until it is set against how
/// long the repetition ran.
fn table(runs: &[BenchManifest]) -> String {
    let secs = |ms: f64| format!("{:.3} s", ms / 1000.0);
    let worst = |series: &[f64], samples: &[f64], fmt: &dyn Fn(f64) -> String| {
        if series.is_empty() {
            return "n/a".to_string();
        }
        let max = series.iter().zip(samples).map(|(x, wall)| x / wall).fold(f64::NEG_INFINITY, f64::max);
        fmt(max)
    };
    let mut out = Vec::new();
    for run in runs {
        let (s, p) = (&run.spec, &run.provenance);
        out.push(format!(
            "### {} models ({} layers x {} wide, {} columns, seed {:#x})\n",
            s.model_count(),
            s.layers,
            s.width,
            s.cols,
            s.seed
        ));
        let commit = p.commit.get(..12).unwrap_or(&p.commit);
        let dirty = if p.dirty { ", worktree dirty" } else { "" };
        out.push(format!("{}, {} cores, {}; commit {commit}{dirty}; {}", p.cpu, p.cores, p.os, p.build));
        if let Some(e) = &run.edit {
            out.push(format!("Edit under test: `{}`, which a correct plan rebuilds with {} model(s) in total.", e.model, e.closure));
        }
        out.push(String::new());
        out.push(
            "| tool | operation | median | MAD | min - max | CPU / wall | others, worst rep | steal, worst rep | n | reported |"
                .into(),
        );
        out.push("|---|---|--:|--:|--:|--:|--:|--:|--:|---|".into());
        for d in &run.external {
            let lo = d.samples_ms.iter().copied().fold(f64::INFINITY, f64::min);
            let hi = d.samples_ms.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            let cpu = freshet_bench::median(&d.cpu_ms) / d.wall_ms;
            let cores = |c: f64| format!("{c:.2} cores");
            let others = worst(&d.other_cpu_ms, &d.samples_ms, &cores);
            let steal = worst(&d.steal_ms, &d.samples_ms, &cores);
            out.push(format!(
                "| {} | {} | {} | {} | {} - {} | {cpu:.2}x | {others} | {steal} | {} | {} |",
                d.tool,
                d.op,
                secs(d.wall_ms),
                secs(d.mad_ms),
                secs(lo),
                secs(hi),
                d.samples_ms.len(),
                d.observed
            ));
        }
        for sk in &run.skipped {
            out.push(format!("| {} | {} | skipped | | | | | | | {} |", sk.tool, sk.op, sk.reason));
        }
        out.push(String::new());
    }
    out.join("\n")
}

/// Seconds with at most three decimals (bash `time`'s `%3R`) → exact milliseconds.
///
/// Parsed as integers rather than `s * 1000.0`, which would turn `1.234` into
/// `1233.9999999999998` in the published JSON.
fn seconds_to_ms(raw: &str) -> Result<f64> {
    let (whole, frac) = raw.split_once('.').unwrap_or((raw, ""));
    let bad = || format!("`{raw}` is not seconds with at most 3 decimals");
    ensure!(
        !whole.is_empty() && frac.len() <= 3 && (whole.chars().chain(frac.chars())).all(|c| c.is_ascii_digit()),
        "{}",
        bad()
    );
    let whole: u64 = whole.parse().with_context(bad)?;
    let frac: u64 = if frac.is_empty() { 0 } else { format!("{frac:0<3}").parse().with_context(bad)? };
    Ok((whole * 1000 + frac) as f64)
}

/// A whole number of milliseconds (possibly negative: tick-granular accounting can
/// undershoot), or `-` for "not measured".
fn whole_ms(raw: &str) -> Result<Option<f64>> {
    if raw == "-" {
        return Ok(None);
    }
    let ms: i64 = raw.parse().with_context(|| format!("`{raw}` is not a whole number of ms or `-`"))?;
    Ok(Some(ms as f64))
}

/// Non-empty lines of a TSV, each with exactly `width` fields.
fn rows<'a>(raw: &'a str, width: usize, file: &str) -> Result<Vec<Vec<&'a str>>> {
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .enumerate()
        .map(|(i, line)| {
            let fields: Vec<&str> = line.split('\t').collect();
            let (line_no, got) = (i + 1, fields.len());
            ensure!(got == width, "{file}:{line_no}: expected {width} tab-separated fields, got {got}");
            Ok(fields)
        })
        .collect()
}

fn read(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))
}

fn optional(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(s) => Ok(Some(s)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    serde_json::from_str(&read(path)?).with_context(|| format!("parsing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seconds_become_exact_milliseconds() {
        assert_eq!(seconds_to_ms("1.234").unwrap(), 1234.0);
        assert_eq!(seconds_to_ms("0.070").unwrap(), 70.0);
        assert_eq!(seconds_to_ms("12.5").unwrap(), 12500.0);
        assert_eq!(seconds_to_ms("3").unwrap(), 3000.0);
        for bad in ["", ".5", "1.2345", "1,234", "-1.000", "1.2e3"] {
            assert!(seconds_to_ms(bad).is_err(), "{bad:?}");
        }
    }

    const OPS: &str = concat!(
        "a\tfreshet\tplan\tfreshet plan\t\tnote a\n",
        "b\tdbt-core\tparse (cold)\tdbt parse\trm -rf target\tnote b\n",
    );

    #[test]
    fn samples_fold_into_ordered_summarized_data() {
        let samples = "b\twarmup\t9.000\t8.000\t1.000\t50\t0\n\
                       b\tsample\t5.000\t4.500\t0.250\t20\t0\n\
                       a\tsample\t0.100\t0.090\t0.010\t-\t-\n\
                       b\tsample\t7.000\t6.000\t0.500\t-10\t30\n\
                       b\tsample\t6.000\t5.000\t0.500\t1500\t0\n";
        let d = data(OPS, samples, "a\t12 models in plan\n").unwrap();
        assert_eq!(d.iter().map(|d| d.op.as_str()).collect::<Vec<_>>(), ["plan", "parse (cold)"], "ops.tsv order");
        assert_eq!((d[0].wall_ms, d[0].cpu_ms.as_slice()), (100.0, &[100.0][..]));
        assert_eq!(d[0].observed, "12 models in plan");
        assert!(d[0].other_cpu_ms.is_empty() && d[0].steal_ms.is_empty(), "`-` is not measured, not zero");
        assert_eq!(d[1].samples_ms, [5000.0, 7000.0, 6000.0], "run order, warm-up excluded");
        assert_eq!(d[1].warmup_ms, [9000.0]);
        assert_eq!(d[1].cpu_ms, [4750.0, 6500.0, 5500.0]);
        assert_eq!(d[1].other_cpu_ms, [20.0, -10.0, 1500.0], "aligned with samples; negatives kept");
        assert_eq!(d[1].steal_ms, [0.0, 30.0, 0.0]);
        assert_eq!((d[1].wall_ms, d[1].mad_ms), (6000.0, 1000.0));
        assert_eq!(d[1].prepare, "rm -rf target");
        assert!(d.iter().all(ExternalDatum::is_consistent));
    }

    #[test]
    fn contention_is_all_or_nothing_per_op() {
        let holes = "a\tsample\t0.100\t0.090\t0.010\t5\t0\na\tsample\t0.100\t0.090\t0.010\t-\t-\n\
                     b\tsample\t5.000\t4.500\t0.250\t1\t0\n";
        let err = data(OPS, holes, "").unwrap_err().to_string();
        assert!(err.contains("not for 1"), "{err}");
        let half = "a\tsample\t0.100\t0.090\t0.010\t5\t-\nb\tsample\t5.000\t4.500\t0.250\t1\t0\n";
        let err = data(OPS, half, "").unwrap_err().to_string();
        assert!(err.contains("one contention column"), "{err}");
    }

    #[test]
    fn an_op_without_samples_or_a_stray_sample_is_an_error() {
        let err = data(OPS, "a\tsample\t0.100\t0.090\t0.010\t0\t0\n", "").unwrap_err().to_string();
        assert!(err.contains("skipped.tsv"), "{err}");
        let err = data(OPS, "z\tsample\t0.100\t0.090\t0.010\t0\t0\n", "").unwrap_err().to_string();
        assert!(err.contains("unknown op id"), "{err}");
        let err = data("a\tt\to\tcmd\t\tnote\ta stray tab\n", "", "").unwrap_err().to_string();
        assert!(err.contains("expected 6"), "{err}");
    }

    #[test]
    fn the_table_shows_every_datum_and_every_skip() {
        let samples = "a\tsample\t0.100\t0.090\t0.010\t-\t-\n\
                       b\tsample\t5.000\t4.500\t0.500\t500\t50\n\
                       b\tsample\t7.000\t6.500\t0.500\t14000\t0\n";
        let run = BenchManifest {
            spec: freshet_bench::GenSpec { layers: 2, width: 3, cols: 1, seed: 0x51 },
            provenance: provenance("commit\t0123456789abcdef\nrustc\tr\ncpu\tXeon\ncores\t4\nos\tLinux\n").unwrap(),
            external: data(OPS, samples, "").unwrap(),
            edit: Some(freshet_bench::EditSpec { model: "m_0_0".into(), closure: 2 }),
            skipped: vec![SkippedDatum { tool: "sqlmesh".into(), op: "info".into(), reason: "not installed".into() }],
        };
        let t = table(&[run]);
        assert!(t.contains("### 6 models (2 layers x 3 wide, 1 columns, seed 0x51)"), "{t}");
        assert!(t.contains("commit 0123456789ab;"), "{t}");
        assert!(t.contains("| freshet | plan | 0.100 s | 0.000 s | 0.100 s - 0.100 s | 1.00x | n/a | n/a | 1 |"), "{t}");
        // Worst repetition by share of its own wall time: 14 s of others' CPU over a
        // 7 s repetition is 2 cores; 50 ms of steal over 5 s is 0.01 of one.
        assert!(
            t.contains("| dbt-core | parse (cold) | 6.000 s | 1.000 s | 5.000 s - 7.000 s | 1.00x | 2.00 cores | 0.01 cores | 2 |"),
            "{t}"
        );
        assert!(t.contains("| sqlmesh | info | skipped |"), "{t}");
        assert!(t.contains("`m_0_0`, which a correct plan rebuilds with 2 model(s)"), "{t}");
    }

    #[test]
    fn provenance_requires_the_machine_and_collects_tools() {
        let p = provenance(
            "commit\tabc\ndirty\tfalse\nrustc\trustc 1.94.1\ncpu\tXeon\ncores\t4\nos\tLinux\nbuild\trelease\n\
             tool:dbt-core\t1.12.5\ntool:sqlmesh\t0.236.2\n",
        )
        .unwrap();
        assert_eq!((p.cores, p.dirty, p.tools.len()), (4, false, 2));
        assert!(provenance("commit\tabc\nrustc\tr\ncpu\tx\n").is_err(), "no core count");
        assert!(provenance("commit\tabc\nrustc\tr\ncpu\tx\ncores\t4\nmystery\t1\n").is_err());
    }
}
