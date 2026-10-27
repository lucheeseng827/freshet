//! `freshet move` — move a warehouse, partition by partition: render each unit's
//! unload, load and parity check, for dagron to run.
//!
//! ```text
//! freshet move --from postgres --to snowflake --stage s3://acme-moves/2026-10/ \
//!              --to-schema ANALYTICS.MARTS --storage-integration FRESHET_STAGE \
//!              --partitions 2026-06-01..2026-06-30 [--model events,sessions] [--json]
//! ```
//!
//! Executes nothing and connects to nothing, like every other command. The SQL is
//! rendered by `freshet_render::migrate`; this file decides *what* moves and prints it.
//!
//! What moves, in plan (topological) order: every model, or the ones `--model`
//! names. An incremental model that declares a `partition_column` moves one unit per
//! `--partitions` key; every other table moves whole; a view does not move, since
//! the target gets it by being planned for. Without `--partitions`, everything moves
//! whole. `--json` prints the `freshet-move/1` document dagron-state compiles.

use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use freshet_core::{BackfillPlan, BackfillUnit, Materialization};
use freshet_render::migrate::{render_move, MoveOptions, RenderedMove};
use freshet_render::Dialect;

#[derive(clap::Args)]
pub struct MoveArgs {
    #[arg(long, default_value = ".")]
    project: PathBuf,
    /// The warehouse moved from: postgres | snowflake | bigquery | databricks | redshift.
    #[arg(long, value_name = "DIALECT")]
    from: String,
    /// The warehouse moved to.
    #[arg(long, value_name = "DIALECT")]
    to: String,
    /// The move's own prefix, which every unit's Parquet goes under:
    /// s3://bucket/prefix/, gs://bucket/prefix/, or file:///path/ (Postgres to Postgres).
    #[arg(long, value_name = "URI")]
    stage: String,
    /// Move only these models (and none of their downstream).
    #[arg(long, value_name = "MODEL,...", value_delimiter = ',')]
    model: Vec<String>,
    /// The partition keys to move each partitioned model by. `YYYY-MM-DD..YYYY-MM-DD`
    /// is every day from the first to the last, both included.
    #[arg(long, value_name = "KEY,...", value_delimiter = ',')]
    partitions: Vec<String>,
    /// Qualify every relation on the source with this schema.
    #[arg(long, value_name = "SCHEMA")]
    from_schema: Option<String>,
    /// Qualify every relation on the target (`analytics`, `db.schema`, `project.dataset`).
    #[arg(long, value_name = "SCHEMA")]
    to_schema: Option<String>,
    /// Snowflake: the storage integration that reaches the stage.
    #[arg(long, value_name = "NAME")]
    storage_integration: Option<String>,
    /// Redshift: the IAM role that reaches the stage (an ARN, or `default`).
    #[arg(long, value_name = "ARN|default")]
    iam_role: Option<String>,
    /// Print the `freshet-move/1` document instead of text.
    #[arg(long)]
    json: bool,
}

/// The most days a range expands to: ten years of daily partitions. A range past it
/// is more likely a typo in a year than a move.
const MAX_RANGE_DAYS: usize = 3660;

pub fn run(args: &MoveArgs) -> Result<()> {
    let crate::Loaded { project, incremental, .. } = crate::load_project(&args.project)?;
    let opts = MoveOptions {
        from: args.from.parse::<Dialect>()?,
        to: args.to.parse::<Dialect>()?,
        from_schema: args.from_schema.clone(),
        to_schema: args.to_schema.clone(),
        stage: args.stage.parse().map_err(anyhow::Error::msg)?,
        storage_integration: args.storage_integration.clone(),
        iam_role: args.iam_role.clone(),
    };
    let keys = expand_partitions(&args.partitions)?;

    let order = project
        .topo_order()
        .map_err(|cycle| anyhow::anyhow!("project has a dependency cycle involving: {}", cycle.join(", ")))?;
    let named: BTreeSet<&str> = args.model.iter().map(String::as_str).collect();
    if let Some(unknown) = named.iter().find(|m| project.get(m).is_none()) {
        bail!("--model {unknown}: the project has no such model");
    }
    let partitioned = |name: &str| {
        incremental
            .models
            .get(name)
            .and_then(|m| m.replace.as_ref())
            .is_some_and(|r| r.partition_column.is_some())
    };
    let mut plan = BackfillPlan::default();
    for name in order.iter().filter(|m| named.is_empty() || named.contains(m.as_str())) {
        plan.models.push(name.clone());
        let incremental_model = project.get(name).is_some_and(|m| m.materialization == Materialization::Incremental);
        // A model named on its own is moved by partition as asked, and refused if it
        // cannot be. One swept up in "every model" moves whole instead.
        let by_partition = !keys.is_empty() && incremental_model && (partitioned(name) || named.contains(name.as_str()));
        if by_partition {
            plan.unit.insert(name.clone(), BackfillUnit::Partitions(keys.clone()));
        }
    }

    // Every reason, and nothing else: a move that renders part of a plan is not one
    // to start. Printed plainly rather than as an error chain, as `--sql` does.
    let rendered = match render_move(&project, &plan, &incremental, &opts) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };
    if args.json {
        let doc = freshet_embed::wire_move(&rendered, &opts);
        println!("{}", serde_json::to_string_pretty(&doc).context("serializing the move")?);
    } else {
        print_human(&rendered, &opts);
    }
    Ok(())
}

/// `--partitions`, with every `A..B` day range spelled out.
fn expand_partitions(raw: &[String]) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for token in raw.iter().map(|t| t.trim()).filter(|t| !t.is_empty()) {
        let Some((from, to)) = token.split_once("..") else {
            out.push(token.to_string());
            continue;
        };
        let (Some(mut day), Some(last)) = (Day::parse(from), Day::parse(to)) else {
            bail!("--partitions {token}: a range is YYYY-MM-DD..YYYY-MM-DD");
        };
        if last < day {
            bail!("--partitions {token}: the range ends before it starts");
        }
        let start = out.len();
        while day <= last {
            out.push(day.to_string());
            if out.len() - start > MAX_RANGE_DAYS {
                bail!("--partitions {token}: more than {MAX_RANGE_DAYS} days; split it, or check the years");
            }
            day = day.next();
        }
    }
    let mut seen = BTreeSet::new();
    out.retain(|k| seen.insert(k.clone()));
    Ok(out)
}

/// A calendar day, enough for ranges: parse, compare, step.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Day(u32, u32, u32);

impl Day {
    fn parse(s: &str) -> Option<Day> {
        let mut it = s.trim().splitn(3, '-');
        let (y, m, d) = (it.next()?.parse().ok()?, it.next()?.parse().ok()?, it.next()?.parse().ok()?);
        (s.trim().len() == 10 && (1..=12).contains(&m) && (1..=Day::days_in(y, m)).contains(&d)).then_some(Day(y, m, d))
    }

    fn days_in(y: u32, m: u32) -> u32 {
        match m {
            4 | 6 | 9 | 11 => 30,
            2 if (y.is_multiple_of(4) && !y.is_multiple_of(100)) || y.is_multiple_of(400) => 29,
            2 => 28,
            _ => 31,
        }
    }

    fn next(self) -> Day {
        let Day(y, m, d) = self;
        if d < Day::days_in(y, m) {
            Day(y, m, d + 1)
        } else if m < 12 {
            Day(y, m + 1, 1)
        } else {
            Day(y + 1, 1, 1)
        }
    }
}

impl std::fmt::Display for Day {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.0, self.1, self.2)
    }
}

fn print_human(m: &RenderedMove, opts: &MoveOptions) {
    println!(
        "move {} -> {} through {} ({} unit(s)):",
        opts.from,
        opts.to,
        opts.stage.root,
        m.units.len()
    );
    for u in &m.units {
        let what = match &u.partition {
            Some(p) => format!("{}={}", p.column, p.key),
            None => "whole".into(),
        };
        println!("  {:<24} {:<24} -> {}", u.model, what, u.location);
    }
    if !m.not_moved.is_empty() {
        println!("not moved:");
        for n in &m.not_moved {
            println!("  {:<24} {}", n.model, n.reason);
        }
    }
    for u in &m.units {
        let what = u.partition.as_ref().map(|p| format!(" {}={}", p.column, p.key)).unwrap_or_default();
        println!();
        println!("-- {}{what}: unload ({})", u.model, u.unload.engine);
        println!("{}", u.unload.script());
        println!("-- {}{what}: load ({})", u.model, u.load.engine);
        println!("{}", u.load.script());
        println!(
            "-- {}{what}: parity {} {} vs {} {}{}",
            u.model,
            u.source.engine,
            u.source.relation,
            u.target.engine,
            u.target.relation,
            u.source.predicate.as_ref().map(|p| format!(" where {p}")).unwrap_or_default()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_day_range_is_every_day_both_ends_included_across_month_and_leap_day() {
        let got = expand_partitions(&["2028-02-28..2028-03-01".into(), "eu".into(), "2028-02-29".into()]).unwrap();
        assert_eq!(got, ["2028-02-28", "2028-02-29", "2028-03-01", "eu"], "a repeated key moves once");
        assert_eq!(expand_partitions(&["2026-12-31..2027-01-01".into()]).unwrap(), ["2026-12-31", "2027-01-01"]);
        assert!(expand_partitions(&["2026-06-30..2026-06-01".into()]).unwrap_err().to_string().contains("ends before"));
        assert!(expand_partitions(&["2026-02-30..2026-03-01".into()]).unwrap_err().to_string().contains("YYYY-MM-DD"));
        assert!(expand_partitions(&["2000-01-01..2026-01-01".into()]).unwrap_err().to_string().contains("check the years"));
    }
}
