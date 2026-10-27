//! Minimal end-to-end demo of the backfill planner.
//!
//!   raw ─► stg(a, b) ─► mart_a (reads stg.a)
//!                    └► mart_b (reads stg.b)
//!
//! Run: `cargo run -p freshet-api --example minimal`

use freshet_api::*;
use std::collections::{BTreeMap, BTreeSet};

fn col(name: &str, ups: &[(&str, &str)]) -> Column {
    Column::new(
        name,
        ups.iter().map(|(m, c)| ColumnRef::new(*m, *c)).collect(),
    )
}

fn model(name: &str, sql: &str, deps: &[&str], cols: Vec<Column>) -> Model {
    Model {
        name: name.into(),
        sql: sql.into(),
        materialization: Materialization::Table,
        columns: cols,
        depends_on: deps.iter().map(|s| s.to_string()).collect(),
    }
}

fn project(stg_sql: &str) -> Project {
    Project::from_models([
        model(
            "raw",
            "select 1 a, 2 b",
            &[],
            vec![col("a", &[]), col("b", &[])],
        ),
        model(
            "stg",
            stg_sql,
            &["raw"],
            vec![col("a", &[("raw", "a")]), col("b", &[("raw", "b")])],
        ),
        model(
            "mart_a",
            "select a as x from stg",
            &["stg"],
            vec![col("x", &[("stg", "a")])],
        ),
        model(
            "mart_b",
            "select b as y from stg",
            &["stg"],
            vec![col("y", &[("stg", "b")])],
        ),
    ])
}

fn main() -> anyhow::Result<()> {
    let backend = MemoryStateBackend::default();

    // First run: cold state → everything backfills, in dependency order.
    let p1 = BackfillPlanner::new(project("select a, b from raw"), &backend);
    let r1 = p1.plan()?;
    println!("cold run          → {:?}", r1.plan.models);
    p1.commit(&r1.next_snapshot)?;

    // No edits → nothing to do.
    let p2 = BackfillPlanner::new(project("select a, b from raw"), &backend);
    println!("no change         → {:?}", p2.plan()?.plan.models);

    // Edit stg. State detects stg changed; without a column-diff the API
    // conservatively treats ALL of stg's columns as changed, so both marts rebuild.
    let p3 = BackfillPlanner::new(project("select a + 0 as a, b from raw"), &backend);
    let r3 = p3.plan()?;
    println!("stg edited (API)  → {:?}", r3.plan.models);
    for m in &r3.plan.models {
        println!("    {m}: {:?}", r3.plan.reasons[m]);
    }

    // The differentiator: when the caller knows *which column* changed (the SQL
    // column-lineage seam, idea #9), freshet-core prunes the untouched branch.
    let proj = project("select a, b from raw");
    let only_a = BTreeMap::from([("stg".to_string(), BTreeSet::from(["a".to_string()]))]);
    let precise = plan_backfill(&proj, &only_a);
    println!(
        "stg.a only (core) → {:?}  ← mart_b pruned (reads stg.b)",
        precise.models
    );
    Ok(())
}
