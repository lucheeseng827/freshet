//! Build a project straight from SQL strings and show column-aware pruning.
//!
//! Run: `cargo run -p freshet-sql --example from_sql`

use freshet_core::plan_backfill;
use freshet_sql::{build_project, Catalog};
use std::collections::{BTreeMap, BTreeSet};

fn main() -> anyhow::Result<()> {
    // Declared source schema (the only thing you hand-declare; the rest is derived).
    let sources: Catalog = [("raw".to_string(), vec!["a".to_string(), "b".to_string()])].into();

    let models = vec![
        ("stg".to_string(), "select a, b from raw".to_string()),
        ("mart_a".to_string(), "select a as x from stg".to_string()),
        ("mart_b".to_string(), "select b as y from stg".to_string()),
    ];

    let project = build_project(&models, &sources)?;
    println!("derived lineage:");
    for name in ["stg", "mart_a", "mart_b"] {
        let m = project.get(name).unwrap();
        println!("  {name}  depends_on={:?}", m.depends_on);
        for c in &m.columns {
            let ups: Vec<String> = c
                .upstream
                .iter()
                .map(|r| format!("{}.{}", r.model, r.column))
                .collect();
            println!("      {} ← {:?}", c.name, ups);
        }
    }

    // Only stg.a changed → mart_b (reads stg.b) is pruned.
    let only_a = BTreeMap::from([("stg".to_string(), BTreeSet::from(["a".to_string()]))]);
    let plan = plan_backfill(&project, &only_a);
    println!(
        "\nstg.a changed → backfill {:?}  (mart_b pruned)",
        plan.models
    );
    Ok(())
}
