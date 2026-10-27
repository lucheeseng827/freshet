//! Partition-scoped restatement: rebuild specific partitions and propagate the
//! scoped backfill downstream (identity-mapped through incremental models).
//!
//! Run: `cargo run -p freshet-api --example restate`

use freshet_api::*;

fn incr(name: &str, deps: &[&str], cols: Vec<Column>) -> Model {
    Model {
        name: name.into(),
        sql: format!("select * from {name}"),
        materialization: Materialization::Incremental, // partition-scopable
        columns: cols,
        depends_on: deps.iter().map(|s| s.to_string()).collect(),
    }
}

fn main() -> anyhow::Result<()> {
    // events(incr) -> daily(incr) -> weekly(Table, can't be scoped)
    let project = Project::from_models([
        incr("events", &[], vec![Column::new("ts", vec![])]),
        incr(
            "daily",
            &["events"],
            vec![Column::new("ts", vec![ColumnRef::new("events", "ts")])],
        ),
        Model {
            materialization: Materialization::Table,
            ..incr(
                "weekly",
                &["daily"],
                vec![Column::new("ts", vec![ColumnRef::new("daily", "ts")])],
            )
        },
    ]);
    let planner = BackfillPlanner::new(project, MemoryStateBackend::default());

    // "Reprocess last two days of events."
    let plan = planner.plan_restatement("events", ["2026-06-20".into(), "2026-06-21".into()])?;
    for m in &plan.models {
        println!("{:<8} → {:?}", m, plan.unit[m]);
    }
    Ok(())
}
