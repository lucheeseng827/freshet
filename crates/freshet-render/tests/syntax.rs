//! Every statement this crate renders, parsed by the matching `sqlparser` dialect.
//!
//! This is a *syntax* check, and a weaker one than execution: it proves the
//! statements are well-formed as far as `sqlparser` understands each dialect, not
//! that a warehouse accepts them or does the right thing. Postgres is executed
//! (`tests/postgres.rs`, against a live server), and so are Redshift's statements —
//! they use only syntax the two share, so Postgres checks what they do to rows.
//! Snowflake, BigQuery and Databricks are rendered and parsed here, and that is the
//! whole of the claim made for them.
//!
//! `sqlparser` 0.52 does not implement a handful of real vendor syntax. Those forms
//! are listed in [`KNOWN_PARSER_GAPS`] by the exact statement prefix, each with the
//! vendor grammar it is written against, so a gap is an explicit, reviewable entry —
//! never a statement that silently skipped the check. A rendered statement that
//! fails to parse and is not listed fails this test.

use freshet_core::{BackfillUnit, Column, Materialization, Model};
use freshet_incremental::replace::resolve;
use freshet_incremental::{PartitionType, ReplaceSpec, ReplaceStrategy};
use freshet_render::{render_model, Dialect, RenderOptions};
use sqlparser::dialect::{
    BigQueryDialect, DatabricksDialect, Dialect as SqlDialect, PostgreSqlDialect, RedshiftSqlDialect,
    SnowflakeDialect,
};
use sqlparser::parser::Parser;

/// `(dialect, statement prefix, why the statement is valid anyway)`.
const KNOWN_PARSER_GAPS: &[(Dialect, &str, &str)] = &[
    (
        Dialect::Postgres,
        "WITH freshet_deleted AS (",
        "data-modifying statements in WITH (PostgreSQL docs §7.8.4); executed for real in tests/postgres.rs",
    ),
    (
        Dialect::Snowflake,
        "INSERT OVERWRITE INTO ",
        "INSERT [ OVERWRITE ] INTO <target_table> [ ( <target_col_name> [ , ... ] ) ] <query> (Snowflake INSERT)",
    ),
    (
        Dialect::Snowflake,
        "CREATE OR REPLACE VIEW ",
        "CREATE [ OR REPLACE ] VIEW <name> … [ COPY GRANTS ] AS <select_statement> (Snowflake CREATE VIEW)",
    ),
    (
        Dialect::Databricks,
        "INSERT INTO ",
        "INSERT INTO [ TABLE ] table_name REPLACE WHERE predicate query (Databricks INSERT, Delta)",
    ),
];

fn parser_for(d: Dialect) -> Box<dyn SqlDialect> {
    match d {
        Dialect::Postgres => Box::new(PostgreSqlDialect {}),
        Dialect::Snowflake => Box::new(SnowflakeDialect),
        Dialect::BigQuery => Box::new(BigQueryDialect),
        Dialect::Databricks => Box::new(DatabricksDialect),
        Dialect::Redshift => Box::new(RedshiftSqlDialect {}),
    }
}

fn model(name: &str, materialization: Materialization, cols: &[&str], sql: &str) -> Model {
    Model {
        name: name.into(),
        sql: sql.into(),
        materialization,
        columns: cols.iter().map(|c| Column::new(*c, Vec::new())).collect(),
        depends_on: Vec::new(),
    }
}

/// Every write shape: both non-incremental materializations, the undeclared
/// incremental full refresh, a first build, and each strategy over partitions —
/// with and without a partition column for merge.
fn every_rendering(d: Dialect) -> Vec<(String, String)> {
    let events = model(
        "events",
        Materialization::Incremental,
        &["dt", "user_id", "n"],
        "select dt, user_id, n from raw_events where n > 0",
    );
    let table = model(
        "daily",
        Materialization::Table,
        &["dt", "total"],
        "select dt, sum(n) as total from events group by dt",
    );
    let view = model(
        "recent",
        Materialization::View,
        &["dt", "total"],
        "select dt, total from daily",
    );
    let unit = BackfillUnit::Partitions(vec!["2026-06-20".into(), "2026-06-21".into()]);
    let opts = RenderOptions::new(d);
    let mut new_opts = RenderOptions::new(d);
    new_opts.recreate.insert("events".into());

    let mut out = Vec::new();
    let mut push = |label: &str, r: freshet_render::RenderedSql| {
        for s in r.statements {
            out.push((label.to_string(), s));
        }
    };
    push(
        "table",
        render_model(&table, &BackfillUnit::FullModel, None, &opts).unwrap(),
    );
    push(
        "view",
        render_model(&view, &BackfillUnit::FullModel, None, &opts).unwrap(),
    );
    push(
        "incremental full refresh",
        render_model(&events, &BackfillUnit::FullModel, None, &opts).unwrap(),
    );
    push(
        "first build",
        render_model(&events, &BackfillUnit::FullModel, None, &new_opts).unwrap(),
    );
    for strategy in [
        ReplaceStrategy::InsertOverwrite,
        ReplaceStrategy::DeleteInsert,
        ReplaceStrategy::ReplaceWhere,
        ReplaceStrategy::Merge,
    ] {
        for ptype in [PartitionType::String, PartitionType::Date] {
            let spec = ReplaceSpec {
                strategy: Some(strategy),
                partition_column: Some("dt".into()),
                unique_key: vec!["user_id".into()],
                partition_type: Some(ptype),
            };
            let op = resolve(&events, Some(&spec), &unit).unwrap();
            push(
                &format!("{strategy:?}/{ptype:?}"),
                render_model(&events, &unit, Some(&op), &opts).unwrap(),
            );
        }
    }
    let unscoped = ReplaceSpec {
        strategy: Some(ReplaceStrategy::Merge),
        unique_key: vec!["user_id".into()],
        ..Default::default()
    };
    let op = resolve(&events, Some(&unscoped), &unit).unwrap();
    push(
        "merge without partition column",
        render_model(&events, &unit, Some(&op), &opts).unwrap(),
    );
    out
}

#[test]
fn every_rendered_statement_parses_or_is_a_listed_parser_gap() {
    let mut parsed = 0;
    let mut gaps_hit = std::collections::BTreeSet::new();
    for d in Dialect::ALL {
        let parser = parser_for(d);
        for (label, stmt) in every_rendering(d) {
            match Parser::parse_sql(parser.as_ref(), &stmt) {
                Ok(ast) => {
                    // One statement per entry — except a BigQuery transaction, which
                    // BigQuery runs only as one script, so it is one entry by design.
                    let expected = if d == Dialect::BigQuery && stmt.starts_with("BEGIN TRANSACTION;") {
                        4
                    } else {
                        1
                    };
                    assert_eq!(
                        ast.len(),
                        expected,
                        "{d}/{label}: statements in this entry, got {}:\n{stmt}",
                        ast.len()
                    );
                    parsed += 1;
                }
                Err(e) => {
                    let gap = KNOWN_PARSER_GAPS
                        .iter()
                        .position(|(gd, prefix, _)| *gd == d && stmt.starts_with(prefix));
                    let Some(i) = gap else {
                        panic!("{d}/{label}: rendered SQL does not parse and is not a listed parser gap: {e}\n{stmt}");
                    };
                    gaps_hit.insert(i);
                }
            }
        }
    }
    assert!(
        parsed > 40,
        "the check must be mostly parsing, not mostly exemptions: {parsed}"
    );
    // A listed gap that no longer fires is stale — sqlparser learned the syntax, or
    // the renderer stopped emitting it — and should be removed, not left to excuse
    // some future statement.
    for (i, (d, prefix, _)) in KNOWN_PARSER_GAPS.iter().enumerate() {
        assert!(
            gaps_hit.contains(&i),
            "listed parser gap for {d} `{prefix}` never fired; remove it"
        );
    }
}
