//! Every statement a move renders, parsed by the matching `sqlparser` dialect: the
//! unload and the load on every side, DuckDB's by DuckDB's grammar.
//!
//! A syntax check, as in `tests/syntax.rs`: well-formed as far as `sqlparser`
//! understands each dialect, not accepted by a warehouse. The Postgres side is
//! executed for real (`tests/move_postgres.rs`, DuckDB and a live Postgres). The
//! rest is rendered from each vendor's documented grammar and parsed here, and that
//! is the whole of the claim made for it.
//!
//! Vendor syntax `sqlparser` does not implement is listed in [`KNOWN_PARSER_GAPS`]
//! by engine and statement prefix, with the grammar it is written against. A gap is
//! a reviewable entry, never a statement that silently skipped the check, and a
//! listed gap that stops firing fails the test until it is removed.

use freshet_core::{BackfillPlan, BackfillUnit, Column, Materialization, Model, Project};
use freshet_incremental::{IncrementalConfig, ModelPartitionConfig, PartitionType, ReplaceSpec, ReplaceStrategy};
use freshet_render::migrate::{render_move, MoveOptions};
use freshet_render::Dialect;
use sqlparser::dialect::{
    BigQueryDialect, DatabricksDialect, Dialect as SqlDialect, DuckDbDialect, RedshiftSqlDialect, SnowflakeDialect,
};
use sqlparser::parser::Parser;

/// `(engine, statement prefix, the vendor grammar it is written against)`.
const KNOWN_PARSER_GAPS: &[(&str, &str, &str)] = &[
    (
        "duckdb",
        "COPY (",
        "COPY (query) TO 'directory' (FORMAT parquet, PER_THREAD_OUTPUT true, OVERWRITE true) (DuckDB COPY, \
         options); executed for real in tests/move_postgres.rs",
    ),
    (
        "redshift",
        "UNLOAD ('",
        "UNLOAD ('select-statement') TO 's3://object-path/name-prefix' authorization FORMAT AS PARQUET CLEANPATH \
         (Amazon Redshift UNLOAD; the query is a string literal, which sqlparser's Athena-style UNLOAD does not take)",
    ),
    (
        "redshift",
        "COPY ",
        "COPY table_name FROM 's3://…' IAM_ROLE { default | 'arn' } FORMAT AS PARQUET (Amazon Redshift COPY)",
    ),
    (
        "snowflake",
        "COPY INTO '",
        "COPY INTO externalLocation FROM ( <query> ) STORAGE_INTEGRATION = … FILE_FORMAT = ( TYPE = PARQUET ) \
         HEADER = TRUE OVERWRITE = TRUE SINGLE = TRUE MAX_FILE_SIZE = … (Snowflake COPY INTO <location>)",
    ),
    (
        "snowflake",
        "COPY INTO ",
        "COPY INTO <table_name> FROM externalLocation STORAGE_INTEGRATION = … FILE_FORMAT = ( TYPE = PARQUET ) \
         MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE PATTERN = '…' FORCE = TRUE (Snowflake COPY INTO <table>)",
    ),
    (
        "bigquery",
        "EXPORT DATA ",
        "EXPORT DATA OPTIONS (uri = 'gs://…/*.parquet', format = 'PARQUET', overwrite = true) AS query (BigQuery EXPORT DATA)",
    ),
    (
        "bigquery",
        "LOAD DATA INTO TEMP TABLE ",
        "LOAD DATA INTO TEMP TABLE name FROM FILES (format = 'PARQUET', uris = […]) (BigQuery LOAD DATA), then a \
         transaction; one script, so the statements after it go unparsed too",
    ),
    (
        "databricks",
        "INSERT OVERWRITE DIRECTORY ",
        "INSERT OVERWRITE DIRECTORY directory_path USING PARQUET query (Databricks INSERT OVERWRITE DIRECTORY)",
    ),
    (
        "databricks",
        "INSERT INTO ",
        "INSERT INTO table_name REPLACE WHERE predicate query (Databricks INSERT, Delta)",
    ),
];

fn parser_for(engine: &str) -> Box<dyn SqlDialect> {
    match engine {
        "duckdb" => Box::new(DuckDbDialect {}),
        "snowflake" => Box::new(SnowflakeDialect),
        "bigquery" => Box::new(BigQueryDialect),
        "databricks" => Box::new(DatabricksDialect),
        "redshift" => Box::new(RedshiftSqlDialect {}),
        other => panic!("no parser for {other}"),
    }
}

fn model(name: &str, materialization: Materialization, cols: &[&str]) -> Model {
    Model {
        name: name.into(),
        sql: format!("select {} from raw", cols.join(", ")),
        materialization,
        columns: cols.iter().map(|c| Column::new(*c, Vec::new())).collect(),
        depends_on: Vec::new(),
    }
}

/// Every statement of a move between each pair of dialects, over every stage both
/// reach: a partitioned model by two keys, and a table moved whole.
fn every_move() -> Vec<(String, &'static str, String)> {
    let project = Project::from_models([
        model("events", Materialization::Incremental, &["dt", "user_id", "amount"]),
        model("user_dim", Materialization::Table, &["user_id", "name"]),
    ]);
    let mut cfg = IncrementalConfig::default();
    cfg.models.insert(
        "events".into(),
        ModelPartitionConfig {
            replace: Some(ReplaceSpec {
                strategy: Some(ReplaceStrategy::InsertOverwrite),
                partition_column: Some("dt".into()),
                unique_key: vec![],
                partition_type: Some(PartitionType::Date),
            }),
            ..Default::default()
        },
    );
    let mut plan = BackfillPlan { models: vec!["events".into(), "user_dim".into()], ..Default::default() };
    plan.unit.insert("events".into(), BackfillUnit::Partitions(vec!["2026-06-20".into(), "2026-06-21".into()]));

    let mut out = Vec::new();
    for from in Dialect::ALL {
        for to in Dialect::ALL {
            for stage in ["s3://acme-moves/m/", "gs://acme-moves/m/", "file:///srv/moves/"] {
                let opts = MoveOptions {
                    from,
                    to,
                    from_schema: Some("src".into()),
                    to_schema: Some("dst".into()),
                    stage: stage.parse().unwrap(),
                    storage_integration: Some("FRESHET_STAGE".into()),
                    iam_role: Some("arn:aws:iam::123456789012:role/freshet-move".into()),
                };
                // A stage one side cannot reach is refused, and that is tested in
                // the unit tests; here only what renders is parsed.
                let Ok(m) = render_move(&project, &plan, &cfg, &opts) else { continue };
                for u in m.units {
                    for (kind, step) in [("unload", &u.unload), ("load", &u.load)] {
                        for s in &step.statements {
                            out.push((format!("{from}->{to} {stage} {} {kind}", u.location), step.engine, s.clone()));
                        }
                    }
                }
            }
        }
    }
    out
}

#[test]
fn every_move_statement_parses_or_is_a_listed_parser_gap() {
    let mut parsed = 0;
    let mut gaps_hit = std::collections::BTreeSet::new();
    let mut failures = Vec::new();
    for (label, engine, stmt) in every_move() {
        match Parser::parse_sql(parser_for(engine).as_ref(), &stmt) {
            Ok(ast) => {
                // One statement per entry, except BigQuery's load: a transaction it
                // runs only as one script, so one entry by design.
                let expected = if engine == "bigquery" && stmt.starts_with("LOAD DATA") { 5 } else { 1 };
                assert_eq!(ast.len(), expected, "{label}: statements in this entry:\n{stmt}");
                parsed += 1;
            }
            Err(e) => match KNOWN_PARSER_GAPS.iter().position(|(g, prefix, _)| *g == engine && stmt.starts_with(prefix)) {
                Some(i) => {
                    gaps_hit.insert(i);
                }
                None => failures.push(format!("{label} [{engine}]: {e}\n{stmt}")),
            },
        }
    }
    assert!(failures.is_empty(), "{} statement(s) neither parse nor are a listed gap:\n\n{}", failures.len(), failures.join("\n\n"));
    assert!(parsed > 50, "the check must be mostly parsing, not mostly exemptions: {parsed}");
    for (i, (engine, prefix, _)) in KNOWN_PARSER_GAPS.iter().enumerate() {
        assert!(gaps_hit.contains(&i), "listed parser gap for {engine} `{prefix}` never fired; remove it");
    }
}
