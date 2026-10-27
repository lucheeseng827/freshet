use super::*;
use freshet_core::Column;
use freshet_incremental::replace::resolve;
use freshet_incremental::ReplaceSpec;

fn model(name: &str, materialization: Materialization, cols: &[&str], sql: &str) -> Model {
    Model {
        name: name.into(),
        sql: sql.into(),
        materialization,
        columns: cols.iter().map(|c| Column::new(*c, Vec::new())).collect(),
        depends_on: Vec::new(),
    }
}

fn events() -> Model {
    model(
        "events",
        Materialization::Incremental,
        &["dt", "user_id", "n"],
        "select dt, user_id, n from raw_events",
    )
}

fn parts(keys: &[&str]) -> BackfillUnit {
    BackfillUnit::Partitions(keys.iter().map(|k| k.to_string()).collect())
}

fn spec(strategy: ReplaceStrategy) -> ReplaceSpec {
    ReplaceSpec {
        strategy: Some(strategy),
        partition_column: Some("dt".into()),
        unique_key: vec!["user_id".into()],
        partition_type: Some(PartitionType::Date),
    }
}

/// Resolve `strategy` against `unit` exactly as the wire does, then render.
fn render(
    d: Dialect,
    m: &Model,
    strategy: Option<ReplaceStrategy>,
    unit: &BackfillUnit,
) -> Result<RenderedSql, RenderError> {
    let op = strategy.and_then(|s| resolve(m, Some(&spec(s)), unit));
    render_model(m, unit, op.as_ref(), &RenderOptions::new(d))
}

// --- golden SQL -------------------------------------------------------------
//
// Pinned byte-for-byte. These are the statements a warehouse will run, so a change
// to any of them must be a deliberate, reviewed diff — never a side effect.

#[test]
fn postgres_replaces_partitions_in_one_atomic_statement() {
    let r = render(
        Dialect::Postgres,
        &events(),
        Some(ReplaceStrategy::InsertOverwrite),
        &parts(&["2026-06-20"]),
    )
    .unwrap();
    assert_eq!(
        r.statements,
        [
            "WITH freshet_deleted AS (\n  DELETE FROM events WHERE dt IN (DATE '2026-06-20')\n)\n\
          INSERT INTO events (dt, user_id, n)\n\
          SELECT dt, user_id, n FROM (\nselect dt, user_id, n from raw_events\n) AS freshet_model\n\
          WHERE freshet_model.dt IN (DATE '2026-06-20')"
        ]
    );
    assert!(r.atomic);
}

#[test]
fn postgres_recreates_a_redefined_table_inside_its_own_transaction() {
    let daily = model(
        "daily",
        Materialization::Table,
        &["dt", "total"],
        "select dt, sum(n) as total from events group by dt",
    );
    let mut opts = RenderOptions::new(Dialect::Postgres);
    opts.recreate.insert("daily".into());
    let r = render_model(&daily, &BackfillUnit::FullModel, None, &opts).unwrap();
    assert_eq!(
        r.statements,
        [
            "BEGIN",
            "DROP TABLE IF EXISTS daily",
            "CREATE TABLE daily AS\nselect dt, sum(n) as total from events group by dt",
            "COMMIT",
        ]
    );
    assert!(
        r.atomic,
        "DDL is transactional in Postgres, so the swap is all-or-nothing"
    );
    assert!(
        !r.script().contains("CASCADE"),
        "CASCADE would drop dependent views, including ones the plan pruned and will never rebuild"
    );
}

#[test]
fn snowflake_replaces_partitions_inside_an_explicit_transaction() {
    let r = render(
        Dialect::Snowflake,
        &events(),
        Some(ReplaceStrategy::InsertOverwrite),
        &parts(&["2026-06-20"]),
    )
    .unwrap();
    assert_eq!(
        r.statements,
        [
            "BEGIN TRANSACTION".to_string(),
            "DELETE FROM events WHERE dt IN (DATE '2026-06-20')".to_string(),
            "INSERT INTO events (dt, user_id, n)\nSELECT dt, user_id, n FROM (\nselect dt, user_id, n from raw_events\n) \
             AS freshet_model\nWHERE freshet_model.dt IN (DATE '2026-06-20')"
                .to_string(),
            "COMMIT".to_string(),
        ]
    );
    assert!(r.atomic);
}

#[test]
fn bigquery_replaces_partitions_with_one_merge_on_false() {
    let r = render(
        Dialect::BigQuery,
        &events(),
        Some(ReplaceStrategy::InsertOverwrite),
        &parts(&["2026-06-20"]),
    )
    .unwrap();
    assert_eq!(
        r.statements,
        ["MERGE INTO events AS freshet_tgt\nUSING (\nSELECT * FROM (\nselect dt, user_id, n from raw_events\n) AS freshet_model\n\
          WHERE freshet_model.dt IN (DATE '2026-06-20')\n) AS freshet_src\nON FALSE\n\
          WHEN NOT MATCHED BY SOURCE AND freshet_tgt.dt IN (DATE '2026-06-20') THEN DELETE\n\
          WHEN NOT MATCHED THEN INSERT (dt, user_id, n) VALUES (freshet_src.dt, freshet_src.user_id, freshet_src.n)"]
    );
    assert!(r.atomic);
}

#[test]
fn databricks_replaces_partitions_with_replace_where() {
    let r = render(
        Dialect::Databricks,
        &events(),
        Some(ReplaceStrategy::ReplaceWhere),
        &parts(&["2026-06-20"]),
    )
    .unwrap();
    assert_eq!(
        r.statements,
        ["INSERT INTO events REPLACE WHERE dt IN (DATE '2026-06-20')\nSELECT * FROM (\nselect dt, user_id, n from raw_events\n) \
          AS freshet_model\nWHERE freshet_model.dt IN (DATE '2026-06-20')"]
    );
    assert!(r.atomic);
}

#[test]
fn a_merge_updates_every_non_key_column_and_inserts_all_of_them() {
    let r = render(
        Dialect::Postgres,
        &events(),
        Some(ReplaceStrategy::Merge),
        &parts(&["2026-06-20"]),
    )
    .unwrap();
    assert_eq!(
        r.statements,
        ["MERGE INTO events AS freshet_tgt\nUSING (\nSELECT * FROM (\nselect dt, user_id, n from raw_events\n) AS freshet_model\n\
          WHERE freshet_model.dt IN (DATE '2026-06-20')\n) AS freshet_src\nON freshet_tgt.user_id = freshet_src.user_id\n\
          WHEN MATCHED THEN UPDATE SET dt = freshet_src.dt, n = freshet_src.n\n\
          WHEN NOT MATCHED THEN INSERT (dt, user_id, n) VALUES (freshet_src.dt, freshet_src.user_id, freshet_src.n)"]
    );
}

// --- strategy semantics ----------------------------------------------------

#[test]
fn the_three_partition_replaces_render_alike_where_a_dialect_has_one_atomic_form() {
    // Postgres and Snowflake each have exactly one way to replace a partition's rows
    // atomically, so the strategy names are three spellings of one write there.
    for d in [Dialect::Postgres, Dialect::Snowflake] {
        let io = render(
            d,
            &events(),
            Some(ReplaceStrategy::InsertOverwrite),
            &parts(&["2026-06-20"]),
        )
        .unwrap();
        let di = render(
            d,
            &events(),
            Some(ReplaceStrategy::DeleteInsert),
            &parts(&["2026-06-20"]),
        )
        .unwrap();
        let rw = render(
            d,
            &events(),
            Some(ReplaceStrategy::ReplaceWhere),
            &parts(&["2026-06-20"]),
        )
        .unwrap();
        assert_eq!(io, di, "{d}");
        assert_eq!(io, rw, "{d}");
    }
}

#[test]
fn delete_insert_is_honoured_literally_where_the_dialect_distinguishes_it() {
    let bq = render(
        Dialect::BigQuery,
        &events(),
        Some(ReplaceStrategy::DeleteInsert),
        &parts(&["2026-06-20"]),
    )
    .unwrap();
    // One entry: BigQuery runs a multi-statement transaction only as one script, so
    // an executor sending each entry as its own job must get the whole of it at once.
    assert_eq!(bq.statements.len(), 1, "{:?}", bq.statements);
    let script = &bq.statements[0];
    assert!(script.starts_with("BEGIN TRANSACTION;\nDELETE FROM "), "{script}");
    assert!(script.contains(";\nINSERT INTO "), "{script}");
    assert!(script.ends_with(";\nCOMMIT TRANSACTION"), "{script}");
    assert!(bq.atomic);

    let dbx = render(
        Dialect::Databricks,
        &events(),
        Some(ReplaceStrategy::DeleteInsert),
        &parts(&["2026-06-20"]),
    )
    .unwrap();
    assert_eq!(
        dbx.statements.len(),
        2,
        "a delete and an insert, and nothing wrapping them"
    );
    assert!(dbx.statements[0].starts_with("DELETE FROM events WHERE"));
    assert!(
        !dbx.atomic,
        "Delta has no multi-statement transactions — a failed insert leaves the partitions \
         deleted, and a caller must be told rather than find out"
    );
}

#[test]
fn a_merge_with_no_partition_column_merges_the_whole_model_output() {
    let s = ReplaceSpec {
        strategy: Some(ReplaceStrategy::Merge),
        unique_key: vec!["user_id".into()],
        ..Default::default()
    };
    let unit = parts(&["2026-06-20"]);
    let op = resolve(&events(), Some(&s), &unit).unwrap();
    let r = render_model(
        &events(),
        &unit,
        Some(&op),
        &RenderOptions::new(Dialect::Snowflake),
    )
    .unwrap();
    let stmt = &r.statements[0];
    assert!(
        stmt.contains("USING (\nselect dt, user_id, n from raw_events\n) AS freshet_src"),
        "the partitions cannot bound a source with no partition column — merge all of it, \
         which is a keyed superset and never a different answer:\n{stmt}"
    );
    assert!(!stmt.contains("WHERE"), "{stmt}");
}

#[test]
fn a_merge_of_only_key_columns_has_no_update_clause() {
    let keys_only = model(
        "user_ids",
        Materialization::Incremental,
        &["user_id"],
        "select distinct user_id from raw_events",
    );
    let s = ReplaceSpec {
        strategy: Some(ReplaceStrategy::Merge),
        unique_key: vec!["user_id".into()],
        ..Default::default()
    };
    let unit = parts(&["p1"]);
    let op = resolve(&keys_only, Some(&s), &unit).unwrap();
    for d in Dialect::ALL {
        let r = render_model(&keys_only, &unit, Some(&op), &RenderOptions::new(d)).unwrap();
        if d == Dialect::Redshift {
            // Redshift's upsert is a keyed delete + insert: nothing to update either way.
            assert!(!r.script().contains("UPDATE"), "{d}");
            assert!(r.script().contains("INSERT INTO user_ids (user_id)"), "{d}");
            continue;
        }
        assert!(
            !r.statements[0].contains("WHEN MATCHED"),
            "{d}: an empty SET list is a syntax error everywhere"
        );
        assert!(
            r.statements[0].contains("WHEN NOT MATCHED THEN INSERT (user_id)"),
            "{d}"
        );
    }
}

// --- materializations and first builds --------------------------------------

#[test]
fn a_table_whose_definition_did_not_change_keeps_its_relation_and_replaces_rows() {
    // Rule 5: an upstream changed, this table's SQL did not, so its columns did not
    // either — and its grants, dependent views and physical layout all survive.
    let daily = model(
        "daily",
        Materialization::Table,
        &["dt", "total"],
        "select dt, sum(n) as total from events group by dt",
    );
    let pg = render(Dialect::Postgres, &daily, None, &BackfillUnit::FullModel).unwrap();
    assert_eq!(
        pg.statements,
        ["WITH freshet_deleted AS (\n  DELETE FROM daily\n)\nINSERT INTO daily (dt, total)\n\
          SELECT dt, total FROM (\nselect dt, sum(n) as total from events group by dt\n) AS freshet_model"]
    );
    let sf = render(Dialect::Snowflake, &daily, None, &BackfillUnit::FullModel).unwrap();
    assert!(
        sf.statements[0].starts_with("INSERT OVERWRITE INTO daily (dt, total)"),
        "{}",
        sf.statements[0]
    );
}

#[test]
fn a_table_with_unnamed_columns_is_recreated_rather_than_refused() {
    // A named insert is impossible, and recreating is a table's natural form — unlike
    // an incremental relation, which is never recreated behind the operator's back.
    let t = model(
        "counts",
        Materialization::Table,
        &["dt", "_col1"],
        "select dt, count(*) from raw group by dt",
    );
    let r = render(Dialect::Snowflake, &t, None, &BackfillUnit::FullModel).unwrap();
    assert_eq!(r.statements, ["CREATE OR REPLACE TABLE counts COPY GRANTS AS\nselect dt, count(*) from raw group by dt"]);
}

#[test]
fn views_and_tables_are_rendered_from_their_materialization_without_a_declaration() {
    let v = model(
        "recent",
        Materialization::View,
        &["dt"],
        "select dt from daily",
    );
    for d in Dialect::ALL {
        let r = render(d, &v, None, &BackfillUnit::FullModel).unwrap();
        assert_eq!(r.statements.len(), 1, "{d}");
        assert!(
            r.statements[0].starts_with("CREATE OR REPLACE VIEW recent "),
            "{d}: {}",
            r.statements[0]
        );
    }
}

#[test]
fn a_partition_declaration_on_a_table_still_rebuilds_the_table_whole() {
    // Resolution widens it (NotIncremental); the renderer must not undo that.
    let t = model(
        "dim",
        Materialization::Table,
        &["dt", "id"],
        "select dt, id from raw",
    );
    let r = render(
        Dialect::BigQuery,
        &t,
        Some(ReplaceStrategy::InsertOverwrite),
        &parts(&["2026-06-20"]),
    )
    .unwrap();
    assert!(
        r.statements[0].contains("ON FALSE\nWHEN NOT MATCHED BY SOURCE THEN DELETE\n"),
        "whole relation: {}",
        r.statements[0]
    );
    assert!(
        !r.statements[0].contains("2026-06-20"),
        "no partition scoping survives the widening"
    );
}

#[test]
fn an_incremental_models_full_refresh_keeps_the_relation() {
    // Rule 5: partitioning, clustering and grants belong to someone else.
    let pg = render(Dialect::Postgres, &events(), None, &BackfillUnit::FullModel).unwrap();
    assert!(
        pg.statements[0].starts_with("WITH freshet_deleted AS (\n  DELETE FROM events\n)"),
        "{}",
        pg.statements[0]
    );
    let sf = render(
        Dialect::Snowflake,
        &events(),
        None,
        &BackfillUnit::FullModel,
    )
    .unwrap();
    assert!(
        sf.statements[0].starts_with("INSERT OVERWRITE INTO events (dt, user_id, n)"),
        "{}",
        sf.statements[0]
    );
    let bq = render(Dialect::BigQuery, &events(), None, &BackfillUnit::FullModel).unwrap();
    assert!(
        bq.statements[0].contains("ON FALSE\nWHEN NOT MATCHED BY SOURCE THEN DELETE"),
        "{}",
        bq.statements[0]
    );
}

#[test]
fn a_widened_op_renders_as_a_full_refresh_of_the_relation() {
    // A perfect declaration on a full-model plan widens to WholeModelInPlan.
    let op = resolve(
        &events(),
        Some(&spec(ReplaceStrategy::InsertOverwrite)),
        &BackfillUnit::FullModel,
    )
    .unwrap();
    assert!(op.is_widened());
    let r = render_model(
        &events(),
        &BackfillUnit::FullModel,
        Some(&op),
        &RenderOptions::new(Dialect::Postgres),
    )
    .unwrap();
    assert!(
        r.statements[0].contains("DELETE FROM events\n)"),
        "whole relation, no predicate: {}",
        r.statements[0]
    );
}

#[test]
fn a_model_the_warehouse_has_never_seen_is_created_not_written_into() {
    let mut opts = RenderOptions::new(Dialect::Snowflake);
    opts.recreate.insert("events".into());
    let r = render_model(&events(), &BackfillUnit::FullModel, None, &opts).unwrap();
    assert_eq!(
        r.statements,
        ["CREATE OR REPLACE TABLE events COPY GRANTS AS\nselect dt, user_id, n from raw_events"]
    );
}

// --- refusals ----------------------------------------------------------------

#[test]
fn a_partition_scoped_rebuild_with_no_declared_strategy_is_refused() {
    let err = render(
        Dialect::Postgres,
        &events(),
        None,
        &parts(&["2026-06-20", "2026-06-21"]),
    )
    .unwrap_err();
    assert_eq!(
        err,
        RenderError::UndeclaredPartitionWrite {
            model: "events".into(),
            partitions: 2
        }
    );
    let msg = err.to_string();
    assert!(
        msg.contains("incremental.json"),
        "the message must say where the fix goes: {msg}"
    );
}

#[test]
fn an_unaliased_expression_blocks_only_the_writes_that_must_name_columns() {
    let m = model(
        "agg",
        Materialization::Incremental,
        &["dt", "_col1"],
        "select dt, count(*) from raw group by dt",
    );
    let err = render(
        Dialect::Postgres,
        &m,
        Some(ReplaceStrategy::InsertOverwrite),
        &parts(&["2026-06-20"]),
    )
    .unwrap_err();
    assert_eq!(
        err,
        RenderError::UnnamedColumn {
            model: "agg".into(),
            column: "_col1".into()
        }
    );

    // Creating from the query needs no names — the warehouse picks them.
    let t = Model {
        materialization: Materialization::Table,
        ..m
    };
    assert!(render(Dialect::Postgres, &t, None, &BackfillUnit::FullModel).is_ok());
}

#[test]
fn a_partition_column_that_is_not_an_output_column_is_refused_as_a_typo() {
    let s = ReplaceSpec {
        partition_column: Some("ds".into()),
        ..spec(ReplaceStrategy::InsertOverwrite)
    };
    let unit = parts(&["2026-06-20"]);
    let op = resolve(&events(), Some(&s), &unit).unwrap();
    let err = render_model(
        &events(),
        &unit,
        Some(&op),
        &RenderOptions::new(Dialect::Postgres),
    )
    .unwrap_err();
    assert_eq!(
        err,
        RenderError::UnknownColumn {
            model: "events".into(),
            column: "ds".into(),
            role: "partition_column"
        }
    );
}

#[test]
fn keys_that_are_not_their_declared_type_are_refused() {
    let err = render(
        Dialect::Postgres,
        &events(),
        Some(ReplaceStrategy::InsertOverwrite),
        &parts(&["2026-02-30"]),
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            RenderError::BadPartitionKey {
                expected: PartitionType::Date,
                ..
            }
        ),
        "{err}"
    );

    let s = ReplaceSpec {
        partition_type: Some(PartitionType::Int),
        ..spec(ReplaceStrategy::InsertOverwrite)
    };
    let unit = parts(&["12x"]);
    let op = resolve(&events(), Some(&s), &unit).unwrap();
    let err = render_model(
        &events(),
        &unit,
        Some(&op),
        &RenderOptions::new(Dialect::Postgres),
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            RenderError::BadPartitionKey {
                expected: PartitionType::Int,
                ..
            }
        ),
        "{err}"
    );
}

#[test]
fn a_string_key_that_would_need_escaping_is_refused_in_every_dialect() {
    // `''` is an escaped quote in Postgres and two adjacent literals in Spark, which
    // concatenates them — the same key would match different rows per warehouse.
    let s = ReplaceSpec {
        partition_type: Some(PartitionType::String),
        ..spec(ReplaceStrategy::InsertOverwrite)
    };
    for key in ["it's", "a\\b", "x\ny", ""] {
        let unit = parts(&[key]);
        let op = resolve(&events(), Some(&s), &unit).unwrap();
        for d in Dialect::ALL {
            let err =
                render_model(&events(), &unit, Some(&op), &RenderOptions::new(d)).unwrap_err();
            assert!(
                matches!(err, RenderError::UnsafePartitionKey { .. }),
                "{d} {key:?}: {err}"
            );
        }
    }
}

#[test]
fn integer_keys_are_normalised_through_a_parse() {
    let s = ReplaceSpec {
        partition_type: Some(PartitionType::Int),
        ..spec(ReplaceStrategy::InsertOverwrite)
    };
    let unit = parts(&["007", "-3"]);
    let op = resolve(&events(), Some(&s), &unit).unwrap();
    let r = render_model(
        &events(),
        &unit,
        Some(&op),
        &RenderOptions::new(Dialect::Postgres),
    )
    .unwrap();
    assert!(
        r.statements[0].contains("dt IN (-3, 7)"),
        "{}",
        r.statements[0]
    );
}

// --- identifiers ---------------------------------------------------------------

#[test]
fn plain_identifiers_are_written_as_the_user_wrote_them() {
    // Quoting would make them case-sensitive in Postgres/Snowflake and point the
    // write at a different relation than the model's own SQL resolves.
    assert_eq!(
        ident(Dialect::Snowflake, "Events").as_deref(),
        Some("Events")
    );
    assert_eq!(
        ident(Dialect::Postgres, "stg_orders_v2").as_deref(),
        Some("stg_orders_v2")
    );
}

#[test]
fn identifiers_that_cannot_be_bare_are_quoted_in_the_dialects_style() {
    assert_eq!(
        ident(Dialect::Postgres, "my-table").as_deref(),
        Some("\"my-table\"")
    );
    assert_eq!(
        ident(Dialect::BigQuery, "my-project").as_deref(),
        Some("`my-project`")
    );
    assert_eq!(
        ident(Dialect::Databricks, "order").as_deref(),
        Some("`order`"),
        "reserved word"
    );
    assert_eq!(
        ident(Dialect::Snowflake, "2fast").as_deref(),
        Some("\"2fast\""),
        "leading digit"
    );
    for bad in ["a\"b", "a`b", "a\\b", "a\nb", ""] {
        assert_eq!(
            ident(Dialect::Postgres, bad),
            None,
            "{bad:?} cannot be quoted safely everywhere"
        );
    }
}

#[test]
fn a_schema_qualifies_every_target_and_may_itself_be_dotted() {
    let mut opts = RenderOptions::new(Dialect::BigQuery);
    opts.schema = Some("my-project.analytics".into());
    let r = render_model(&events(), &BackfillUnit::FullModel, None, &opts).unwrap();
    assert!(
        r.statements[0].starts_with("MERGE INTO `my-project`.analytics.events AS freshet_tgt"),
        "{}",
        r.statements[0]
    );
}

// --- the embedded model SQL ------------------------------------------------------

/// Render a table in its create form, which embeds the model's SQL unwrapped.
fn created(d: Dialect, m: &Model) -> RenderedSql {
    let mut opts = RenderOptions::new(d);
    opts.recreate.insert(m.name.clone());
    render_model(m, &BackfillUnit::FullModel, None, &opts).unwrap()
}

#[test]
fn trailing_semicolons_are_stripped_from_the_model() {
    let m = model(
        "daily",
        Materialization::Table,
        &["x"],
        "  select 1 as x ;; \n",
    );
    let r = created(Dialect::BigQuery, &m);
    assert_eq!(
        r.statements,
        ["CREATE OR REPLACE TABLE daily AS\nselect 1 as x"]
    );
}

#[test]
fn a_trailing_line_comment_can_never_swallow_what_follows_it() {
    let m = model(
        "daily",
        Materialization::Table,
        &["x"],
        "select 1 as x -- the answer",
    );
    let r = created(Dialect::Postgres, &m);
    assert!(
        r.script().contains("-- the answer\n;"),
        "the terminator must land on its own line:\n{}",
        r.script()
    );
    let inc = model(
        "events",
        Materialization::Incremental,
        &["dt"],
        "select dt from raw -- src",
    );
    let unit = parts(&["2026-06-20"]);
    let r = render(
        Dialect::Postgres,
        &inc,
        Some(ReplaceStrategy::InsertOverwrite),
        &unit,
    )
    .unwrap();
    assert!(
        r.statements[0].contains("-- src\n\n) AS freshet_model"),
        "{}",
        r.statements[0]
    );
}

#[test]
fn an_empty_model_is_refused() {
    let m = model("nothing", Materialization::Table, &[], " ; ");
    assert_eq!(
        render(Dialect::Postgres, &m, None, &BackfillUnit::FullModel).unwrap_err(),
        RenderError::EmptySql {
            model: "nothing".into()
        }
    );
}

// --- the safety property --------------------------------------------------------

/// Rule 1, over every combination: whatever the dialect, strategy and key set, a
/// partition-scoped write names **every** key the plan asked for on **both** sides —
/// the target rows it removes and the model rows it inserts. A key present on only
/// one side is a stale-row or duplicate-row bug that no downstream check would see.
#[test]
fn no_rendering_ever_drops_a_partition_from_either_side_of_the_write() {
    let key_sets: [&[&str]; 3] = [
        &["2026-06-20"],
        &["2026-06-20", "2026-06-21", "2026-07-01"],
        &["2024-02-29"],
    ];
    let mut checked = 0;
    for d in Dialect::ALL {
        for strategy in [
            ReplaceStrategy::InsertOverwrite,
            ReplaceStrategy::DeleteInsert,
            ReplaceStrategy::ReplaceWhere,
        ] {
            for keys in key_sets {
                let r = render(d, &events(), Some(strategy), &parts(keys)).unwrap();
                let script = r.script();
                for k in keys {
                    let lit = format!("DATE '{k}'");
                    let hits = script.matches(&lit).count();
                    assert!(
                        hits >= 2,
                        "{d}/{strategy:?}: {lit} appears {hits} time(s) — the delete and the insert \
                         must both name it:\n{script}"
                    );
                }
                checked += 1;
            }
        }
        // A scoped merge filters the source by every key.
        let r = render(
            d,
            &events(),
            Some(ReplaceStrategy::Merge),
            &parts(&["2026-06-20", "2026-06-21"]),
        )
        .unwrap();
        for k in ["2026-06-20", "2026-06-21"] {
            assert!(
                r.script().contains(&format!("DATE '{k}'")),
                "{d}/merge dropped {k}"
            );
        }
    }
    assert_eq!(checked, Dialect::ALL.len() * 9);
}

// --- Redshift -------------------------------------------------------------------

#[test]
fn redshift_replaces_partitions_in_one_transaction_with_named_columns() {
    let r = render(
        Dialect::Redshift,
        &events(),
        Some(ReplaceStrategy::InsertOverwrite),
        &parts(&["2026-06-20"]),
    )
    .unwrap();
    assert_eq!(r.statements.first().map(String::as_str), Some("BEGIN"));
    assert_eq!(r.statements.last().map(String::as_str), Some("COMMIT"));
    assert!(r.atomic, "a Redshift transaction covers the delete and the insert");
    assert_eq!(r.statements[1], "DELETE FROM events WHERE dt IN (DATE '2026-06-20')");
    assert!(r.statements[2].starts_with("INSERT INTO events (dt, user_id, n)"), "{}", r.statements[2]);
    assert!(
        r.statements[2].ends_with("WHERE freshet_model.dt IN (DATE '2026-06-20')"),
        "{}",
        r.statements[2]
    );
    assert!(!r.script().contains("WITH freshet_deleted"), "no data-modifying CTE on Redshift");
}

#[test]
fn redshift_never_truncates_which_would_commit_the_transaction() {
    let t = model("daily", Materialization::Table, &["d", "n"], "select d, n from raw");
    let r = render(Dialect::Redshift, &t, None, &BackfillUnit::FullModel).unwrap();
    assert!(!r.script().to_ascii_uppercase().contains("TRUNCATE"), "{}", r.script());
    assert_eq!(r.statements[1], "DELETE FROM daily");
    assert!(r.atomic);
}

#[test]
fn redshift_merges_by_deleting_matched_keys_then_inserting() {
    let s = ReplaceSpec {
        strategy: Some(ReplaceStrategy::Merge),
        unique_key: vec!["user_id".into()],
        partition_column: Some("dt".into()),
        partition_type: Some(PartitionType::Date),
    };
    let unit = parts(&["2026-06-20"]);
    let op = resolve(&events(), Some(&s), &unit).unwrap();
    let r = render_model(&events(), &unit, Some(&op), &RenderOptions::new(Dialect::Redshift)).unwrap();
    let script = r.script();
    assert!(!script.contains("MERGE"), "Redshift's MERGE takes only a table as its source:\n{script}");
    assert!(r.statements[1].starts_with("DELETE FROM events\nWHERE EXISTS ("), "{script}");
    assert!(r.statements[1].contains("events.user_id = freshet_src.user_id"), "{script}");
    assert!(r.statements[2].starts_with("INSERT INTO events (dt, user_id, n)"), "{script}");
    // Both statements read only the scoped partitions.
    assert_eq!(script.matches("freshet_model.dt IN (DATE '2026-06-20')").count(), 2, "{script}");
}

#[test]
fn render_plan_reports_every_failing_model_at_once() {
    let project = Project::from_models([
        events(),
        model(
            "other",
            Materialization::Incremental,
            &["dt"],
            "select dt from raw",
        ),
        model(
            "fine",
            Materialization::Table,
            &["dt"],
            "select dt from raw",
        ),
    ]);
    let plan = BackfillPlan {
        models: vec!["events".into(), "other".into(), "fine".into()],
        reasons: BTreeMap::new(),
        unit: BTreeMap::from([
            ("events".to_string(), parts(&["2026-06-20"])),
            ("other".to_string(), parts(&["2026-06-20"])),
        ]),
    };
    let errs = render_plan(
        &project,
        &plan,
        &IncrementalConfig::default(),
        &RenderOptions::new(Dialect::Postgres),
    )
    .unwrap_err();
    assert_eq!(
        errs.0.len(),
        2,
        "both undeclared incrementals, not just the first: {errs}"
    );
    assert!(errs.to_string().contains("`events`") && errs.to_string().contains("`other`"));
}

#[test]
fn render_plan_resolves_declarations_from_the_config_like_the_wire_does() {
    let project = Project::from_models([events()]);
    let cfg = IncrementalConfig {
        models: BTreeMap::from([(
            "events".to_string(),
            freshet_incremental::ModelPartitionConfig {
                replace: Some(spec(ReplaceStrategy::Merge)),
                ..Default::default()
            },
        )]),
    };
    let plan = BackfillPlan {
        models: vec!["events".into()],
        reasons: BTreeMap::new(),
        unit: BTreeMap::from([("events".to_string(), parts(&["2026-06-20"]))]),
    };
    let out = render_plan(
        &project,
        &plan,
        &cfg,
        &RenderOptions::new(Dialect::Databricks),
    )
    .unwrap();
    assert!(out["events"].statements[0].starts_with("MERGE INTO events AS freshet_tgt"));
}

#[test]
fn dialect_names_parse_case_insensitively_and_unknown_ones_list_the_choices() {
    assert_eq!("PostgreSQL".parse::<Dialect>(), Ok(Dialect::Postgres));
    assert_eq!(" bigquery ".parse::<Dialect>(), Ok(Dialect::BigQuery));
    let err = "oracle".parse::<Dialect>().unwrap_err().to_string();
    assert!(
        err.contains("postgres, snowflake, bigquery, databricks"),
        "{err}"
    );
    for d in Dialect::ALL {
        assert_eq!(
            d.as_str().parse::<Dialect>(),
            Ok(d),
            "as_str must round-trip"
        );
    }
}

// --- recreating, with a plan around it ---------------------------------------------

fn chain_project() -> Project {
    // raw_events -> events (incremental) -> daily (table) -> recent (view) -> top (view)
    let mut events = events();
    events.depends_on = vec![];
    let mut daily = model(
        "daily",
        Materialization::Table,
        &["dt", "total"],
        "select dt, sum(n) as total from events group by dt",
    );
    daily.depends_on = vec!["events".into()];
    let mut recent = model(
        "recent",
        Materialization::View,
        &["dt", "total"],
        "select dt, total from daily",
    );
    recent.depends_on = vec!["daily".into()];
    let mut top = model(
        "top",
        Materialization::View,
        &["dt"],
        "select dt from recent where total > 10",
    );
    top.depends_on = vec!["recent".into()];
    Project::from_models([events, daily, recent, top])
}

fn plan_of(models: &[&str]) -> BackfillPlan {
    BackfillPlan {
        models: models.iter().map(|m| m.to_string()).collect(),
        reasons: BTreeMap::new(),
        unit: BTreeMap::new(),
    }
}

#[test]
fn postgres_drops_the_views_the_plan_rebuilds_before_recreating_their_table() {
    let mut opts = RenderOptions::new(Dialect::Postgres);
    opts.recreate.insert("daily".into());
    let out = render_plan(
        &chain_project(),
        &plan_of(&["daily", "recent", "top"]),
        &IncrementalConfig::default(),
        &opts,
    )
    .unwrap();
    assert_eq!(
        out["daily"].statements,
        [
            "BEGIN",
            "DROP VIEW IF EXISTS top, recent",
            "DROP TABLE IF EXISTS daily",
            "CREATE TABLE daily AS\nselect dt, sum(n) as total from events group by dt",
            "COMMIT",
        ],
        "both views bind to the table (one through the other); dependents are listed first"
    );
    // …and each comes back in its own step, later in the same run.
    assert_eq!(
        out["recent"].statements,
        ["CREATE OR REPLACE VIEW recent AS\nselect dt, total from daily"]
    );
}

#[test]
fn a_view_the_plan_does_not_rebuild_is_never_dropped() {
    // `top` is pruned from this plan. Dropping it would lose it; leaving it makes the
    // DROP TABLE fail loudly, which is the point.
    let mut opts = RenderOptions::new(Dialect::Postgres);
    opts.recreate.insert("daily".into());
    let out = render_plan(
        &chain_project(),
        &plan_of(&["daily", "recent"]),
        &IncrementalConfig::default(),
        &opts,
    )
    .unwrap();
    assert_eq!(out["daily"].statements[1], "DROP VIEW IF EXISTS recent");
    assert!(
        !out["daily"].script().contains("top"),
        "{}",
        out["daily"].script()
    );
    assert!(!out["daily"].script().contains("CASCADE"));
}

#[test]
fn other_dialects_leave_late_binding_views_alone() {
    for d in [Dialect::Snowflake, Dialect::BigQuery, Dialect::Databricks] {
        let mut opts = RenderOptions::new(d);
        opts.recreate.insert("daily".into());
        let out = render_plan(
            &chain_project(),
            &plan_of(&["daily", "recent", "top"]),
            &IncrementalConfig::default(),
            &opts,
        )
        .unwrap();
        assert!(
            !out["daily"].script().contains("DROP VIEW"),
            "{d}: {}",
            out["daily"].script()
        );
    }
}

#[test]
fn postgres_recreates_a_redefined_view_by_drop_and_create() {
    // CREATE OR REPLACE VIEW cannot drop or rename a column in Postgres.
    let mut opts = RenderOptions::new(Dialect::Postgres);
    opts.recreate.insert("recent".into());
    let out = render_plan(
        &chain_project(),
        &plan_of(&["recent", "top"]),
        &IncrementalConfig::default(),
        &opts,
    )
    .unwrap();
    assert_eq!(
        out["recent"].statements,
        [
            "BEGIN",
            "DROP VIEW IF EXISTS top, recent",
            "CREATE VIEW recent AS\nselect dt, total from daily",
            "COMMIT"
        ]
    );
}

#[test]
fn recreating_an_incremental_model_keeps_its_declared_partitioning_where_ddl_can_say_so() {
    let cfg = IncrementalConfig {
        models: BTreeMap::from([(
            "events".to_string(),
            freshet_incremental::ModelPartitionConfig {
                replace: Some(spec(ReplaceStrategy::InsertOverwrite)),
                ..Default::default()
            },
        )]),
    };
    let project = Project::from_models([events()]);
    let plan = plan_of(&["events"]);
    let render_in = |d: Dialect| {
        let mut opts = RenderOptions::new(d);
        opts.recreate.insert("events".into());
        render_plan(&project, &plan, &cfg, &opts)
            .unwrap()
            .remove("events")
            .unwrap()
            .statements
    };
    assert_eq!(
        render_in(Dialect::BigQuery),
        ["CREATE OR REPLACE TABLE events\nPARTITION BY dt AS\nselect dt, user_id, n from raw_events"],
        "the widened op lost its partition column; the declaration did not"
    );
    assert_eq!(
        render_in(Dialect::Databricks),
        ["CREATE OR REPLACE TABLE events\nPARTITIONED BY (dt) AS\nselect dt, user_id, n from raw_events"]
    );
    assert_eq!(
        render_in(Dialect::Snowflake),
        ["CREATE OR REPLACE TABLE events COPY GRANTS AS\nselect dt, user_id, n from raw_events"]
    );
}

#[test]
fn a_string_partition_column_is_not_declared_as_bigquery_partitioning() {
    // BigQuery partitions by dates (or integer ranges it would need bounds for), so a
    // string column is left to the table as it is rather than a DDL error.
    let s = ReplaceSpec {
        partition_type: Some(PartitionType::String),
        ..spec(ReplaceStrategy::InsertOverwrite)
    };
    let cfg = IncrementalConfig {
        models: BTreeMap::from([(
            "events".to_string(),
            freshet_incremental::ModelPartitionConfig {
                replace: Some(s),
                ..Default::default()
            },
        )]),
    };
    let mut opts = RenderOptions::new(Dialect::BigQuery);
    opts.recreate.insert("events".into());
    let out = render_plan(
        &Project::from_models([events()]),
        &plan_of(&["events"]),
        &cfg,
        &opts,
    )
    .unwrap();
    assert!(
        !out["events"].statements[0].contains("PARTITION BY"),
        "{}",
        out["events"].statements[0]
    );
}

#[test]
fn a_restatement_never_recreates_what_it_restates() {
    // The seed of a restatement is "directly changed" in the plan's vocabulary, but its
    // definition did not move; with `recreate` empty (as a restatement passes it) the
    // write stays partition-scoped.
    let r = render(
        Dialect::Postgres,
        &events(),
        Some(ReplaceStrategy::InsertOverwrite),
        &parts(&["2026-06-20"]),
    )
    .unwrap();
    assert!(!r.script().contains("DROP"), "{}", r.script());
    assert!(r.script().contains("DATE '2026-06-20'"));
}
