//! Rendered SQL, executed against a live PostgreSQL.
//!
//! The syntax check in `tests/syntax.rs` proves statements are well-formed; this
//! proves they do the right thing to real rows — that a restatement replaces the
//! partitions it names and **only** those, that a merge upserts only the scoped
//! rows, that a full refresh keeps the relation, and that recreating a table brings
//! its dependent views back rather than losing them.
//!
//! ```sh
//! FRESHET_PG_URL=postgresql://postgres@127.0.0.1:5432/postgres \
//!   cargo test -p freshet-render --test postgres -- --ignored
//! ```
//!
//! Needs `psql` on `PATH` (the harness shells out rather than adding a driver, so the
//! renderer's dependency graph stays empty). Each test works in its own schema and
//! drops it afterwards. Statements run the way the contract says they must: in order,
//! on one session — one `psql` process, one `-c` per statement.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;

use freshet_core::{BackfillPlan, BackfillUnit, Column, Materialization, Model, Project};
use freshet_incremental::replace::resolve;
use freshet_incremental::{IncrementalConfig, PartitionType, ReplaceSpec, ReplaceStrategy};
use freshet_render::{render_model, render_plan, Dialect, RenderOptions, RenderedSql};

/// One schema on a live server, dropped on drop.
struct Pg {
    url: String,
    schema: String,
}

impl Pg {
    fn new(test: &str) -> Pg {
        let url = std::env::var("FRESHET_PG_URL")
            .expect("set FRESHET_PG_URL to run the live Postgres tests");
        let schema = format!("freshet_{test}_{}", std::process::id());
        let pg = Pg { url, schema };
        pg.raw(&[
            format!("DROP SCHEMA IF EXISTS {} CASCADE", pg.schema),
            format!("CREATE SCHEMA {}", pg.schema),
        ])
        .expect("creating the test schema");
        pg
    }

    /// Run statements in one session, in order, stopping at the first error.
    fn raw(&self, statements: &[String]) -> Result<String, String> {
        let mut cmd = Command::new("psql");
        cmd.args([
            self.url.as_str(),
            "-X",
            "-q",
            "-A",
            "-t",
            "-v",
            "ON_ERROR_STOP=1",
        ]);
        for s in statements {
            cmd.arg("-c").arg(s);
        }
        let out = cmd.output().expect("running psql (is it on PATH?)");
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).into_owned())
        } else {
            Err(String::from_utf8_lossy(&out.stderr).into_owned())
        }
    }

    /// Run statements with this test's schema first on the search path.
    fn exec(&self, statements: &[String]) -> Result<String, String> {
        let mut all = vec![format!("SET search_path TO {}", self.schema)];
        all.extend(statements.iter().cloned());
        self.raw(&all)
    }

    fn run(&self, sql: &RenderedSql) {
        if let Err(e) = self.exec(&sql.statements) {
            panic!("rendered SQL failed on Postgres:\n{}\n\n{e}", sql.script());
        }
    }

    fn sql(&self, statements: &[&str]) {
        let owned: Vec<String> = statements.iter().map(|s| s.to_string()).collect();
        self.exec(&owned)
            .unwrap_or_else(|e| panic!("{statements:?}: {e}"));
    }

    /// Rows of a query as `|`-joined strings, in the query's order.
    fn rows(&self, query: &str) -> Vec<String> {
        self.exec(&[query.to_string()])
            .unwrap_or_else(|e| panic!("{query}: {e}"))
            .lines()
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect()
    }
}

impl Drop for Pg {
    fn drop(&mut self) {
        let _ = self.raw(&[format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema)]);
    }
}

fn model(
    name: &str,
    materialization: Materialization,
    cols: &[&str],
    deps: &[&str],
    sql: &str,
) -> Model {
    Model {
        name: name.into(),
        sql: sql.into(),
        materialization,
        columns: cols.iter().map(|c| Column::new(*c, Vec::new())).collect(),
        depends_on: deps.iter().map(|d| d.to_string()).collect(),
    }
}

fn events() -> Model {
    model(
        "events",
        Materialization::Incremental,
        &["dt", "user_id", "n"],
        &[],
        "select dt, user_id, n from raw_events",
    )
}

fn created(m: &Model) -> RenderedSql {
    created_in(Dialect::Postgres, m)
}

fn created_in(d: Dialect, m: &Model) -> RenderedSql {
    let mut opts = RenderOptions::new(d);
    opts.recreate.insert(m.name.clone());
    render_model(m, &BackfillUnit::FullModel, None, &opts).unwrap()
}

/// Redshift speaks the Postgres wire but not every Postgres form, so its renderings
/// use only syntax the two share — and Postgres can check what they do to rows.
const EXECUTED: [Dialect; 2] = [Dialect::Postgres, Dialect::Redshift];

fn seed_raw_events(pg: &Pg) {
    pg.sql(&[
        "CREATE TABLE raw_events (dt date, user_id int, n int)",
        "INSERT INTO raw_events VALUES ('2026-06-19',1,1),('2026-06-19',2,1),('2026-06-20',1,1),('2026-06-20',2,1),('2026-06-21',1,1)",
    ]);
}

#[test]
#[ignore = "needs a live PostgreSQL 15+ (FRESHET_PG_URL) and psql on PATH"]
fn a_restatement_replaces_exactly_the_partitions_it_names() {
    for (d, strategy) in EXECUTED.into_iter().flat_map(|d| {
        [
            ReplaceStrategy::InsertOverwrite,
            ReplaceStrategy::DeleteInsert,
            ReplaceStrategy::ReplaceWhere,
        ]
        .map(|s| (d, s))
    }) {
        let pg = Pg::new(&format!("restate_{d}_{strategy:?}").to_lowercase());
        seed_raw_events(&pg);
        pg.run(&created_in(d, &events()));
        assert_eq!(pg.rows("SELECT count(*) FROM events"), ["5"]);

        // Mark every row, so a row this write should not have touched is visible.
        pg.sql(&["UPDATE events SET n = 99"]);
        // Change the source inside 06-20: an update, an insert and a delete.
        pg.sql(&[
            "UPDATE raw_events SET n = 5 WHERE dt = '2026-06-20'",
            "INSERT INTO raw_events VALUES ('2026-06-20',3,7)",
            "DELETE FROM raw_events WHERE dt = '2026-06-20' AND user_id = 2",
        ]);

        let unit = BackfillUnit::Partitions(vec!["2026-06-20".into()]);
        let spec = ReplaceSpec {
            strategy: Some(strategy),
            partition_column: Some("dt".into()),
            partition_type: Some(PartitionType::Date),
            ..Default::default()
        };
        let op = resolve(&events(), Some(&spec), &unit).unwrap();
        pg.run(&render_model(&events(), &unit, Some(&op), &RenderOptions::new(d)).unwrap());

        assert_eq!(
            pg.rows("SELECT user_id, n FROM events WHERE dt = '2026-06-20' ORDER BY user_id"),
            ["1|5", "3|7"],
            "{d}/{strategy:?}: 06-20 must be exactly the source's 06-20 — updated, inserted, and the deleted row gone"
        );
        assert_eq!(
            pg.rows("SELECT dt, n FROM events WHERE dt <> '2026-06-20' ORDER BY dt, user_id"),
            ["2026-06-19|99", "2026-06-19|99", "2026-06-21|99"],
            "{d}/{strategy:?}: partitions outside the restatement must not be rewritten"
        );
    }
}

#[test]
#[ignore = "needs a live PostgreSQL 15+ (FRESHET_PG_URL) and psql on PATH"]
fn a_merge_upserts_only_the_rows_of_the_scoped_partitions() {
    for d in EXECUTED {
        merge_upserts_only_the_scoped_partitions(d);
    }
}

fn merge_upserts_only_the_scoped_partitions(d: Dialect) {
    let pg = Pg::new(&format!("merge_{d}"));
    pg.sql(&[
        "CREATE TABLE raw_users (user_id int, name text, dt date)",
        "INSERT INTO raw_users VALUES (1,'ann','2026-06-20'),(2,'bob','2026-06-19')",
    ]);
    let users = model(
        "users",
        Materialization::Incremental,
        &["user_id", "name", "dt"],
        &[],
        "select user_id, name, dt from raw_users",
    );
    pg.run(&created_in(d, &users));

    pg.sql(&[
        "UPDATE raw_users SET name = 'ANN' WHERE user_id = 1", // in scope
        "UPDATE raw_users SET name = 'BOB' WHERE user_id = 2", // out of scope
        "INSERT INTO raw_users VALUES (4,'dee','2026-06-20')", // in scope, new
    ]);
    let unit = BackfillUnit::Partitions(vec!["2026-06-20".into()]);
    let spec = ReplaceSpec {
        strategy: Some(ReplaceStrategy::Merge),
        partition_column: Some("dt".into()),
        unique_key: vec!["user_id".into()],
        partition_type: Some(PartitionType::Date),
    };
    let op = resolve(&users, Some(&spec), &unit).unwrap();
    pg.run(&render_model(&users, &unit, Some(&op), &RenderOptions::new(d)).unwrap());

    assert_eq!(
        pg.rows("SELECT user_id, name FROM users ORDER BY user_id"),
        ["1|ANN", "2|bob", "4|dee"],
        "{d}: matched in scope → updated; out of scope → untouched; new in scope → inserted"
    );
}

#[test]
#[ignore = "needs a live PostgreSQL 15+ (FRESHET_PG_URL) and psql on PATH"]
fn a_full_refresh_keeps_the_relation_and_replaces_every_row() {
    for d in EXECUTED {
        full_refresh_keeps_the_relation(d);
    }
}

fn full_refresh_keeps_the_relation(d: Dialect) {
    let pg = Pg::new(&format!("full_refresh_{d}"));
    seed_raw_events(&pg);
    pg.run(&created_in(d, &events()));
    // Physical configuration a recreate would lose: an index, and the relation itself.
    pg.sql(&["CREATE INDEX events_dt ON events (dt)"]);
    let oid_before = pg.rows("SELECT 'events'::regclass::oid");

    pg.sql(&[
        "UPDATE events SET n = 99",
        "DELETE FROM raw_events WHERE dt = '2026-06-21'",
    ]);
    pg.run(&render_model(&events(), &BackfillUnit::FullModel, None, &RenderOptions::new(d)).unwrap());

    assert_eq!(
        pg.rows("SELECT 'events'::regclass::oid"),
        oid_before,
        "{d}: the same relation, not a new one"
    );
    assert_eq!(
        pg.rows("SELECT count(*) FROM pg_indexes WHERE indexname = 'events_dt'"),
        ["1"]
    );
    assert_eq!(
        pg.rows("SELECT dt, user_id, n FROM events ORDER BY dt, user_id"),
        [
            "2026-06-19|1|1",
            "2026-06-19|2|1",
            "2026-06-20|1|1",
            "2026-06-20|2|1"
        ],
        "every row is the model's current output — including the partition that disappeared"
    );
}

fn chain() -> Project {
    Project::from_models([
        model(
            "daily",
            Materialization::Table,
            &["dt", "total"],
            &[],
            "select dt, sum(n) as total from raw_events group by dt",
        ),
        model(
            "recent",
            Materialization::View,
            &["dt", "total"],
            &["daily"],
            "select dt, total from daily",
        ),
        model(
            "top",
            Materialization::View,
            &["dt"],
            &["recent"],
            "select dt from recent where total > 1",
        ),
    ])
}

fn plan_of(models: &[&str]) -> BackfillPlan {
    BackfillPlan {
        models: models.iter().map(|m| m.to_string()).collect(),
        reasons: BTreeMap::new(),
        unit: BTreeMap::new(),
    }
}

fn run_plan(
    pg: &Pg,
    project: &Project,
    plan: &BackfillPlan,
    recreate: &[&str],
) -> Result<(), String> {
    let mut opts = RenderOptions::new(Dialect::Postgres);
    opts.recreate = recreate
        .iter()
        .map(|s| s.to_string())
        .collect::<BTreeSet<_>>();
    let sql = render_plan(project, plan, &IncrementalConfig::default(), &opts).unwrap();
    for name in &plan.models {
        pg.exec(&sql[name].statements)?;
    }
    Ok(())
}

#[test]
#[ignore = "needs a live PostgreSQL 15+ (FRESHET_PG_URL) and psql on PATH"]
fn recreating_a_table_brings_back_the_views_the_plan_rebuilds() {
    let pg = Pg::new("views");
    seed_raw_events(&pg);
    let project = chain();
    // First build: everything is new.
    run_plan(
        &pg,
        &project,
        &plan_of(&["daily", "recent", "top"]),
        &["daily", "recent", "top"],
    )
    .unwrap();
    assert_eq!(
        pg.rows("SELECT dt FROM top ORDER BY dt"),
        ["2026-06-19", "2026-06-20"]
    );

    // `daily` is redefined; both views depend on it (one through the other) and the
    // plan rebuilds both. Without dropping them first, DROP TABLE would refuse.
    pg.sql(&["INSERT INTO raw_events VALUES ('2026-06-21',9,1)"]);
    run_plan(
        &pg,
        &project,
        &plan_of(&["daily", "recent", "top"]),
        &["daily"],
    )
    .expect("the plan runs end to end");
    assert_eq!(
        pg.rows("SELECT dt FROM top ORDER BY dt"),
        ["2026-06-19", "2026-06-20", "2026-06-21"]
    );
}

#[test]
#[ignore = "needs a live PostgreSQL 15+ (FRESHET_PG_URL) and psql on PATH"]
fn a_dependent_view_the_plan_does_not_rebuild_stops_the_recreate_and_survives() {
    let pg = Pg::new("pruned_view");
    seed_raw_events(&pg);
    let project = chain();
    run_plan(
        &pg,
        &project,
        &plan_of(&["daily", "recent", "top"]),
        &["daily", "recent", "top"],
    )
    .unwrap();

    // A plan that rebuilds `daily` and `recent` but prunes `top`.
    let err = run_plan(&pg, &project, &plan_of(&["daily", "recent"]), &["daily"]).unwrap_err();
    assert!(
        err.contains("depend"),
        "the DROP must fail naming the dependency, got: {err}"
    );
    // The transaction rolled back whole: nothing was dropped, `top` still answers.
    assert_eq!(pg.rows("SELECT count(*) FROM top"), ["2"]);
    assert_eq!(pg.rows("SELECT count(*) FROM recent"), ["3"]);
}

#[test]
#[ignore = "needs a live PostgreSQL 15+ (FRESHET_PG_URL) and psql on PATH"]
fn a_table_rebuilt_only_because_of_its_upstream_keeps_its_relation_and_views() {
    let pg = Pg::new("preserve_table");
    seed_raw_events(&pg);
    let project = chain();
    run_plan(
        &pg,
        &project,
        &plan_of(&["daily", "recent", "top"]),
        &["daily", "recent", "top"],
    )
    .unwrap();
    let oid_before = pg.rows("SELECT 'daily'::regclass::oid");

    // Rebuild `daily` alone (its upstream data changed; its SQL did not). The views are
    // not in the plan and need not be: the relation they bind to is the same one.
    pg.sql(&["INSERT INTO raw_events VALUES ('2026-06-22',9,3)"]);
    run_plan(&pg, &project, &plan_of(&["daily"]), &[]).unwrap();
    assert_eq!(pg.rows("SELECT 'daily'::regclass::oid"), oid_before);
    assert_eq!(
        pg.rows("SELECT dt FROM top ORDER BY dt"),
        ["2026-06-19", "2026-06-20", "2026-06-22"]
    );
}

#[test]
#[ignore = "needs a live PostgreSQL 15+ (FRESHET_PG_URL) and psql on PATH"]
fn a_redefined_view_can_drop_a_column_which_create_or_replace_cannot() {
    let pg = Pg::new("view_redefine");
    seed_raw_events(&pg);
    let v1 = model(
        "recent",
        Materialization::View,
        &["dt", "n"],
        &[],
        "select dt, n from raw_events",
    );
    pg.run(&created(&v1));
    let v2 = model(
        "recent",
        Materialization::View,
        &["dt"],
        &[],
        "select dt from raw_events",
    );
    // What CREATE OR REPLACE VIEW would say:
    let plain = render_model(
        &v2,
        &BackfillUnit::FullModel,
        None,
        &RenderOptions::new(Dialect::Postgres),
    )
    .unwrap();
    assert!(pg
        .exec(&plain.statements)
        .unwrap_err()
        .contains("cannot drop columns from view"));
    // What a redefinition renders:
    pg.run(&created(&v2));
    assert_eq!(pg.rows("SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'recent'"), ["1"]);
}
