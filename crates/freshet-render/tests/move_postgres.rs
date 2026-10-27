//! A rendered move, executed: Postgres to Postgres through DuckDB and Parquet on a
//! local stage.
//!
//! The syntax check (`tests/move_syntax.rs`) proves the statements are well-formed.
//! This proves the Postgres side does the right thing to real rows: the partition
//! moved holds exactly the source's rows, a stale row in it is replaced, the other
//! partitions are not touched, a table moves whole, and running a unit twice leaves
//! what running it once does.
//!
//! ```sh
//! FRESHET_PG_URL=postgresql://postgres@127.0.0.1:5432/postgres \
//! FRESHET_DUCKDB=duckdb \
//!   cargo test -p freshet-render --test move_postgres -- --ignored
//! ```
//!
//! `FRESHET_DUCKDB` is the DuckDB CLI, run as `dagron-step-sql` runs it: `-bail` and
//! `:memory:`, the script on stdin, the database through libpq's `PG*` variables.
//! Needs `psql` on `PATH` too, as `tests/postgres.rs` does. Each test works in two
//! schemas and a stage directory of its own, and removes them.

use std::collections::BTreeMap;
use std::io::Write;
use std::process::{Command, Stdio};

use freshet_core::{BackfillPlan, BackfillUnit, Column, Materialization, Model, Project};
use freshet_incremental::{IncrementalConfig, ModelPartitionConfig, PartitionType, ReplaceSpec, ReplaceStrategy};
use freshet_render::migrate::{render_move, MoveOptions, StepSql};
use freshet_render::Dialect;

struct Live {
    url: String,
    duckdb: String,
    src: String,
    dst: String,
    stage: std::path::PathBuf,
}

impl Live {
    fn new(test: &str) -> Live {
        let url = std::env::var("FRESHET_PG_URL").expect("set FRESHET_PG_URL to run the live move tests");
        let duckdb = std::env::var("FRESHET_DUCKDB").expect("set FRESHET_DUCKDB to the DuckDB CLI");
        let base = format!("freshet_move_{test}_{}", std::process::id());
        let stage = std::env::temp_dir().join(&base);
        let _ = std::fs::remove_dir_all(&stage);
        std::fs::create_dir_all(&stage).unwrap();
        let live = Live { url, duckdb, src: format!("{base}_src"), dst: format!("{base}_dst"), stage };
        live.psql(&format!(
            "DROP SCHEMA IF EXISTS {s} CASCADE; DROP SCHEMA IF EXISTS {d} CASCADE; CREATE SCHEMA {s}; CREATE SCHEMA {d};
             CREATE TABLE {s}.events (dt date, user_id bigint, amount numeric(10,2));
             INSERT INTO {s}.events VALUES ('2026-06-20', 1, 1.50), ('2026-06-20', 2, NULL), ('2026-06-21', 3, 3.00);
             CREATE TABLE {d}.events (LIKE {s}.events);
             INSERT INTO {d}.events VALUES ('2026-06-20', 99, 9.99), ('2026-06-21', 3, 3.00), ('2026-06-22', 4, 4.00);
             CREATE TABLE {s}.user_dim (user_id bigint, name text);
             INSERT INTO {s}.user_dim VALUES (1, 'ann'), (2, 'ünïcode');
             CREATE TABLE {d}.user_dim (LIKE {s}.user_dim);
             INSERT INTO {d}.user_dim VALUES (7, 'stale');",
            s = live.src,
            d = live.dst
        ));
        live
    }

    fn psql(&self, sql: &str) -> String {
        let out = Command::new("psql")
            .args([self.url.as_str(), "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", sql])
            .output()
            .expect("running psql (is it on PATH?)");
        assert!(out.status.success(), "psql failed: {}\n{sql}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// Run a step's script the way dagron-step-sql's DuckDB transport does.
    fn duckdb(&self, step: &StepSql) {
        assert_eq!(step.engine, "duckdb", "a Postgres side is run by DuckDB");
        let mut child = Command::new(&self.duckdb)
            .args(["-bail", ":memory:"])
            .envs(pg_env(&self.url))
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("running the DuckDB CLI");
        child.stdin.take().unwrap().write_all(step.script().as_bytes()).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success(), "DuckDB failed: {}\n{}", String::from_utf8_lossy(&out.stderr), step.script());
    }

    fn render(&self) -> freshet_render::migrate::RenderedMove {
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
        plan.unit.insert("events".into(), BackfillUnit::Partitions(vec!["2026-06-20".into()]));
        let opts = MoveOptions {
            from: Dialect::Postgres,
            to: Dialect::Postgres,
            from_schema: Some(self.src.clone()),
            to_schema: Some(self.dst.clone()),
            stage: format!("file://{}/", self.stage.display()).parse().unwrap(),
            storage_integration: None,
            iam_role: None,
        };
        render_move(&project, &plan, &cfg, &opts).unwrap()
    }

    fn rows(&self, schema: &str, table: &str, filter: &str) -> String {
        self.psql(&format!("SELECT * FROM {schema}.{table} {filter} ORDER BY 1, 2"))
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        let _ = Command::new("psql")
            .args([self.url.as_str(), "-X", "-q", "-c", &format!("DROP SCHEMA IF EXISTS {} CASCADE; DROP SCHEMA IF EXISTS {} CASCADE;", self.src, self.dst)])
            .output();
        let _ = std::fs::remove_dir_all(&self.stage);
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

/// `postgresql://user[:pw]@host:port/db` as libpq's environment, which is how the
/// rendered `ATTACH ''` finds its database.
fn pg_env(url: &str) -> BTreeMap<&'static str, String> {
    let rest = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let (auth, hostpart) = rest.rsplit_once('@').unwrap_or(("", rest));
    let (hostport, db) = hostpart.split_once('/').unwrap_or((hostpart, "postgres"));
    let (host, port) = hostport.split_once(':').unwrap_or((hostport, "5432"));
    let (user, pw) = auth.split_once(':').map(|(u, p)| (u, Some(p))).unwrap_or((auth, None));
    let mut env = BTreeMap::from([
        ("PGHOST", host.to_string()),
        ("PGPORT", port.to_string()),
        ("PGDATABASE", db.split('?').next().unwrap_or(db).to_string()),
    ]);
    if !user.is_empty() {
        env.insert("PGUSER", user.to_string());
    }
    if let Some(pw) = pw {
        env.insert("PGPASSWORD", pw.to_string());
    }
    env
}

#[test]
#[ignore = "needs a live Postgres (FRESHET_PG_URL) and the DuckDB CLI (FRESHET_DUCKDB)"]
fn a_partition_moves_exactly_and_nothing_else_is_touched() {
    let live = Live::new("partition");
    let m = live.render();
    let unit = m.units.iter().find(|u| u.model == "events").unwrap();
    live.duckdb(&unit.unload);
    assert!(
        std::fs::read_dir(live.stage.join("events.dt=2026-06-20")).unwrap().any(|f| f.unwrap().file_name().to_string_lossy().ends_with(".parquet")),
        "the unload wrote Parquet into the unit's own directory"
    );
    live.duckdb(&unit.load);
    let (s, d) = (&live.src, &live.dst);
    assert_eq!(
        live.rows(d, "events", "WHERE dt = '2026-06-20'"),
        live.rows(s, "events", "WHERE dt = '2026-06-20'"),
        "the moved partition is the source's, and its stale row is gone"
    );
    assert_eq!(live.rows(d, "events", "WHERE dt <> '2026-06-20'"), "2026-06-21|3|3.00\n2026-06-22|4|4.00", "other partitions untouched");
}

#[test]
#[ignore = "needs a live Postgres (FRESHET_PG_URL) and the DuckDB CLI (FRESHET_DUCKDB)"]
fn a_table_moves_whole_and_a_rerun_changes_nothing() {
    let live = Live::new("rerun");
    let m = live.render();
    let unit = m.units.iter().find(|u| u.model == "user_dim").unwrap();
    assert_eq!(unit.partition, None);
    for _ in 0..2 {
        live.duckdb(&unit.unload);
        live.duckdb(&unit.load);
        assert_eq!(live.rows(&live.dst, "user_dim", ""), "1|ann\n2|ünïcode", "the whole table, once, whatever the rerun");
    }
    let events = m.units.iter().find(|u| u.model == "events").unwrap();
    for _ in 0..2 {
        live.duckdb(&events.unload);
        live.duckdb(&events.load);
    }
    assert_eq!(live.psql(&format!("SELECT count(*) FROM {}.events WHERE dt = '2026-06-20'", live.dst)), "2");
}
