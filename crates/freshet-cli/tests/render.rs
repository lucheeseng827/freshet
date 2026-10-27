//! `--sql DIALECT` through the real binary: what `plan`, `restate` and `submit` emit
//! when asked for SQL, and what they refuse to emit.
//!
//! The SQL itself is pinned in `freshet-render` (golden strings, a syntax check, and
//! a live-Postgres suite). What is asserted here is the CLI's own half: the flag
//! reaches the renderer, the redefined models are recreated and nothing else is, a
//! restatement recreates nothing, and a plan that cannot be rendered prints reasons
//! instead of SQL and never commits.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

fn project(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("freshet-render-{case}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write(&dir, "events.sql", "select dt, user_id, n from raw_events");
    write(
        &dir,
        "daily.sql",
        "select dt, sum(n) as total from events group by dt",
    );
    write(
        &dir,
        "sources.json",
        r#"{"raw_events": ["dt", "user_id", "n"]}"#,
    );
    write(
        &dir,
        "materializations.json",
        r#"{"events": "incremental"}"#,
    );
    dir
}

fn write(dir: &Path, name: &str, body: &str) {
    std::fs::write(dir.join(name), body).unwrap();
}

fn freshet(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_freshet"))
        .args(args)
        .output()
        .unwrap()
}

fn json(out: &Output) -> Value {
    assert!(
        out.status.success(),
        "freshet failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("stdout is JSON")
}

fn statements<'a>(resp: &'a Value, model: &str) -> Vec<&'a str> {
    resp["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["name"] == model)
        .unwrap_or_else(|| panic!("{model} not in plan"))["sql"]["statements"]
        .as_array()
        .unwrap_or_else(|| panic!("{model} has no sql"))
        .iter()
        .map(|s| s.as_str().unwrap())
        .collect()
}

#[test]
fn plan_json_carries_sql_for_every_model_only_when_asked() {
    let dir = project("ask");
    let state = dir.join("state.json");
    let st = state.to_str().unwrap();
    let p = dir.to_str().unwrap();

    let plain = json(&freshet(&["plan", "--project", p, "--state", st, "--json"]));
    assert!(plain["models"]
        .as_array()
        .unwrap()
        .iter()
        .all(|m| m.get("sql").is_none()));

    let resp = json(&freshet(&[
        "plan",
        "--project",
        p,
        "--state",
        st,
        "--json",
        "--sql",
        "snowflake",
    ]));
    for m in resp["models"].as_array().unwrap() {
        assert_eq!(m["sql"]["dialect"], "snowflake", "{m}");
    }
    assert_eq!(
        statements(&resp, "events"),
        ["CREATE OR REPLACE TABLE events COPY GRANTS AS\nselect dt, user_id, n from raw_events"],
        "cold: every model is new, so every model is created"
    );
}

#[test]
fn only_a_redefined_model_is_recreated_once_state_exists() {
    let dir = project("redefined");
    let state = dir.join("state.json");
    let (p, st) = (dir.to_str().unwrap(), state.to_str().unwrap());
    assert!(
        freshet(&["plan", "--project", p, "--state", st, "--commit"])
            .status
            .success()
    );

    // Redefine `events`; `daily` rebuilds only because of it.
    write(
        &dir,
        "events.sql",
        "select dt, user_id, n from raw_events where n > 0",
    );
    write(
        &dir,
        "incremental.json",
        r#"{"models": {"events": {"replace": {"strategy": "insert_overwrite", "partition_column": "dt"}}}}"#,
    );
    let resp = json(&freshet(&[
        "plan",
        "--project",
        p,
        "--state",
        st,
        "--json",
        "--sql",
        "postgres",
    ]));
    assert!(
        statements(&resp, "events")
            .iter()
            .any(|s| s.starts_with("CREATE TABLE events AS")),
        "redefined → recreated"
    );
    assert!(
        statements(&resp, "daily")[0]
            .starts_with("WITH freshet_deleted AS (\n  DELETE FROM daily\n)"),
        "daily's own SQL did not change, so it keeps its relation: {:?}",
        statements(&resp, "daily")
    );
}

#[test]
fn a_restatement_writes_partitions_and_recreates_nothing_unless_told_to() {
    let dir = project("restate");
    write(
        &dir,
        "incremental.json",
        r#"{"models": {"events": {"replace": {"strategy": "insert_overwrite", "partition_column": "dt", "partition_type": "date"}}}}"#,
    );
    let p = dir.to_str().unwrap();
    let resp = json(&freshet(&[
        "restate",
        "--project",
        p,
        "--model",
        "events",
        "--partitions",
        "2026-06-20",
        "--json",
        "--sql",
        "bigquery",
    ]));
    let events = statements(&resp, "events");
    assert!(
        events[0].contains(
            "WHEN NOT MATCHED BY SOURCE AND freshet_tgt.dt IN (DATE '2026-06-20') THEN DELETE"
        ),
        "{events:?}"
    );

    let forced = json(&freshet(&[
        "restate",
        "--project",
        p,
        "--model",
        "events",
        "--partitions",
        "2026-06-20",
        "--json",
        "--sql",
        "bigquery",
        "--recreate",
        "daily",
    ]));
    assert_eq!(
        statements(&forced, "daily"),
        ["CREATE OR REPLACE TABLE daily AS\nselect dt, sum(n) as total from events group by dt"]
    );
}

#[test]
fn an_unrenderable_plan_prints_reasons_not_sql_and_does_not_commit() {
    let dir = project("refuse");
    let state = dir.join("state.json");
    let (p, st) = (dir.to_str().unwrap(), state.to_str().unwrap());
    // An undeclared incremental model, restated: no strategy to write partitions with.
    let out = freshet(&[
        "restate",
        "--project",
        p,
        "--model",
        "events",
        "--partitions",
        "2026-06-20",
        "--sql",
        "postgres",
    ]);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("`events`") && err.contains("incremental.json"),
        "{err}"
    );
    assert!(
        out.stdout.is_empty(),
        "no plan, no SQL: {}",
        String::from_utf8_lossy(&out.stdout)
    );

    // And a code-change plan that fails to render must not commit: the same project
    // with a bad dialect name.
    let out = freshet(&[
        "plan",
        "--project",
        p,
        "--state",
        st,
        "--commit",
        "--sql",
        "oracle",
    ]);
    assert!(!out.status.success());
    assert!(
        !state.exists(),
        "a plan that could not be rendered must not commit"
    );
}

#[test]
fn schema_requires_sql() {
    let dir = project("requires");
    let out = freshet(&[
        "plan",
        "--project",
        dir.to_str().unwrap(),
        "--schema",
        "analytics",
    ]);
    assert!(
        !out.status.success(),
        "--schema without --sql is a mistake, not a no-op"
    );
}

#[cfg(feature = "submit")]
#[test]
fn submit_carries_the_sql_so_the_consumer_can_run_it() {
    let dir = project("submit");
    let state = dir.join("state.json");
    let (p, st) = (dir.to_str().unwrap(), state.to_str().unwrap());
    let body = json(&freshet(&[
        "submit",
        "--project",
        p,
        "--state",
        st,
        "--dry-run",
        "--sql",
        "postgres",
        "--schema",
        "analytics",
    ]));
    assert_eq!(
        statements(&body["plan"], "events"),
        [
            "BEGIN",
            "DROP TABLE IF EXISTS analytics.events",
            "CREATE TABLE analytics.events AS\nselect dt, user_id, n from raw_events",
            "COMMIT"
        ]
    );
}
