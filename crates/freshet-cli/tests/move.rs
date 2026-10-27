//! `freshet move` through the real binary: what moves, by partition or whole, and
//! the `freshet-move/1` document dagron-state compiles.
//!
//! The statements are pinned in `freshet-render` (golden strings, a syntax check,
//! and a live Postgres-through-DuckDB move). What is asserted here is the CLI's half:
//! which models become which units, the day ranges, and what it refuses.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

fn project(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("freshet-move-{case}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write(&dir, "events.sql", "select dt, user_id, n from raw_events");
    write(&dir, "sessions.sql", "select dt, user_id from raw_events");
    write(&dir, "daily.sql", "select dt, sum(n) as total from events group by dt");
    write(&dir, "recent.sql", "select dt, total from daily");
    write(&dir, "sources.json", r#"{"raw_events": ["dt", "user_id", "n"]}"#);
    write(
        &dir,
        "materializations.json",
        r#"{"events": "incremental", "sessions": "incremental", "daily": "table", "recent": "view"}"#,
    );
    write(
        &dir,
        "incremental.json",
        r#"{"models": {"events": {"replace": {"strategy": "insert_overwrite", "partition_column": "dt", "partition_type": "date"}}}}"#,
    );
    dir
}

fn write(dir: &Path, name: &str, body: &str) {
    std::fs::write(dir.join(name), body).unwrap();
}

fn freshet(dir: &Path, extra: &[&str]) -> Output {
    let mut args = vec![
        "move",
        "--project",
        dir.to_str().unwrap(),
        "--from",
        "postgres",
        "--to",
        "snowflake",
        "--stage",
        "s3://acme-moves/2026-10",
        "--from-schema",
        "public",
        "--to-schema",
        "ANALYTICS.MARTS",
        "--storage-integration",
        "FRESHET_STAGE",
    ];
    args.extend_from_slice(extra);
    Command::new(env!("CARGO_BIN_EXE_freshet")).args(&args).output().unwrap()
}

fn json(out: &Output) -> Value {
    assert!(out.status.success(), "freshet failed: {}", String::from_utf8_lossy(&out.stderr));
    serde_json::from_slice(&out.stdout).expect("stdout is JSON")
}

fn units(doc: &Value) -> Vec<(String, String)> {
    doc["units"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| (u["model"].as_str().unwrap().to_string(), u["partition"]["key"].as_str().unwrap_or("whole").to_string()))
        .collect()
}

#[test]
fn partitioned_models_move_by_day_and_everything_else_whole() {
    let dir = project("days");
    let doc = json(&freshet(&dir, &["--partitions", "2026-06-20..2026-06-21", "--json"]));
    assert_eq!(doc["contract"], "freshet-move/1");
    assert_eq!((doc["from"].as_str(), doc["to"].as_str()), (Some("postgres"), Some("snowflake")));
    assert_eq!(doc["stage"], "s3://acme-moves/2026-10/");
    assert_eq!(
        units(&doc),
        [
            ("events".into(), "2026-06-20".into()),
            ("events".into(), "2026-06-21".into()),
            // Incremental, but with no declared partition column: swept up in
            // "every model", so it moves whole rather than failing the move.
            ("sessions".into(), "whole".into()),
            ("daily".into(), "whole".into()),
        ],
        "plan order, one unit per partition of a partitioned model"
    );
    assert_eq!(doc["not_moved"][0]["model"], "recent", "a view is not moved");

    let u = &doc["units"][0];
    assert_eq!(u["location"], "s3://acme-moves/2026-10/events.dt=2026-06-20/");
    assert_eq!(u["partition"]["partition_type"], "date");
    assert_eq!(u["unload"]["engine"], "duckdb", "Postgres is unloaded through DuckDB");
    assert_eq!(u["load"]["engine"], "snowflake");
    assert!(u["unload"]["statements"].as_array().unwrap().iter().any(|s| s.as_str().unwrap().starts_with("COPY (SELECT dt, user_id, n FROM freshet_pg.public.events WHERE dt IN (DATE '2026-06-20'))")));
    assert_eq!(u["parity"]["source"]["engine"], "postgres", "and checked on Postgres itself");
    assert_eq!(u["parity"]["source"]["relation"], "public.events");
    assert_eq!(u["parity"]["target"]["relation"], "ANALYTICS.MARTS.events");
    assert_eq!(u["parity"]["target"]["predicate"], "dt IN (DATE '2026-06-20')");
    assert!(doc["units"][3]["partition"].is_null(), "whole is null, not absent");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn model_moves_only_the_models_named() {
    let dir = project("named");
    let doc = json(&freshet(&dir, &["--model", "daily", "--json"]));
    assert_eq!(units(&doc), [("daily".into(), "whole".into())], "no downstream is pulled in");
    assert!(doc["not_moved"].as_array().unwrap().is_empty());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_named_model_that_cannot_move_by_partition_is_refused_with_the_reason() {
    let dir = project("refused");
    let out = freshet(&dir, &["--model", "sessions", "--partitions", "2026-06-20"]);
    assert!(!out.status.success());
    let e = String::from_utf8_lossy(&out.stderr);
    assert!(e.contains("`sessions`") && e.contains("declares no partition_column"), "{e}");
    assert!(out.stdout.is_empty(), "nothing is printed but the reasons");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_move_it_cannot_reach_or_name_is_refused_before_anything_prints() {
    let dir = project("setup");
    let out = Command::new(env!("CARGO_BIN_EXE_freshet"))
        .args(["move", "--project", dir.to_str().unwrap(), "--from", "postgres", "--to", "snowflake", "--stage", "s3://b/m/"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--storage-integration"));

    let out = freshet(&dir, &["--model", "nope"]);
    assert!(String::from_utf8_lossy(&out.stderr).contains("--model nope"));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn the_text_form_lists_each_unit_and_its_statements() {
    let dir = project("text");
    let out = freshet(&dir, &["--model", "events", "--partitions", "2026-06-20"]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.starts_with("move postgres -> snowflake through s3://acme-moves/2026-10/ (1 unit(s)):"), "{text}");
    assert!(text.contains("-- events dt=2026-06-20: unload (duckdb)"), "{text}");
    assert!(text.contains("-- events dt=2026-06-20: load (snowflake)\nBEGIN TRANSACTION;"), "{text}");
    assert!(text.contains("-- events dt=2026-06-20: parity postgres public.events vs snowflake ANALYTICS.MARTS.events where dt IN (DATE '2026-06-20')"), "{text}");
    std::fs::remove_dir_all(&dir).ok();
}
