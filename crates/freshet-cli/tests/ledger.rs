//! The restatement ledger through the real binary: a restatement recorded with
//! `--state` survives a run that dies, is re-planned by `freshet plan`, and is settled
//! by `--commit`.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

fn project(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("freshet-ledger-{case}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write(&dir, "events.sql", "select dt, n from raw_events");
    write(&dir, "rolling.sql", "select dt, n from events");
    write(&dir, "sources.json", r#"{"raw_events": ["dt", "n"]}"#);
    write(&dir, "materializations.json", r#"{"events": "incremental", "rolling": "incremental"}"#);
    write(&dir, "incremental.json", r#"{"models": {"rolling": {"grain": "Day", "edges": {"events": {"window": 2}}}}}"#);
    dir
}

fn write(dir: &Path, name: &str, body: &str) {
    std::fs::write(dir.join(name), body).unwrap();
}

fn freshet(args: &[&str]) -> Output {
    let out = Command::new(env!("CARGO_BIN_EXE_freshet")).args(args).output().unwrap();
    assert!(out.status.success(), "freshet {args:?} failed: {}", String::from_utf8_lossy(&out.stderr));
    out
}

fn plan_json(p: &str, st: &str) -> Value {
    serde_json::from_slice(&freshet(&["plan", "--project", p, "--state", st, "--json"]).stdout).unwrap()
}

fn unit(resp: &Value, model: &str) -> Value {
    resp["models"].as_array().unwrap().iter().find(|m| m["name"] == model).map(|m| m["unit"].clone()).unwrap_or(Value::Null)
}

#[test]
fn a_recorded_restatement_is_replanned_until_it_is_settled() {
    let dir = project("owed");
    let state = dir.join("state.json");
    let (p, st) = (dir.to_str().unwrap(), state.to_str().unwrap());
    freshet(&["plan", "--project", p, "--state", st, "--commit"]);
    assert!(plan_json(p, st)["models"].as_array().unwrap().is_empty(), "baseline committed");

    // Plan the restatement with --state, then "the run dies": nothing is committed.
    let out = freshet(&["restate", "--project", p, "--model", "events", "--partitions", "2026-06-20", "--state", st, "--json"]);
    let restated: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(unit(&restated, "rolling"), serde_json::json!({"partitions": ["2026-06-20", "2026-06-21"]}), "the 2-day window runs forward");

    // The project did not change, but the plan remembers what is owed.
    let replan = plan_json(p, st);
    assert_eq!(unit(&replan, "events"), serde_json::json!({"partitions": ["2026-06-20"]}));
    assert_eq!(unit(&replan, "rolling"), serde_json::json!({"partitions": ["2026-06-20", "2026-06-21"]}));

    // The rerun succeeds and is committed: owed nothing.
    freshet(&["plan", "--project", p, "--state", st, "--commit"]);
    assert!(plan_json(p, st)["models"].as_array().unwrap().is_empty(), "settled by the committed plan");
}

#[test]
fn a_restatement_committed_directly_settles_its_own_partitions() {
    let dir = project("settle");
    let state = dir.join("state.json");
    let (p, st) = (dir.to_str().unwrap(), state.to_str().unwrap());
    freshet(&["plan", "--project", p, "--state", st, "--commit"]);
    freshet(&["restate", "--project", p, "--model", "events", "--partitions", "2026-06-20", "--state", st]);
    freshet(&["restate", "--project", p, "--model", "events", "--partitions", "2026-06-20", "--state", st, "--commit"]);
    assert!(plan_json(p, st)["models"].as_array().unwrap().is_empty());
}

#[test]
fn restating_without_state_writes_nothing() {
    let dir = project("stateless");
    freshet(&["restate", "--project", dir.to_str().unwrap(), "--model", "events", "--partitions", "2026-06-20"]);
    assert!(!dir.join("state.json").exists() && !dir.join(".planner").exists());
}
