//! Undeclared tables through the real binary: a misspelled model is named on every
//! command that loads a project, and `--strict-refs` refuses it.
//!
//! Which names count as undeclared, CTE scoping and the spelling candidates are
//! pinned in `freshet-sql`. What is asserted here is the CLI's half: the warning
//! without the flag, the refusal with it, and that `--json` stdout stays clean.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn project(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("freshet-strict-{case}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write(&dir, "stg.sql", "select a from raw");
    // Meant `stg`. Without a check it is an external table: no edge, wave 0.
    write(&dir, "mart.sql", "select a from stgg");
    write(&dir, "sources.json", r#"{"raw": ["a"]}"#);
    dir
}

fn write(dir: &Path, name: &str, body: &str) {
    std::fs::write(dir.join(name), body).unwrap();
}

fn freshet(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_freshet")).args(args).output().unwrap()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

const NAMED: &str =
    "model `mart` reads `stgg`, which is neither a model nor a declared source: did you mean `stg`?";

#[test]
fn a_misspelled_model_is_warned_about_and_still_planned() {
    let dir = project("warn");
    let state = dir.join("state.json");
    let out = freshet(&["plan", "--project", dir.to_str().unwrap(), "--state", state.to_str().unwrap(), "--json"]);
    assert!(out.status.success(), "{}", stderr(&out));
    let err = stderr(&out);
    assert!(err.contains(&format!("warning: {NAMED}")), "{err}");
    assert!(err.contains("--strict-refs refuses it"), "{err}");
    // The warning is on stderr only: stdout is still the plan, and still the old one.
    let plan: serde_json::Value = serde_json::from_slice(&out.stdout).expect("stdout is JSON");
    let mart = plan["models"].as_array().unwrap().iter().find(|m| m["name"] == "mart").unwrap();
    assert_eq!(mart["depends_on"], serde_json::json!([]));
}

#[test]
fn strict_refs_refuses_it_on_every_command_that_loads_a_project() {
    let dir = project("refuse");
    let d = dir.to_str().unwrap();
    let state = dir.join("state.json");
    let s = state.to_str().unwrap();
    for args in [
        vec!["plan", "--project", d, "--state", s, "--strict-refs"],
        vec!["--strict-refs", "graph", "--project", d],
        vec!["fingerprint", "--project", d, "--strict-refs"],
        vec!["restate", "--project", d, "--model", "stg", "--partitions", "2026-01-01", "--strict-refs"],
    ] {
        let out = freshet(&args);
        assert_eq!(out.status.code(), Some(1), "{args:?}: {}", stderr(&out));
        assert!(out.stdout.is_empty(), "{args:?} printed a plan");
        let err = stderr(&out);
        assert!(err.starts_with(&format!("error: {NAMED}")), "{args:?}: {err}");
        assert!(err.contains("an empty column list declares it"), "{args:?}: {err}");
    }
    assert!(!state.exists(), "a refused plan commits nothing");
}

#[test]
fn a_declared_source_passes_strict_refs_with_or_without_columns() {
    let dir = project("declared");
    write(&dir, "mart.sql", "select a from stg join raw_users using (a)");
    write(&dir, "sources.json", r#"{"raw": ["a"], "raw_users": []}"#);
    let out = freshet(&["--strict-refs", "graph", "--project", dir.to_str().unwrap()]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stderr(&out).is_empty(), "{}", stderr(&out));
    assert_eq!(String::from_utf8_lossy(&out.stdout), "stg\nmart\n");
}

#[test]
fn a_table_read_on_purpose_is_not_warned_about() {
    let dir = project("quiet");
    // Nothing is spelled like `raw_users`: an external table, as it always was.
    write(&dir, "mart.sql", "select a from raw_users");
    let out = freshet(&["graph", "--project", dir.to_str().unwrap()]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stderr(&out).is_empty(), "{}", stderr(&out));
}
