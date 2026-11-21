//! `freshet cost record` and the measured estimate through the real binary.
//!
//! The pricing itself is pinned in `freshet_embed::measured`. What is asserted here is
//! the CLI's half: the record it writes, what it refuses, and the line a plan prints
//! once runs are recorded — beside the rate card, never instead of it, and never on
//! `--json`, whose contract is frozen.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn project(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("freshet-cost-{case}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write(&dir, "stg.sql", "select x, y from raw");
    write(&dir, "a.sql", "select x from stg");
    write(&dir, "b.sql", "select y from stg");
    write(&dir, "sources.json", r#"{"raw": ["x", "y"]}"#);
    dir
}

fn write(dir: &Path, name: &str, body: &str) {
    std::fs::write(dir.join(name), body).unwrap();
}

fn freshet(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_freshet")).args(args).env_remove("RUST_BACKTRACE").output().unwrap()
}

fn record(dir: &Path, extra: &[&str]) -> Output {
    let mut args = vec!["cost", "record", "--project", dir.to_str().unwrap()];
    args.extend_from_slice(extra);
    freshet(&args)
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn recorded_runs_price_the_plan_in_their_unit_beside_the_rate_card() {
    let dir = project("price");
    let d = dir.to_str().unwrap();
    let state = dir.join("state.json");
    let s = state.to_str().unwrap();

    // No record: the rate card alone.
    let plain = freshet(&["plan", "--project", d, "--state", s]);
    assert!(plain.status.success(), "{}", stderr(&plain));
    let text = String::from_utf8_lossy(&plain.stdout).into_owned();
    assert!(text.contains("rebuild work: 3.000 CU planned"), "{text}");
    assert!(!text.contains("measured:"), "{text}");

    for (model, value) in [("stg", "10"), ("stg", "14"), ("a", "40")] {
        let out = record(&dir, &["--model", model, "--value", value, "--unit", "seconds"]);
        assert!(out.status.success(), "{}", stderr(&out));
    }
    let costs: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(dir.join("costs.json")).unwrap()).unwrap();
    assert_eq!(costs, serde_json::json!({"unit": "seconds", "models": {"stg": {"full": [10.0, 14.0]}, "a": {"full": [40.0]}}}));

    // stg at its median 12, a at 40, b extrapolated at the median rate of 26 a CU.
    let priced = freshet(&["plan", "--project", d, "--state", s]);
    let text = String::from_utf8_lossy(&priced.stdout).into_owned();
    assert!(text.contains("rebuild work: 3.000 CU planned"), "the rate card stays: {text}");
    assert!(
        text.contains("measured: 78.0 seconds planned, 67% of it from the models' own runs (the rest at 26.00 seconds per CU)"),
        "{text}"
    );

    // --json is the frozen wire contract: the record does not reach it.
    let json = freshet(&["plan", "--project", d, "--state", s, "--json"]);
    assert!(!String::from_utf8_lossy(&json.stdout).contains("seconds"));
}

#[test]
fn a_partition_run_is_recorded_per_partition() {
    let dir = project("partitions");
    let out = record(&dir, &["--model", "stg", "--value", "9", "--partitions", "3", "--unit", "seconds"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stderr(&out).contains("recorded stg: 3 partition(s) at 9 seconds"), "{}", stderr(&out));
    let costs: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(dir.join("costs.json")).unwrap()).unwrap();
    assert_eq!(costs["models"]["stg"]["partition"], serde_json::json!([3.0]));
}

#[test]
fn a_record_is_refused_without_a_unit_with_another_unit_or_for_an_unknown_model() {
    let dir = project("refuse");
    let out = record(&dir, &["--model", "stg", "--value", "1"]);
    assert!(!out.status.success() && stderr(&out).contains("say what unit costs are in with --unit"), "{}", stderr(&out));

    assert!(record(&dir, &["--model", "stg", "--value", "1", "--unit", "seconds"]).status.success());
    let out = record(&dir, &["--model", "stg", "--value", "1", "--unit", "credits"]);
    assert!(!out.status.success() && stderr(&out).contains("records costs in seconds, not credits"), "{}", stderr(&out));

    let out = record(&dir, &["--model", "stgg", "--value", "1"]);
    assert!(!out.status.success() && stderr(&out).contains("has no model `stgg`"), "{}", stderr(&out));

    let out = record(&dir, &["--model", "stg", "--value=-2"]);
    assert!(!out.status.success() && stderr(&out).contains("finite number >= 0"), "{}", stderr(&out));
    let costs: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(dir.join("costs.json")).unwrap()).unwrap();
    assert_eq!(costs["models"]["stg"]["full"], serde_json::json!([1.0]), "a refused record writes nothing");
}
