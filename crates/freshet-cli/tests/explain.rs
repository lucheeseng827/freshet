//! `freshet explain --against <dbt project>` end to end: the binary, the flags, the exit
//! code, and both output forms, over freshet-dbt's hand-written dbt artifacts.
//!
//! What the numbers *mean* is freshet-dbt's to test (`crates/freshet-dbt/tests`); this
//! file pins what the CLI adds — argument handling, printing, and failing with a hint.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

/// A dbt project whose `target/` holds a `dbt build`'s manifest and run results, and
/// whose `state/` holds the production manifest it is compared against.
fn project() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../freshet-dbt/tests/fixtures/jaffle_shop")
}

fn freshet(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_freshet")).args(args).output().unwrap()
}

fn stdout(out: &Output) -> String {
    String::from_utf8(out.stdout.clone()).unwrap()
}

fn stderr(out: &Output) -> String {
    String::from_utf8(out.stderr.clone()).unwrap()
}

#[test]
fn explain_prints_what_freshet_would_have_pruned_from_the_last_run() {
    let dir = project();
    let out = freshet(&[
        "explain",
        "--against",
        dir.to_str().unwrap(),
        "--state",
        dir.join("state").to_str().unwrap(),
    ]);
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    let text = stdout(&out);
    for line in [
        "freshet explain: 9 dbt model(s), 9 built by dbt",
        "stg_orders              sql — only [amount]",
        "Freshet would rebuild 7 model(s):",
        "orders_enriched         downstream of stg_orders via [amount]",
        "vs the last `dbt build` (9 model(s)): 2 fewer — 22.2% avoided",
        "vs state:modified+ (Freshet's approximation) (8 model(s)): 1 fewer — 12.5% avoided",
        "it builds, Freshet does not: mart_customers",
        "orders_daily            its SQL has Jinja beyond ref()/source()",
    ] {
        assert!(text.contains(line), "missing {line:?} in:\n{text}");
    }
}

#[test]
fn explain_json_is_the_report_planner_dbt_serializes() {
    let dir = project();
    let out = freshet(&[
        "explain",
        "--against",
        dir.to_str().unwrap(),
        "--state",
        dir.join("state").to_str().unwrap(),
        "--json",
    ]);
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    let report: Value = serde_json::from_str(&stdout(&out)).expect("stdout is exactly one JSON document");
    assert_eq!(report["report_version"], 1);
    let c = &report["comparison"];
    assert_eq!(c["vs_state_modified_plus"]["pruned"], serde_json::json!(["mart_customers"]));
    assert_eq!(c["vs_last_run"]["avoided"], 2);
    assert_eq!(c["vs_last_run"]["avoided_pct"], 22.2);
    assert_eq!(c["freshet"].as_array().unwrap().len(), 7);
}

#[test]
fn explain_without_state_reports_the_graph_and_no_pruning_number() {
    let out = freshet(&["explain", "--against", project().to_str().unwrap()]);
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains("no --state manifest: nothing to compare, so no pruning number."), "{text}");
    assert!(!text.contains("avoided"), "no saving may be claimed without a baseline:\n{text}");
}

#[test]
fn explain_on_a_directory_without_a_manifest_fails_and_says_how_to_make_one() {
    let empty = std::env::temp_dir().join(format!("freshet-explain-empty-{}", std::process::id()));
    std::fs::create_dir_all(&empty).unwrap();
    let out = freshet(&["explain", "--against", empty.to_str().unwrap()]);
    std::fs::remove_dir_all(&empty).unwrap();
    assert!(!out.status.success());
    let err = stderr(&out);
    assert!(err.contains("run `dbt parse` or `dbt compile`"), "{err}");
    assert!(stdout(&out).is_empty(), "nothing on stdout when there is no report");
}

#[test]
fn explain_requires_the_project_to_explain() {
    let out = freshet(&["explain"]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("--against"), "{}", stderr(&out));
}
