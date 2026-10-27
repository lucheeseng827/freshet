//! `explain_against` over a small dbt project's artifacts (`tests/fixtures/jaffle_shop`).
//!
//! The fixture is hand-written in dbt's manifest v12 / run-results v6 layout (both
//! validate against dbt's published JSON schemas), and tells one story:
//!
//! ```text
//! source shop.orders          ──> stg_orders (alias stg_orders_v2)
//! source shop.customers       ──> stg_customers
//! stg_orders + stg_customers  ──> orders_enriched ──> mart_revenue      (reads amount)
//!                                                 ──> mart_customers    (reads names, regions, ids)
//!                                                 ──> mart_big_orders   (filters on amount)
//! stg_orders                  ──> orders_daily (incremental, Jinja only) ──> weekly_orders
//! stg_customers               ──> customer_regions (new)
//! ```
//!
//! Between the `--state` manifest (production: `"prod"."analytics"`) and the current
//! one (development: `"dev"."dbt_alice"`), `stg_orders` changed one expression —
//! `amount_cents / 100` became `amount_cents / 100.0` — `customer_regions` is new and
//! `legacy_orders` is gone. The last run was a `dbt build` of every model.

use std::path::{Path, PathBuf};

use freshet_core::ColumnRef;
use freshet_dbt::{explain_against, DbtProject, ExplainReport, Lineage, Manifest, Reason};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/jaffle_shop")
}

fn report() -> ExplainReport {
    explain_against(&fixture(), Some(&fixture().join("state"))).expect("the fixture explains")
}

fn rebuilt(report: &ExplainReport) -> Vec<&str> {
    report.comparison.as_ref().unwrap().freshet.iter().map(|r| r.model.as_str()).collect()
}

fn reason<'a>(report: &'a ExplainReport, model: &str) -> &'a Reason {
    &report.comparison.as_ref().unwrap().freshet.iter().find(|r| r.model == model).expect(model).reason
}

#[test]
fn freshet_prunes_the_mart_that_reads_only_unchanged_columns_and_state_modified_plus_does_not() {
    let r = report();
    assert!(!rebuilt(&r).contains(&"mart_customers"), "mart_customers reads no column the change touched");
    let modified_plus = r.comparison.as_ref().unwrap().vs_state_modified_plus.as_ref().unwrap();
    assert_eq!(modified_plus.models, 8, "state:modified+ is the two changed models and all six descendants");
    assert_eq!(modified_plus.pruned, vec!["mart_customers"]);
    assert_eq!((modified_plus.avoided, modified_plus.avoided_pct), (1, 12.5));
}

#[test]
fn the_change_is_narrowed_to_the_one_column_it_touched() {
    let r = report();
    let stg = r.comparison.as_ref().unwrap().changed.iter().find(|c| c.model == "stg_orders").unwrap();
    assert_eq!(stg.because, vec!["sql"]);
    assert_eq!(stg.columns.as_deref(), Some(&["amount".to_string()][..]));
    assert_eq!(reason(&r, "stg_orders"), &Reason::DirectlyChanged);
    assert_eq!(
        reason(&r, "orders_enriched"),
        &Reason::Downstream { because_of: "stg_orders".into(), via_columns: vec!["amount".into()] }
    );
    assert_eq!(
        reason(&r, "mart_revenue"),
        &Reason::Downstream { because_of: "orders_enriched".into(), via_columns: vec!["revenue".into()] }
    );
}

#[test]
fn the_aliased_models_downstream_is_attributed_to_the_model_not_its_relation_name() {
    // orders_enriched's compiled SQL reads "dev"."dbt_alice"."stg_orders_v2". A reference
    // left under that name would never match a change to `stg_orders`.
    let manifest = Manifest::from_path(&fixture().join("target/manifest.json")).unwrap();
    let project = DbtProject::build(&manifest);
    let enriched = project.project.get("orders_enriched").unwrap();
    let amount = enriched.columns.iter().find(|c| c.name == "amount").unwrap();
    assert!(amount.upstream.contains(&ColumnRef::new("stg_orders", "amount")), "{:?}", amount.upstream);
    assert!(enriched.columns.iter().flat_map(|c| &c.upstream).all(|r| r.model != "stg_orders_v2"));
    assert!(rebuilt(&report()).contains(&"orders_enriched"));
}

#[test]
fn a_model_known_only_by_its_jinja_falls_back_and_its_downstreams_still_rebuild() {
    let r = report();
    let daily = r.model_level.iter().find(|m| m.model == "orders_daily").expect("orders_daily is model-level");
    assert!(daily.reason.contains("Jinja"), "{}", daily.reason);
    // It reads only `ordered_at`, so precise lineage would prune it; falling back
    // rebuilds it — and what reads it — instead.
    let rebuilt = rebuilt(&r);
    assert!(rebuilt.contains(&"orders_daily"));
    assert!(rebuilt.contains(&"weekly_orders"));
    let weekly = r.model_level.iter().find(|m| m.model == "weekly_orders").unwrap();
    assert!(weekly.reason.contains("orders_daily"), "{}", weekly.reason);
}

#[test]
fn an_unanalyzable_model_is_given_its_documented_columns_never_none() {
    let manifest = Manifest::from_path(&fixture().join("target/manifest.json")).unwrap();
    let project = DbtProject::build(&manifest);
    let daily = project.project.get("orders_daily").unwrap();
    let names: Vec<&str> = daily.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["order_date", "orders"]);
    assert!(daily.columns.iter().all(|c| c.upstream.is_empty()));
    assert_eq!(daily.depends_on, vec!["stg_orders"], "edges come from the manifest, not the unreadable SQL");
    assert!(matches!(project.models["orders_daily"].lineage, Lineage::ModelLevel { .. }));
}

#[test]
fn a_filter_on_a_changed_column_rebuilds_the_mart_that_never_projects_it() {
    // mart_big_orders selects customer_name and a count, and filters `amount >= 100`.
    let r = report();
    assert_eq!(
        reason(&r, "mart_big_orders"),
        &Reason::Downstream {
            because_of: "orders_enriched".into(),
            via_columns: vec!["big_orders".into(), "customer_name".into()]
        }
    );
}

#[test]
fn new_and_removed_models_are_reported() {
    let r = report();
    let c = r.comparison.as_ref().unwrap();
    let new = c.changed.iter().find(|m| m.model == "customer_regions").unwrap();
    assert_eq!(new.because, vec!["new model"]);
    assert_eq!(new.columns, None, "every column of a new model is new");
    assert!(rebuilt(&r).contains(&"customer_regions"));
    assert_eq!(c.removed, vec!["legacy_orders"]);
}

#[test]
fn building_into_a_different_schema_is_not_a_change() {
    // Every compiled relation name differs between the two manifests ("prod"."analytics"
    // vs "dev"."dbt_alice"); only the edited model and the new one count as changed.
    let r = report();
    let changed: Vec<&str> = r.comparison.as_ref().unwrap().changed.iter().map(|c| c.model.as_str()).collect();
    assert_eq!(changed, vec!["customer_regions", "stg_orders"]);
}

#[test]
fn the_last_run_counts_the_models_it_built_and_the_saving_is_net() {
    let r = report();
    assert_eq!((r.last_run.source.as_str(), r.last_run.command.as_deref()), ("run_results", Some("build")));
    assert_eq!(r.last_run.models, 9, "the build's test result is not a model");
    let last = r.comparison.as_ref().unwrap().vs_last_run.as_ref().unwrap();
    assert_eq!(last.models, 9);
    assert_eq!(last.freshet_rebuilds, 7);
    assert_eq!(last.pruned, vec!["mart_customers", "stg_customers"]);
    assert!(last.freshet_extra.is_empty());
    assert_eq!((last.avoided, last.avoided_pct), (2, 22.2));
}

#[test]
fn every_rebuild_comes_in_dependency_order() {
    let r = report();
    let order = rebuilt(&r);
    let pos = |m: &str| order.iter().position(|x| *x == m).unwrap();
    assert!(pos("stg_orders") < pos("orders_enriched"));
    assert!(pos("orders_enriched") < pos("mart_revenue"));
    assert!(pos("orders_daily") < pos("weekly_orders"));
    assert_eq!(order.len(), 7);
}

#[test]
fn the_json_report_has_the_documented_shape() {
    let json = serde_json::to_value(report()).unwrap();
    assert_eq!(json["report_version"], 1);
    for key in ["models", "runnable", "ephemeral", "column_lineage", "model_lineage", "compiled_sql"] {
        assert!(json["graph"][key].is_u64(), "graph.{key}");
    }
    assert_eq!(json["last_run"]["source"], "run_results");
    assert_eq!(json["model_level"][0]["model"], "orders_daily");
    let c = &json["comparison"];
    assert_eq!(c["compared_on"]["compiled_sql"], 8);
    assert_eq!(c["compared_on"]["raw_sql"], 1);
    assert_eq!(c["changed"][1]["columns"], serde_json::json!(["amount"]));
    assert!(c["changed"][0]["columns"].is_null());
    assert_eq!(c["removed"], serde_json::json!(["legacy_orders"]));
    // The reason uses freshet-embed's wire encoding.
    assert_eq!(c["freshet"][0]["reason"], "directly_changed");
    let enriched = c["freshet"].as_array().unwrap().iter().find(|r| r["model"] == "orders_enriched").unwrap();
    assert_eq!(
        enriched["reason"],
        serde_json::json!({"downstream": {"because_of": "stg_orders", "via_columns": ["amount"]}})
    );
    assert_eq!(enriched["materialized"], "table");
    assert_eq!(enriched["lineage"], "column");
    for baseline in ["vs_last_run", "vs_state_modified_plus"] {
        for key in ["label", "models", "freshet_rebuilds", "pruned", "freshet_extra", "avoided", "avoided_pct"] {
            assert!(!c[baseline][key].is_null(), "{baseline}.{key}");
        }
    }
    assert!(json["notes"].as_array().unwrap().iter().any(|n| n.as_str().unwrap().contains("approximation")));
}

#[test]
fn without_a_baseline_manifest_there_is_no_pruning_number() {
    let r = explain_against(&fixture(), None).unwrap();
    assert!(r.comparison.is_none());
    assert_eq!(r.graph.models, 9);
    assert_eq!((r.graph.column_lineage, r.graph.model_lineage), (7, 2));
    assert!(r.notes.iter().any(|n| n.contains("--state")));
    assert!(r.render_text().contains("no pruning number"));
}

#[test]
fn comparing_a_manifest_with_itself_plans_nothing_and_claims_no_saving() {
    let r = explain_against(&fixture(), Some(&fixture().join("target"))).unwrap();
    let c = r.comparison.unwrap();
    assert!(c.changed.is_empty() && c.freshet.is_empty());
    assert!(c.vs_last_run.is_none() && c.vs_state_modified_plus.is_none(), "an empty plan is not a saving");
}

#[test]
fn the_text_report_leads_with_the_plan_and_the_two_comparisons() {
    let text = report().render_text();
    for line in [
        "Freshet would rebuild 7 model(s):",
        "stg_orders              sql — only [amount]",
        "vs the last `dbt build` (9 model(s)): 2 fewer — 22.2% avoided",
        "vs state:modified+ (Freshet's approximation) (8 model(s)): 1 fewer — 12.5% avoided",
        "it builds, Freshet does not: mart_customers",
    ] {
        assert!(text.contains(line), "missing {line:?} in:\n{text}");
    }
}

#[test]
fn a_target_directory_or_a_manifest_file_is_accepted_too() {
    let via_target = explain_against(&fixture().join("target"), Some(&fixture().join("state/manifest.json"))).unwrap();
    assert_eq!(rebuilt(&via_target), rebuilt(&report()));
}

#[test]
fn a_project_without_a_manifest_says_how_to_make_one() {
    let dir = std::env::temp_dir().join(format!("freshet-dbt-empty-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let err = explain_against(&dir, None).unwrap_err().to_string();
    assert!(err.contains("dbt parse") && err.contains("dbt compile"), "{err}");
    let err = explain_against(&fixture(), Some(&dir)).unwrap_err().to_string();
    assert!(err.contains("--state"), "{err}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn without_run_results_the_baseline_is_every_model() {
    let dir = std::env::temp_dir().join(format!("freshet-dbt-no-run-{}", std::process::id()));
    std::fs::create_dir_all(dir.join("target")).unwrap();
    std::fs::copy(fixture().join("target/manifest.json"), dir.join("target/manifest.json")).unwrap();
    let r = explain_against(&dir, Some(&fixture().join("state"))).unwrap();
    assert_eq!(r.last_run.source, "every_model");
    assert_eq!(r.last_run.models, 9);
    assert!(r.notes.iter().any(|n| n.contains("No target/run_results.json")));
    std::fs::remove_dir_all(&dir).unwrap();
}
