//! One scenario per rule, each on a manifest small enough to read in the test.
//!
//! Compiled SQL here names relations the way dbt renders them for Postgres:
//! `"db"."s"."<alias>"` for models and seeds, `"raw"."<source>"."<table>"` for sources.

use std::collections::{BTreeMap, BTreeSet};

use freshet_core::{plan_backfill, ColumnRef};
use freshet_dbt::{explain, Artifacts, DbtProject, ExplainReport, Lineage, Manifest, RunResults, SqlBasis};
use serde_json::{json, Map, Value};

const PKG: &str = "pkg";

fn uid(model: &str) -> String {
    format!("model.{PKG}.{model}")
}

/// A model node: a `table` whose compiled SQL is `sql`.
#[derive(Clone)]
struct M(Value);

impl M {
    fn new(name: &str, sql: &str) -> Self {
        M(json!({
            "unique_id": uid(name), "resource_type": "model", "name": name, "package_name": PKG,
            "alias": name, "database": "db", "schema": "s",
            "relation_name": format!("\"db\".\"s\".\"{name}\""),
            "raw_code": sql, "compiled_code": sql, "language": "sql",
            "config": {"materialized": "table"},
            "depends_on": {"macros": [], "nodes": []},
            "columns": {},
            "checksum": {"name": "sha256", "checksum": name},
        }))
    }
    fn reads(mut self, nodes: &[&str]) -> Self {
        self.0["depends_on"]["nodes"] = json!(nodes);
        self
    }
    fn set(mut self, key: &str, value: Value) -> Self {
        self.0[key] = value;
        self
    }
    fn materialized(mut self, materialized: &str) -> Self {
        self.0["config"]["materialized"] = json!(materialized);
        self
    }
    fn raw_only(mut self, raw: &str) -> Self {
        self.0["raw_code"] = json!(raw);
        self.0.as_object_mut().unwrap().remove("compiled_code");
        self
    }
    fn calls(mut self, macros: &[&str]) -> Self {
        self.0["depends_on"]["macros"] = json!(macros);
        self
    }
}

#[derive(Clone, Default)]
struct Dbt {
    nodes: Map<String, Value>,
    sources: Map<String, Value>,
    macros: Map<String, Value>,
}

impl Dbt {
    fn model(mut self, m: M) -> Self {
        self.nodes.insert(m.0["unique_id"].as_str().unwrap().to_string(), m.0);
        self
    }
    fn source(mut self, source: &str, table: &str, columns: &[&str]) -> Self {
        let id = format!("source.{PKG}.{source}.{table}");
        let cols: Map<String, Value> = columns.iter().map(|c| (c.to_string(), json!({ "name": c }))).collect();
        self.sources.insert(
            id.clone(),
            json!({
                "unique_id": id, "resource_type": "source", "name": table, "source_name": source,
                "package_name": PKG, "identifier": table, "database": "raw", "schema": source,
                "relation_name": format!("\"raw\".\"{source}\".\"{table}\""), "columns": cols,
            }),
        );
        self
    }
    fn seed(mut self, name: &str, contents: &str, columns: &[&str]) -> Self {
        let id = format!("seed.{PKG}.{name}");
        let cols: Map<String, Value> = columns.iter().map(|c| (c.to_string(), json!({ "name": c }))).collect();
        self.nodes.insert(
            id.clone(),
            json!({
                "unique_id": id, "resource_type": "seed", "name": name, "package_name": PKG, "alias": name,
                "relation_name": format!("\"db\".\"s\".\"{name}\""), "raw_code": "",
                "checksum": {"name": "sha256", "checksum": contents}, "columns": cols,
            }),
        );
        self
    }
    fn macro_def(mut self, id: &str, sql: &str, calls: &[&str]) -> Self {
        self.macros.insert(id.to_string(), json!({"macro_sql": sql, "depends_on": {"macros": calls}}));
        self
    }
    fn manifest(&self) -> Manifest {
        serde_json::from_value(json!({
            "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"},
            "nodes": self.nodes, "sources": self.sources, "macros": self.macros,
        }))
        .expect("a manifest")
    }
    fn project(&self) -> DbtProject {
        DbtProject::build(&self.manifest())
    }
}

fn plan(p: &DbtProject, model: &str, columns: &[&str]) -> Vec<String> {
    let changed = BTreeMap::from([(model.to_string(), columns.iter().map(|c| c.to_string()).collect::<BTreeSet<_>>())]);
    plan_backfill(&p.project, &changed).models
}

fn compare(before: &Dbt, after: &Dbt) -> ExplainReport {
    explain(&after.manifest(), Some(&before.manifest()), None, Artifacts::default())
}

fn model_level_reason(p: &DbtProject, model: &str) -> String {
    match &p.models[model].lineage {
        Lineage::ModelLevel { reason } => reason.clone(),
        Lineage::Columns => panic!("{model} kept column-level lineage"),
    }
}

// ------------------------------------------------------------------ lineage --

#[test]
fn two_relations_sharing_a_name_leave_their_readers_on_model_level_lineage() {
    // Same alias, different schemas: compiled SQL's last identifier cannot tell them apart.
    let dbt = Dbt::default()
        .source("app", "orders", &["id", "amount"])
        .model(
            M::new("orders_eu", "select id, amount from \"raw\".\"app\".\"orders\"")
                .reads(&["source.pkg.app.orders"])
                .set("alias", json!("orders"))
                .set("relation_name", json!("\"db\".\"eu\".\"orders\"")),
        )
        .model(
            M::new("orders_us", "select id, amount from \"raw\".\"app\".\"orders\"")
                .reads(&["source.pkg.app.orders"])
                .set("alias", json!("orders"))
                .set("relation_name", json!("\"db\".\"us\".\"orders\"")),
        )
        .model(M::new("report", "select id, amount from \"db\".\"eu\".\"orders\"").reads(&[&uid("orders_eu")]));
    let p = dbt.project();
    assert!(model_level_reason(&p, "report").contains("`orders`"));
    assert!(plan(&p, "orders_eu", &["amount"]).contains(&"report".to_string()));
}

#[test]
fn an_ephemeral_model_dbt_inlined_as_a_cte_still_carries_its_changes_to_its_reader() {
    let flags_sql = "select id, kind = 'x' as is_x from \"raw\".\"app\".\"events\"";
    let dbt = Dbt::default()
        .source("app", "events", &["id", "kind"])
        .model(
            M::new("flags", flags_sql)
                .reads(&["source.pkg.app.events"])
                .materialized("ephemeral")
                .set("relation_name", Value::Null),
        )
        .model(
            M::new(
                "counts",
                &format!(
                    "with __dbt__cte__flags as ({flags_sql}) \
                     select count(id) as n from __dbt__cte__flags where is_x"
                ),
            )
            .reads(&[&uid("flags")]),
        );
    let p = dbt.project();
    assert!(plan(&p, "flags", &["id"]).contains(&"counts".to_string()), "a change to flags.id was not carried");
    // `id` traces through the inlined CTE to the source, past `flags` itself, so every
    // column of the ephemeral model is counted against the reader.
    assert_eq!(p.models["counts"].lineage, Lineage::Columns);
    let n = &p.project.get("counts").unwrap().columns[0];
    assert!(n.upstream.contains(&ColumnRef::new("flags", "id")), "{:?}", n.upstream);
}

#[test]
fn raw_sql_whose_only_jinja_is_ref_and_source_is_analyzed_without_compiled_sql() {
    let dbt = Dbt::default()
        .source("app", "orders", &["id", "amount"])
        .model(
            M::new("stg", "")
                .raw_only("select id, amount from {{ source('app', 'orders') }}")
                .reads(&["source.pkg.app.orders"]),
        )
        .model(
            M::new("mart", "")
                .raw_only("select s.amount * 2 as doubled from {{ ref('stg') }} s")
                .reads(&[&uid("stg")]),
        );
    let p = dbt.project();
    assert_eq!(p.models["mart"].basis, SqlBasis::Raw);
    assert_eq!(p.models["mart"].lineage, Lineage::Columns);
    assert_eq!(p.project.get("mart").unwrap().columns[0].upstream, vec![ColumnRef::new("stg", "amount")]);
}

#[test]
fn a_ref_with_arguments_beyond_the_name_is_not_rewritten_and_the_model_falls_back() {
    let dbt = Dbt::default()
        .model(M::new("stg", "select 1 as id"))
        .model(M::new("mart", "").raw_only("select id from {{ ref('stg', v=2) }}").reads(&[&uid("stg")]));
    let p = dbt.project();
    assert!(model_level_reason(&p, "mart").contains("Jinja"));
    // No documented columns: one placeholder, never none.
    let cols: Vec<&str> = p.project.get("mart").unwrap().columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec![freshet_dbt::UNKNOWN_COLUMNS]);
    assert!(plan(&p, "stg", &["id"]).contains(&"mart".to_string()));
}

#[test]
fn a_join_on_a_column_a_cte_renamed_is_not_traced_by_name() {
    // `o.customer_id` is stg's `user_id`; by name it would look like customers' column,
    // and a change to stg.user_id — which re-pairs every order with a customer — would
    // reach neither projected column.
    let dbt = Dbt::default()
        .model(M::new("stg", "select 1 as id, 2 as user_id"))
        .model(M::new("customers", "select 2 as customer_id, 'x' as name"))
        .model(
            M::new(
                "mart",
                "with o as (select id, user_id as customer_id from \"db\".\"s\".\"stg\") \
                 select o.id, c.name from o join \"db\".\"s\".\"customers\" c on o.customer_id = c.customer_id",
            )
            .reads(&[&uid("stg"), &uid("customers")]),
        );
    let p = dbt.project();
    assert!(plan(&p, "stg", &["user_id"]).contains(&"mart".to_string()), "the join key's change was not carried");
    assert!(model_level_reason(&p, "mart").contains("customer_id"));
}

#[test]
fn a_column_with_unknown_lineage_passes_every_change_on_to_its_readers() {
    // d.m reads `u.c`, a column u's derived schema does not have, so its lineage cannot
    // be proven; e reads only d.m. Changing u.a also hits d.x, whose lineage is known —
    // and a planner told only "x is known, m is unknown" reports just x as changed,
    // leaving e behind.
    let dbt = Dbt::default()
        .model(M::new("u", "select 1 as a, 2 as b"))
        .model(M::new("d", "select u.a as x, u.c as m from \"db\".\"s\".\"u\" u").reads(&[&uid("u")]))
        .model(M::new("e", "select m from \"db\".\"s\".\"d\"").reads(&[&uid("d")]));
    let p = dbt.project();
    assert_eq!(p.models["d"].lineage, Lineage::Columns, "x keeps its precise lineage");
    assert!(plan(&p, "u", &["a"]).contains(&"e".to_string()));
    assert!(plan(&p, "u", &["b"]).contains(&"e".to_string()));
}

#[test]
fn a_cross_join_is_model_level_because_every_row_depends_on_both_sides() {
    let dbt = Dbt::default()
        .model(M::new("a", "select 1 as x"))
        .model(M::new("b", "select 2 as y"))
        .model(
            M::new("ab", "select a.x from \"db\".\"s\".\"a\" a cross join \"db\".\"s\".\"b\" b")
                .reads(&[&uid("a"), &uid("b")]),
        );
    let p = dbt.project();
    assert!(model_level_reason(&p, "ab").contains("CROSS JOIN"));
    assert!(plan(&p, "b", &["y"]).contains(&"ab".to_string()));
}

#[test]
fn a_python_model_falls_back_to_model_level_lineage() {
    let dbt = Dbt::default()
        .model(M::new("stg", "select 1 as id"))
        .model(
            M::new("scored", "def model(dbt, session): return dbt.ref('stg')")
                .set("language", json!("python"))
                .reads(&[&uid("stg")]),
        );
    let p = dbt.project();
    assert!(model_level_reason(&p, "scored").contains("python"));
}

#[test]
fn a_model_that_reads_only_sources_keeps_column_lineage_even_when_they_are_undocumented() {
    let dbt = Dbt::default()
        .source("app", "orders", &[])
        .model(M::new("stg", "select id, amount from \"raw\".\"app\".\"orders\"").reads(&["source.pkg.app.orders"]));
    assert_eq!(dbt.project().models["stg"].lineage, Lineage::Columns);
}

#[test]
fn versions_of_one_model_get_their_own_planner_names() {
    let dbt = Dbt::default()
        .model(M::new("dim", "select 1 as id").set("unique_id", json!("model.pkg.dim.v1")).set("version", json!(1)))
        .model(M::new("dim", "select 1 as id").set("unique_id", json!("model.pkg.dim.v2")).set("version", json!("2")));
    let names: Vec<String> = dbt.project().models.into_keys().collect();
    assert_eq!(names, vec!["dim.v1", "dim.v2"]);
}

// ------------------------------------------------------------ what changed --

fn stg_and_marts(stg_sql: &str) -> Dbt {
    Dbt::default()
        .source("app", "orders", &["id", "amount"])
        .model(M::new("stg", stg_sql).reads(&["source.pkg.app.orders"]))
        .model(M::new("mart_ids", "select id from \"db\".\"s\".\"stg\"").reads(&[&uid("stg")]))
        .model(M::new("mart_amounts", "select sum(amount) as total from \"db\".\"s\".\"stg\"").reads(&[&uid("stg")]))
}

fn rebuilt(r: &ExplainReport) -> Vec<String> {
    r.comparison.as_ref().unwrap().freshet.iter().map(|x| x.model.clone()).collect()
}

#[test]
fn an_edit_to_one_expression_rebuilds_only_what_reads_that_column() {
    let before = stg_and_marts("select id, amount from \"raw\".\"app\".\"orders\"");
    let after = stg_and_marts("select id, amount * 100 as amount from \"raw\".\"app\".\"orders\"");
    let r = compare(&before, &after);
    assert_eq!(r.comparison.as_ref().unwrap().changed[0].columns, Some(vec!["amount".to_string()]));
    assert_eq!(rebuilt(&r), vec!["stg", "mart_amounts"]);
}

#[test]
fn an_edit_to_a_filter_changes_every_column_so_every_reader_rebuilds() {
    let before = stg_and_marts("select id, amount from \"raw\".\"app\".\"orders\"");
    let after = stg_and_marts("select id, amount from \"raw\".\"app\".\"orders\" where amount > 0");
    let r = compare(&before, &after);
    let stg = &r.comparison.as_ref().unwrap().changed[0];
    assert_eq!(stg.columns, None);
    assert!(stg.all_columns_because.as_deref().unwrap().contains("structure"));
    assert_eq!(rebuilt(&r), vec!["stg", "mart_amounts", "mart_ids"]);
}

#[test]
fn a_changed_seed_changes_every_model_that_reads_it() {
    let with_seed = |contents: &str| {
        Dbt::default()
            .seed("fx", contents, &["ccy", "rate"])
            .model(M::new("rates", "select ccy, rate * 2 as doubled from \"db\".\"s\".\"fx\"").reads(&["seed.pkg.fx"]))
    };
    let r = compare(&with_seed("aaa"), &with_seed("bbb"));
    let changed = &r.comparison.as_ref().unwrap().changed;
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].because, vec!["input seed.pkg.fx"]);
}

#[test]
fn a_changed_macro_changes_every_model_that_calls_it_even_through_another_macro() {
    let with_macro = |body: &str| {
        Dbt::default()
            .macro_def("macro.pkg.cents", body, &[])
            .macro_def("macro.pkg.money", "{{ cents(x) }}", &["macro.pkg.cents"])
            .model(M::new("priced", "select 1 as price").calls(&["macro.pkg.money"]))
            .model(M::new("plain", "select 1 as id"))
    };
    let r = compare(&with_macro("x / 100"), &with_macro("x / 100.0"));
    let changed = &r.comparison.as_ref().unwrap().changed;
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].model, "priced");
    assert_eq!(changed[0].because, vec!["macro macro.pkg.money"]);
}

#[test]
fn a_materialization_change_is_a_change() {
    let with = |m: &str| Dbt::default().model(M::new("t", "select 1 as id").materialized(m));
    let r = compare(&with("view"), &with("table"));
    assert_eq!(r.comparison.as_ref().unwrap().changed[0].because, vec!["materialized: view -> table"]);
}

#[test]
fn an_ephemeral_model_is_never_counted_as_a_rebuild() {
    let with = |expr: &str| {
        let flags = format!("select id, {expr} as is_x from \"raw\".\"app\".\"events\"");
        Dbt::default()
            .source("app", "events", &["id", "kind"])
            .model(
                M::new("flags", &flags)
                    .reads(&["source.pkg.app.events"])
                    .materialized("ephemeral")
                    .set("relation_name", Value::Null),
            )
            .model(
                M::new("counts", &format!("with __dbt__cte__flags as ({flags}) select count(id) as n from __dbt__cte__flags"))
                    .reads(&[&uid("flags")]),
            ) // a reader, so a change to the ephemeral model has somewhere to go
    };
    let r = compare(&with("kind = 'x'"), &with("kind = 'y'"));
    assert_eq!(r.graph.ephemeral, 1);
    assert_eq!(rebuilt(&r), vec!["counts"], "the ephemeral model changed, but only its reader is built");
}

// ------------------------------------------------------------ the artifacts --

#[test]
fn run_results_written_by_a_compile_are_not_a_run() {
    let dbt = Dbt::default().model(M::new("t", "select 1 as id"));
    let run_results: RunResults = serde_json::from_value(json!({
        "args": {"which": "compile"},
        "results": [{"unique_id": uid("t"), "status": "success"}],
    }))
    .unwrap();
    let r = explain(&dbt.manifest(), Some(&dbt.manifest()), Some(&run_results), Artifacts::default());
    assert_eq!((r.last_run.source.as_str(), r.last_run.command.as_deref()), ("every_model", Some("compile")));
    assert!(r.notes.iter().any(|n| n.contains("`dbt compile`")));
}

#[test]
fn a_skipped_model_is_not_counted_as_built() {
    let dbt = Dbt::default().model(M::new("a", "select 1 as id")).model(M::new("b", "select 1 as id"));
    let run_results: RunResults = serde_json::from_value(json!({
        "args": {"which": "run"},
        "results": [
            {"unique_id": uid("a"), "status": "success"},
            {"unique_id": uid("b"), "status": "skipped"},
            {"unique_id": uid("gone"), "status": "success"},
        ],
    }))
    .unwrap();
    let r = explain(&dbt.manifest(), None, Some(&run_results), Artifacts::default());
    assert_eq!((r.last_run.models, r.last_run.not_in_manifest), (1, 1));
}

#[test]
fn fields_the_reader_does_not_know_and_nulls_where_dbt_allows_them_are_ignored() {
    let manifest = Manifest::from_slice(
        br#"{
            "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v99.json", "shiny": true},
            "nodes": {
                "model.pkg.t": {
                    "unique_id": "model.pkg.t", "resource_type": "model", "name": "t", "package_name": "pkg",
                    "alias": null, "database": null, "schema": "s", "relation_name": null,
                    "raw_code": "select 1 as id", "compiled_code": null, "language": "sql",
                    "config": {"materialized": "table", "some_future_knob": [1, 2]},
                    "depends_on": {"macros": [], "nodes": []}, "columns": null, "version": null,
                    "a_field_from_dbt_2": {"nested": true}
                }
            },
            "sources": {}, "macros": {}, "semantic_models": {"whatever": {}}
        }"#,
    )
    .unwrap();
    let p = DbtProject::build(&manifest);
    assert_eq!(p.models["t"].basis, SqlBasis::Raw);
    assert_eq!(p.models["t"].lineage, Lineage::Columns);
}
