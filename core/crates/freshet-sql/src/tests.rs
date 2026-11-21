use super::*;
use freshet_core::plan_backfill;

fn cat(entries: &[(&str, &[&str])]) -> Catalog {
    entries
        .iter()
        .map(|(t, cols)| (t.to_string(), cols.iter().map(|c| c.to_string()).collect()))
        .collect()
}

fn known(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

fn refs(c: &Column) -> Vec<(String, String)> {
    c.upstream
        .iter()
        .map(|r| (r.model.clone(), r.column.clone()))
        .collect()
}

fn col<'a>(cols: &'a [Column], name: &str) -> &'a Column {
    cols.iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("no column {name}"))
}

#[test]
fn qualified_refs_resolve_to_exact_columns() {
    let catalog = cat(&[("raw", &["a", "b"])]);
    let l = analyze(
        "select raw.a as x, raw.b as y from raw",
        &known(&["raw"]),
        &catalog,
    )
    .unwrap();
    assert_eq!(l.depends_on, vec!["raw".to_string()]);
    assert_eq!(refs(col(&l.columns, "x")), vec![("raw".into(), "a".into())]);
    assert_eq!(refs(col(&l.columns, "y")), vec![("raw".into(), "b".into())]);
}

#[test]
fn expression_over_two_columns_lists_both() {
    let catalog = cat(&[("raw", &["a", "b"])]);
    let l = analyze("select a + b as total from raw", &known(&["raw"]), &catalog).unwrap();
    assert_eq!(
        refs(col(&l.columns, "total")),
        vec![("raw".into(), "a".into()), ("raw".into(), "b".into())]
    );
}

#[test]
fn join_uses_alias_to_disambiguate() {
    let catalog = cat(&[("orders", &["id", "amount"]), ("users", &["id", "name"])]);
    let sql = "select o.amount as amt, u.name as nm from orders o join users u on o.id = u.id";
    let l = analyze(sql, &known(&["orders", "users"]), &catalog).unwrap();
    assert_eq!(
        l.depends_on,
        vec!["orders".to_string(), "users".to_string()]
    );
    // Each column is its own value plus the join keys: which rows exist is decided
    // by `o.id = u.id`, so a change to either key reaches both columns.
    assert_eq!(
        refs(col(&l.columns, "amt")),
        vec![
            ("orders".into(), "amount".into()),
            ("orders".into(), "id".into()),
            ("users".into(), "id".into())
        ]
    );
    assert_eq!(
        refs(col(&l.columns, "nm")),
        vec![
            ("orders".into(), "id".into()),
            ("users".into(), "id".into()),
            ("users".into(), "name".into())
        ]
    );
}

#[test]
fn wildcard_expands_against_catalog() {
    let catalog = cat(&[("raw", &["a", "b"])]);
    let l = analyze("select * from raw", &known(&["raw"]), &catalog).unwrap();
    assert_eq!(
        l.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert_eq!(refs(col(&l.columns, "a")), vec![("raw".into(), "a".into())]);
}

#[test]
fn wildcard_over_unknown_schema_errors() {
    // raw not in catalog → cannot enumerate its columns.
    let err = analyze("select * from raw", &known(&["raw"]), &Catalog::new()).unwrap_err();
    assert!(err.to_string().contains("schema is unknown"), "got: {err}");
}

#[test]
fn cte_flattens_to_real_upstreams() {
    let catalog = cat(&[("raw", &["a", "b"])]);
    let sql = "with s as (select a as aa from raw) select aa as final from s";
    let l = analyze(sql, &known(&["raw"]), &catalog).unwrap();
    // depends_on excludes the CTE name `s`; only the real model `raw`.
    assert_eq!(l.depends_on, vec!["raw".to_string()]);
    // `final` traces through the CTE back to raw.a.
    assert_eq!(
        refs(col(&l.columns, "final")),
        vec![("raw".into(), "a".into())]
    );
}

#[test]
fn scalar_subquery_in_projection_is_conservative() {
    let catalog = cat(&[("raw", &["a"]), ("other", &["k"])]);
    let sql = "select a, (select max(k) from other) as m from raw";
    let l = analyze(sql, &known(&["raw", "other"]), &catalog).unwrap();
    // `m` involves a subquery → empty upstream (conservative). `a` still precise.
    assert_eq!(refs(col(&l.columns, "a")), vec![("raw".into(), "a".into())]);
    assert!(
        col(&l.columns, "m").upstream.is_empty(),
        "subquery column must be conservative"
    );
}

#[test]
fn derived_table_in_from() {
    let catalog = cat(&[("raw", &["a", "b"])]);
    let sql = "select t.a as x from (select a, b from raw) t";
    let l = analyze(sql, &known(&["raw"]), &catalog).unwrap();
    assert_eq!(refs(col(&l.columns, "x")), vec![("raw".into(), "a".into())]);
}

#[test]
fn union_merges_branch_lineage() {
    let catalog = cat(&[("a", &["x"]), ("b", &["x"])]);
    let sql = "select x from a union all select x from b";
    let l = analyze(sql, &known(&["a", "b"]), &catalog).unwrap();
    assert_eq!(
        refs(col(&l.columns, "x")),
        vec![("a".into(), "x".into()), ("b".into(), "x".into())]
    );
}

#[test]
fn build_project_links_and_prunes_end_to_end() {
    // raw(a,b) -> stg(a,b) -> mart_a(reads stg.a), mart_b(reads stg.b)
    let sources = cat(&[("raw", &["a", "b"])]);
    let models = vec![
        ("stg".to_string(), "select a, b from raw".to_string()),
        ("mart_a".to_string(), "select a as x from stg".to_string()),
        ("mart_b".to_string(), "select b as y from stg".to_string()),
    ];
    let project = build_project(&models, &sources).unwrap();

    // The derived column lineage feeds freshet-core's pruning directly.
    let only_a = BTreeMap::from([("stg".to_string(), BTreeSet::from(["a".to_string()]))]);
    let plan = plan_backfill(&project, &only_a);
    assert!(plan.models.contains(&"mart_a".to_string()));
    assert!(
        !plan.models.contains(&"mart_b".to_string()),
        "mart_b reads stg.b, not stg.a"
    );
}

#[test]
fn duplicate_model_names_error() {
    let sources = cat(&[("raw", &["a"])]);
    let models = vec![
        ("m".to_string(), "select a from raw".to_string()),
        ("m".to_string(), "select a from raw".to_string()),
    ];
    let err = build_project(&models, &sources).unwrap_err();
    assert!(
        err.to_string().contains("duplicate model name"),
        "got: {err}"
    );
}

#[test]
fn top_level_cte_is_not_a_model_dep() {
    // A top-level CTE shadows the same-named model for the whole query.
    let q = parse_single_query("with stg as (select 1 x) select x from stg").unwrap();
    assert!(referenced_models(&q, &known(&["stg"])).is_empty());
}

#[test]
fn nested_cte_does_not_hide_outer_model_dep() {
    // Outer query truly depends on model `foo`; a *nested* CTE also named `foo`
    // must not suppress that outer dependency (the dangerous under-attribution).
    let sql = "select foo.a from foo, (with foo as (select 1 b) select b from foo) t";
    let q = parse_single_query(sql).unwrap();
    assert!(
        referenced_models(&q, &known(&["foo"])).contains(&"foo".to_string()),
        "outer model dep must survive a nested CTE of the same name"
    );
}

// ---- soundness: every way a model can read an upstream reaches its lineage ----

fn project(sources: &[(&str, &[&str])], models: &[(&str, &str)]) -> Project {
    let models: Vec<(String, String)> = models
        .iter()
        .map(|(n, sql)| (n.to_string(), sql.to_string()))
        .collect();
    build_project(&models, &cat(sources)).unwrap()
}

/// Every model a direct change to `model` must rebuild.
fn rebuilt_by(p: &Project, model: &str) -> Vec<String> {
    let cols = p.get(model).unwrap().output_columns();
    plan_backfill(p, &BTreeMap::from([(model.to_string(), cols)])).models
}

#[test]
fn an_import_cte_reads_the_model_it_shadows() {
    // The dbt import-CTE idiom: inside its own body the name is the real model.
    let sql = "with orders as (select id, amount from orders) select amount from orders";
    let q = parse_single_query(sql).unwrap();
    assert_eq!(
        referenced_models(&q, &known(&["orders"])),
        vec!["orders".to_string()]
    );

    let l = analyze(
        sql,
        &known(&["orders"]),
        &cat(&[("orders", &["id", "amount"])]),
    )
    .unwrap();
    assert_eq!(l.depends_on, vec!["orders".to_string()]);
    assert_eq!(
        refs(col(&l.columns, "amount")),
        vec![("orders".into(), "amount".into())]
    );
}

#[test]
fn a_later_cte_hides_nothing_from_an_earlier_one() {
    // `b` is a CTE only from the second CTE on: the first reads the real model `b`.
    let sql = "with a as (select x from b), b as (select x from a) select x from b";
    let q = parse_single_query(sql).unwrap();
    assert_eq!(referenced_models(&q, &known(&["b"])), vec!["b".to_string()]);
}

#[test]
fn a_qualified_name_is_never_a_cte() {
    let sql = "with orders as (select 1 as x) select o.amount from analytics.orders o";
    let q = parse_single_query(sql).unwrap();
    assert_eq!(
        referenced_models(&q, &known(&["orders"])),
        vec!["orders".to_string()]
    );
}

#[test]
fn a_recursive_cte_reading_itself_is_not_a_model_dep_and_proves_nothing() {
    let sql = "with recursive t (n) as (select 1 union all select n + 1 from t where n < 5) \
               select n from t";
    let q = parse_single_query(sql).unwrap();
    assert!(referenced_models(&q, &known(&["t"])).is_empty());
    let l = analyze(sql, &known(&["t"]), &Catalog::new()).unwrap();
    assert!(col(&l.columns, "n").upstream.is_empty());
}

#[test]
fn an_unquoted_name_reads_the_model_whatever_its_case() {
    let q = parse_single_query("select x from Orders").unwrap();
    assert_eq!(
        referenced_models(&q, &known(&["orders"])),
        vec!["orders".to_string()]
    );
    let q = parse_single_query(r#"select x from "Orders""#).unwrap();
    assert!(
        referenced_models(&q, &known(&["orders"])).is_empty(),
        "a quoted name is exact"
    );
}

#[test]
fn import_cte_lineage_does_not_depend_on_model_names() {
    // The reader sorts before `orders` and uses `*`: it must still be analyzed after
    // `orders` (its edge is real), so the build neither fails nor loses lineage.
    let p = project(
        &[("raw", &["id", "amount"])],
        &[
            ("orders", "select id, amount from raw"),
            (
                "a_reader",
                "with orders as (select * from orders) select amount from orders",
            ),
        ],
    );
    let reader = p.get("a_reader").unwrap();
    assert_eq!(reader.depends_on, vec!["orders".to_string()]);
    assert_eq!(
        refs(col(&reader.columns, "amount")),
        vec![("orders".into(), "amount".into())]
    );
    assert_eq!(
        rebuilt_by(&p, "orders"),
        vec!["orders".to_string(), "a_reader".to_string()]
    );
}

#[test]
fn every_way_of_reading_a_changed_model_rebuilds_the_reader() {
    // Each reader's rows or values depend on `b`, and none of them lists a column
    // of `b` in its projection the old way: a filter, a join key, an import CTE, an
    // uncertain CTE column next to a certain one, an uncertain UNION branch.
    let p = project(
        &[("src_a", &["id", "x"]), ("src_b", &["id", "flag"])],
        &[
            ("a", "select id, x from src_a"),
            ("b", "select id, flag from src_b"),
            (
                "filtered",
                "select a.x from a join b on a.id = b.id where b.flag",
            ),
            (
                "import_cte",
                "with b as (select id, flag from b) select b.flag from b",
            ),
            (
                "mixed_cte",
                "with t as (select (select max(flag) from b) as m, a.x from a) \
                 select t.m + t.x as z from t",
            ),
            (
                "mixed_union",
                "select a.x from a union all select (select max(flag) from b) as x from a",
            ),
            ("crossed", "select a.x from a cross join b"),
            ("semi", "select a.x from a where a.id in (select id from b)"),
        ],
    );
    let rebuilt = rebuilt_by(&p, "b");
    for reader in [
        "filtered",
        "import_cte",
        "mixed_cte",
        "mixed_union",
        "crossed",
        "semi",
    ] {
        assert!(
            rebuilt.contains(&reader.to_string()),
            "{reader} reads b but was skipped: {rebuilt:?}"
        );
    }
    assert!(!rebuilt.contains(&"a".to_string()));
}

#[test]
fn a_row_count_computed_from_another_model_decides_the_rows() {
    // `cfg` reaches these readers only through how many rows they keep, or where
    // they start: no column of theirs names it. Whatever spelling the count takes,
    // a change to `cfg` changes their rows, and everything downstream of them.
    let p = project(
        &[("src_a", &["id", "x"]), ("src_cfg", &["n"])],
        &[
            ("a", "select id, x from src_a"),
            ("cfg", "select n from src_cfg"),
            (
                "limited",
                "select a.x from a order by a.x limit (select n from cfg)",
            ),
            (
                "topped",
                "select top ((select n from cfg)) a.x from a order by a.x",
            ),
            (
                "offset",
                "select a.x from a order by a.x offset (select n from cfg) rows",
            ),
            ("after_topped", "select x from topped"),
        ],
    );
    let rebuilt = rebuilt_by(&p, "cfg");
    for reader in ["limited", "topped", "offset", "after_topped"] {
        assert!(
            rebuilt.contains(&reader.to_string()),
            "{reader}'s rows depend on cfg but it was skipped: {rebuilt:?}"
        );
    }
    // A constant count decides nothing about `cfg`.
    let p = project(
        &[("src_a", &["id", "x"]), ("src_cfg", &["n"])],
        &[
            ("a", "select id, x from src_a"),
            ("cfg", "select n from src_cfg"),
            ("top_const", "select top 5 a.x from a order by a.x"),
            (
                "fetch_const",
                "select a.x from a order by a.x fetch first 5 rows only",
            ),
        ],
    );
    assert_eq!(rebuilt_by(&p, "cfg"), vec!["cfg".to_string()]);
    // sqlparser 0.52 takes only a literal FETCH count, so a computed one is refused
    // outright — never analyzed as if it decided nothing. (The analyzer checks the
    // FETCH quantity too, for the day the parser accepts an expression there.)
    let models = [
        ("cfg".to_string(), "select 1 as n".to_string()),
        (
            "fetched".to_string(),
            "select n from cfg order by n fetch first (select n from cfg) rows only".to_string(),
        ),
    ];
    assert!(build_project(&models, &Catalog::new()).is_err());
}

#[test]
fn a_changed_row_set_reaches_columns_that_never_name_the_model() {
    // `orders_enriched` joins users; `revenue` reads only its `amount`. When users'
    // rows change, the join drops orders — so revenue changes although it never
    // names a users column. Before: amount carried only orders.amount, and revenue
    // was skipped.
    let p = project(
        &[
            ("raw_orders", &["id", "uid", "amount"]),
            ("raw_users", &["id", "name"]),
        ],
        &[
            ("orders", "select id, uid, amount from raw_orders"),
            ("users", "select id, name from raw_users"),
            (
                "orders_enriched",
                "select o.id, o.amount, u.name from orders o join users u on o.uid = u.id",
            ),
            (
                "revenue",
                "select sum(amount) as total from orders_enriched",
            ),
        ],
    );
    let rebuilt = rebuilt_by(&p, "users");
    assert!(rebuilt.contains(&"revenue".to_string()), "{rebuilt:?}");
}

#[test]
fn column_pruning_still_holds_where_rows_cannot_change() {
    // A value-only change to one column of `stg` (its rows are untouched) still
    // skips the mart that reads only the other column — the precision is kept
    // exactly where it is sound.
    let p = project(
        &[("raw", &["id", "a", "b"])],
        &[
            ("stg", "select id, a, b from raw"),
            ("mart_a", "select id, a from stg"),
            ("mart_b", "select id, b from stg"),
        ],
    );
    let only_b = BTreeMap::from([("stg".to_string(), BTreeSet::from(["b".to_string()]))]);
    let plan = plan_backfill(&p, &only_b);
    assert!(plan.models.contains(&"mart_b".to_string()));
    assert!(
        !plan.models.contains(&"mart_a".to_string()),
        "{:?}",
        plan.models
    );
}

#[test]
fn row_deciding_clauses_reach_every_column() {
    let catalog = cat(&[("t", &["k", "v", "w", "ts"])]);
    let models = known(&["t"]);
    let lin = |sql: &str| analyze(sql, &models, &catalog).unwrap();
    let has = |l: &ModelLineage, c: &str, r: &str| {
        refs(col(&l.columns, c)).contains(&("t".to_string(), r.to_string()))
    };

    let l = lin("select k, sum(v) as s from t group by k");
    assert!(
        has(&l, "s", "k"),
        "grouping keys decide each aggregate's rows"
    );

    let l = lin("select k, sum(v) as s from t group by 1");
    assert!(has(&l, "s", "k"), "positional GROUP BY");

    let l = lin("select k, sum(v) as s from t group by k having sum(w) > 0");
    assert!(has(&l, "k", "w"), "HAVING decides which groups exist");

    let l = lin("select distinct k, v from t");
    assert!(
        has(&l, "k", "v") && has(&l, "v", "k"),
        "DISTINCT compares every output"
    );

    let l = lin("select k from t order by ts desc limit 10");
    assert!(
        has(&l, "k", "ts"),
        "the ORDER BY of a LIMIT decides the rows"
    );

    let l = lin("select k from t order by ts desc");
    assert!(
        !has(&l, "k", "ts"),
        "an ORDER BY alone decides nothing a table keeps"
    );

    let l = lin("select count(*) as n from t");
    assert!(
        !col(&l.columns, "n").upstream.is_empty(),
        "count(*) is the row set, which is proven"
    );
    assert!(
        has(&l, "n", "k"),
        "the row set is carried by one of t's columns"
    );

    let l = lin("select row_number() over w as rn from t window w as (partition by k order by ts)");
    assert!(
        has(&l, "rn", "k") && has(&l, "rn", "ts"),
        "a named window's terms"
    );
}

#[test]
fn a_union_that_deduplicates_compares_every_column() {
    let catalog = cat(&[("a", &["x", "y"]), ("b", &["x", "y"])]);
    let l = analyze(
        "select x, y from a union select x, y from b",
        &known(&["a", "b"]),
        &catalog,
    )
    .unwrap();
    let x = refs(col(&l.columns, "x"));
    assert!(x.contains(&("a".into(), "y".into())) && x.contains(&("b".into(), "y".into())));
}

#[test]
fn wildcard_options_rename_replace_and_drop_columns() {
    let catalog = cat(&[("t", &["k", "v", "w"])]);
    let l = analyze(
        "select * exclude (w) rename (k as key) from t",
        &known(&["t"]),
        &catalog,
    )
    .unwrap();
    assert_eq!(
        l.columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["key", "v"]
    );
    let l = analyze("select * replace (w as v) from t", &known(&["t"]), &catalog).unwrap();
    assert_eq!(refs(col(&l.columns, "v")), vec![("t".into(), "w".into())]);
}

#[test]
fn cte_column_aliases_name_the_outputs() {
    let catalog = cat(&[("t", &["k", "v"])]);
    let l = analyze(
        "with c (key, val) as (select k, v from t) select key, val from c",
        &known(&["t"]),
        &catalog,
    )
    .unwrap();
    assert_eq!(refs(col(&l.columns, "val")), vec![("t".into(), "v".into())]);
}

#[test]
fn a_table_function_source_proves_nothing_and_refuses_star() {
    let catalog = cat(&[("t", &["k", "arr"])]);
    let l = analyze(
        "select t.k, u.v from t cross join unnest(t.arr) as u (v)",
        &known(&["t"]),
        &catalog,
    )
    .unwrap();
    assert!(
        col(&l.columns, "k").upstream.is_empty(),
        "UNNEST multiplies t's rows"
    );
    let err = analyze(
        "select * from t cross join unnest(t.arr) as u (v)",
        &known(&["t"]),
        &catalog,
    )
    .unwrap_err();
    assert!(err.to_string().contains("cannot be derived"), "{err}");
}

#[test]
fn a_quoted_cte_name_hides_only_its_exact_spelling() {
    // `orders` folds to lowercase; the CTE is the case-sensitive "Orders". The query
    // reads the real model, in lineage as in `depends_on`.
    let sql = r#"with "Orders" as (select 1 as x) select amount from orders"#;
    let l = analyze(sql, &known(&["orders"]), &cat(&[("orders", &["amount"])])).unwrap();
    assert_eq!(l.depends_on, vec!["orders".to_string()]);
    assert_eq!(
        refs(col(&l.columns, "amount")),
        vec![("orders".into(), "amount".into())]
    );
}

#[test]
fn a_set_returning_function_in_the_projection_decides_the_rows() {
    // `u` has abs(v) % 3 + 1 rows per row of `a`: its row count moves with a.v even
    // when a.v changes value only. `d` reads only u.k — its rows are u's rows.
    let p = project(
        &[("src", &["k", "v"])],
        &[
            ("a", "select k, v from src"),
            (
                "u",
                "select k, v, generate_series(1, abs(v) % 3 + 1) as w from a",
            ),
            ("d", "select k from u"),
        ],
    );
    let only_v = BTreeMap::from([("a".to_string(), BTreeSet::from(["v".to_string()]))]);
    let plan = plan_backfill(&p, &only_v);
    assert!(plan.models.contains(&"d".to_string()), "{:?}", plan.models);

    // Databricks spells it explode(); Postgres and DuckDB unnest().
    for srf in ["explode(tags)", "unnest(tags)", "posexplode(tags)"] {
        let l = analyze(
            &format!("select id, {srf} as tag from t"),
            &known(&["t"]),
            &cat(&[("t", &["id", "tags"])]),
        )
        .unwrap();
        assert!(
            refs(col(&l.columns, "id")).contains(&("t".into(), "tags".into())),
            "{srf}: `id` repeats once per element of `tags`"
        );
    }
}

// ---- undeclared tables: a typo'd reference is not an external table ----------

fn pairs(models: &[(&str, &str)]) -> Vec<(String, String)> {
    models
        .iter()
        .map(|(n, sql)| (n.to_string(), sql.to_string()))
        .collect()
}

/// `(model, table, candidates)` for every undeclared table, lenient build.
fn undeclared(
    sources: &[(&str, &[&str])],
    models: &[(&str, &str)],
) -> Vec<(String, String, Vec<String>)> {
    build_project_with(&pairs(models), &cat(sources), Undeclared::External)
        .unwrap()
        .undeclared
        .into_iter()
        .map(|u| (u.model, u.table, u.candidates))
        .collect()
}

fn refused(sources: &[(&str, &[&str])], models: &[(&str, &str)]) -> UndeclaredTables {
    let err = build_project_with(&pairs(models), &cat(sources), Undeclared::Refuse).unwrap_err();
    err.downcast::<UndeclaredTables>()
        .unwrap_or_else(|e| panic!("not an UndeclaredTables refusal: {e:#}"))
}

#[test]
fn a_misspelled_model_is_refused_with_the_model_it_meant() {
    // The shortfall exactly: `stgg` was an external table, so `mart` had no edge to
    // `stg` and the two ran side by side.
    let models = [("stg", "select a from raw"), ("mart", "select a from stgg")];
    let lenient = build_project(&pairs(&models), &cat(&[("raw", &["a"])])).unwrap();
    assert!(
        lenient.get("mart").unwrap().depends_on.is_empty(),
        "the hazard this guards"
    );

    let err = refused(&[("raw", &["a"])], &models);
    assert_eq!(
        err.0,
        vec![UndeclaredTable {
            model: "mart".into(),
            table: "stgg".into(),
            candidates: vec!["stg".into()],
        }]
    );
    let text = err.to_string();
    assert!(
        text.starts_with("model `mart` reads `stgg`, which is neither a model nor a declared source: did you mean `stg`?"),
        "got: {text}"
    );
}

#[test]
fn the_lenient_build_reports_what_the_strict_one_refuses() {
    let models = [("stg", "select a from raw"), ("mart", "select a from stgg")];
    let built = build_project_with(
        &pairs(&models),
        &cat(&[("raw", &["a"])]),
        Undeclared::External,
    )
    .unwrap();
    assert_eq!(built.project.names().count(), 2);
    assert_eq!(built.undeclared.len(), 1);
    assert_eq!(built.undeclared[0].candidates, vec!["stg".to_string()]);
}

#[test]
fn models_and_declared_sources_are_not_undeclared() {
    let none = undeclared(
        &[("raw", &["a"]), ("events", &[])],
        &[
            ("stg", "select a from raw join events using (a)"),
            // Unquoted names fold; a schema does not hide the model.
            ("m1", "select a from STG"),
            ("m2", "select a from analytics.stg"),
            ("m3", "select a from \"db\".\"public\".raw"),
        ],
    );
    assert!(none.is_empty(), "got: {none:?}");
}

#[test]
fn each_undeclared_table_is_named_once_per_model_as_written() {
    let got = undeclared(
        &[],
        &[
            (
                "a",
                "select x from raw.events e join raw.events f using (x)",
            ),
            ("b", "select x from raw.events"),
        ],
    );
    assert_eq!(
        got,
        vec![
            ("a".into(), "raw.events".into(), vec![]),
            ("b".into(), "raw.events".into(), vec![]),
        ]
    );
}

#[test]
fn a_cte_is_not_a_table_where_sql_resolves_the_name_to_it() {
    let none = undeclared(
        &[("raw", &["x"])],
        &[
            ("top", "with t as (select x from raw) select x from t"),
            ("nested", "select x from (with t as (select x from raw) select x from t) s"),
            ("in_where", "select x from raw where x in (with t as (select x from raw) select x from t)"),
            // A query nested in a later CTE's body sees the earlier CTEs.
            ("later", "with a as (select x from raw), b as (select x from (select x from a) s) select x from b"),
            ("outer", "with a as (select x from raw) select x from (select x from a) s"),
            ("recursive", "with recursive r as (select x from raw union all select x from r) select x from r"),
        ],
    );
    assert!(none.is_empty(), "got: {none:?}");
}

#[test]
fn a_cte_does_not_hide_a_table_where_sql_reads_the_table() {
    let got = undeclared(
        &[],
        &[
            // The import CTE: its own body reads the real `orders`.
            (
                "import",
                "with orders as (select id from orders) select id from orders",
            ),
            // A non-recursive CTE cannot see the CTEs after it.
            (
                "ahead",
                "with a as (select x from b), b as (select 1 as x) select x from a",
            ),
            // A nested CTE is not visible to the outer query.
            (
                "escaped",
                "select x from (with t as (select 1 as x) select x from t) s join t using (x)",
            ),
            // A qualified name is never a CTE.
            ("qualified", "with t as (select 1 as x) select x from s.t"),
        ],
    );
    let tables: Vec<(&str, &str)> = got
        .iter()
        .map(|(m, t, _)| (m.as_str(), t.as_str()))
        .collect();
    assert_eq!(
        tables,
        vec![
            ("import", "orders"),
            ("ahead", "b"),
            ("escaped", "t"),
            ("qualified", "s.t")
        ]
    );
}

#[test]
fn every_place_a_table_can_be_read_is_checked() {
    let got = undeclared(
        &[("raw", &["x"])],
        &[
            (
                "subquery",
                "select x from raw where x in (select x from stgg)",
            ),
            (
                "exists",
                "select x from raw where exists (select 1 from stgg)",
            ),
            ("union", "select x from raw union all select x from stgg"),
            ("join", "select raw.x from raw join stgg on raw.x = stgg.x"),
            ("scalar", "select (select max(x) from stgg) as m from raw"),
        ],
    );
    let models: Vec<&str> = got.iter().map(|(m, _, _)| m.as_str()).collect();
    assert_eq!(
        models,
        vec!["subquery", "exists", "union", "join", "scalar"]
    );
}

#[test]
fn a_table_function_is_not_a_table() {
    let none = undeclared(&[], &[("g", "select x from generate_series(1, 3) as g(x)")]);
    assert!(none.is_empty(), "got: {none:?}");
}

#[test]
fn every_undeclared_table_is_refused_at_once() {
    let err = refused(
        &[],
        &[
            ("stg", "select 1 as a"),
            ("m1", "select a from stgg"),
            ("m2", "select a from sgt join raw_users using (a)"),
        ],
    );
    assert_eq!(err.0.len(), 3);
    let text = err.to_string();
    assert!(
        text.starts_with("3 tables are neither models nor declared sources:"),
        "got: {text}"
    );
    assert!(text.contains("\n  model `m2` reads `sgt`, which is neither a model nor a declared source: did you mean `stg`?"));
    assert!(text.contains(
        "\n  model `m2` reads `raw_users`, which is neither a model nor a declared source\n"
    ));
    assert!(
        text.ends_with("an empty column list declares it without its columns)"),
        "got: {text}"
    );
}

#[test]
fn an_empty_column_list_declares_a_source_whose_columns_are_unknown() {
    let sources = cat(&[("events", &[])]);
    let built = build_project_with(
        &pairs(&[("m", "select user_id from events")]),
        &sources,
        Undeclared::Refuse,
    )
    .unwrap();
    // Unknown columns, so nothing about `user_id` is proven: the safe answer.
    assert!(built.project.get("m").unwrap().columns[0]
        .upstream
        .is_empty());

    // And `*` over it is refused, never expanded to no columns at all.
    let err = build_project(&pairs(&[("m", "select * from events")]), &sources).unwrap_err();
    assert!(
        format!("{err:#}").contains("schema is unknown"),
        "got: {err:#}"
    );
}

#[test]
fn star_over_a_model_with_no_derivable_columns_still_expands_to_none() {
    // An empty list means "declared without its columns" only for a source, which
    // someone wrote down. A model whose columns cannot be derived (a VALUES body) is
    // the planner's own reading, and `*` over it expands to nothing, as it always did.
    let built = build_project(
        &pairs(&[("v", "values (1, 2)"), ("m", "select * from v")]),
        &Catalog::new(),
    )
    .expect("a model with no derivable columns is not a source to declare");
    assert!(built.get("m").unwrap().columns.is_empty());
    assert_eq!(built.get("m").unwrap().depends_on, ["v"]);
}

#[test]
fn candidates_are_the_closest_names_ignoring_case_and_at_most_three() {
    let got = undeclared(
        &[("orders", &["a"]), ("order_items", &["a"])],
        &[
            ("ordrs", "select 1 as a"),
            ("Orderz", "select 1 as a"),
            ("odrers", "select 1 as a"),
            ("ordersx", "select 1 as a"),
            ("m", "select a from \"ORDERS\""),
            ("n", "select a from xyz"),
        ],
    );
    assert_eq!(got.len(), 2, "got: {got:?}");
    // A quoted name does not fold, but case is still the likeliest typo. The
    // fourth name, one edit away like the rest, is left out; `order_items`, more
    // than one edit away, is never in.
    assert_eq!(got[0].1, "\"ORDERS\"");
    assert_eq!(
        got[0].2,
        vec!["orders".to_string(), "Orderz".into(), "odrers".into()]
    );
    assert_eq!(got[1].2, Vec::<String>::new());
}

fn edits(a: &str, b: &str, bound: usize) -> Option<usize> {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    edits_within(&a, &b, bound)
}

#[test]
fn the_index_finds_every_name_one_edit_away_and_no_other() {
    // Every one-edit change of `orders`, and a few two-edit ones.
    let base = "orders";
    let mut near: BTreeSet<String> = BTreeSet::new();
    let chars: Vec<char> = base.chars().collect();
    for i in 0..=chars.len() {
        for c in ['x', 'r', 'S'] {
            let mut ins = chars.clone();
            ins.insert(i, c);
            near.insert(ins.into_iter().collect());
        }
        if i < chars.len() {
            let mut del = chars.clone();
            del.remove(i);
            near.insert(del.iter().collect());
            let mut sub = chars.clone();
            sub[i] = 'q';
            near.insert(sub.into_iter().collect());
        }
        if i + 1 < chars.len() {
            let mut swap = chars.clone();
            swap.swap(i, i + 1);
            near.insert(swap.into_iter().collect());
        }
    }
    let far = ["ordxrsx", "osdxrs", "redros", "ord", "order_items"].map(String::from);
    let names: Vec<String> = near.iter().chain(&far).cloned().collect();
    let index = Spellings::of(names.iter());
    // The lookup returns at most three, so ask from each name's side instead.
    for name in &near {
        assert_eq!(
            Spellings::of(std::iter::once(name)).within_one_edit(base),
            vec![name.clone()],
            "{name}"
        );
    }
    for name in &far {
        assert!(
            Spellings::of(std::iter::once(name))
                .within_one_edit(base)
                .is_empty(),
            "{name}"
        );
    }
    assert_eq!(index.within_one_edit(base).len(), 3);
}

#[test]
fn edit_distance_counts_an_adjacent_swap_as_one_edit() {
    assert_eq!(edits("stg", "sgt", 1), Some(1));
    assert_eq!(edits("stg", "stgg", 1), Some(1));
    assert_eq!(edits("stg", "stg", 1), Some(0));
    assert_eq!(edits("foo", "bar", 1), None);
    assert_eq!(edits("ab", "abcd", 1), None);
    assert_eq!(edits("kitten", "sitting", 3), Some(3));
    assert_eq!(edits("kitten", "sitting", 2), None);
    assert_eq!(edits("", "ab", 2), Some(2));
    assert_eq!(edits("ab", "", 2), Some(2));
    assert_eq!(
        edits("ca", "abc", 3),
        Some(3),
        "optimal string alignment, not Damerau"
    );
}

/// The banded, early-exit distance agrees with the full table on every pair of
/// short strings over a small alphabet, at every bound.
#[test]
fn edit_distance_agrees_with_the_full_table() {
    fn full(a: &[char], b: &[char]) -> usize {
        let mut d = vec![vec![0usize; b.len() + 1]; a.len() + 1];
        for (i, row) in d.iter_mut().enumerate() {
            row[0] = i;
        }
        for (j, cell) in d[0].iter_mut().enumerate() {
            *cell = j;
        }
        for i in 1..=a.len() {
            for j in 1..=b.len() {
                let cost = usize::from(a[i - 1] != b[j - 1]);
                d[i][j] = (d[i - 1][j] + 1)
                    .min(d[i][j - 1] + 1)
                    .min(d[i - 1][j - 1] + cost);
                if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                    d[i][j] = d[i][j].min(d[i - 2][j - 2] + 1);
                }
            }
        }
        d[a.len()][b.len()]
    }
    let mut words: Vec<Vec<char>> = vec![vec![]];
    for _ in 0..4 {
        let longer: Vec<Vec<char>> = words
            .iter()
            .filter(|w| w.len() == words.last().unwrap().len())
            .flat_map(|w| "abc".chars().map(move |c| [w.clone(), vec![c]].concat()))
            .collect();
        words.extend(longer);
    }
    for a in &words {
        for b in &words {
            let d = full(a, b);
            for bound in 1..=3 {
                assert_eq!(
                    edits_within(a, b, bound),
                    (d <= bound).then_some(d),
                    "{a:?} {b:?} {bound}"
                );
            }
        }
    }
}
