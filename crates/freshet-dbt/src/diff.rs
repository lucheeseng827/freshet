//! Which models changed between a `--state` manifest and the current one, and — when it
//! can be proven — which of their columns.
//!
//! ## Changed models
//!
//! Both manifests go through the same project-building path, with the same SQL basis
//! per model (compiled SQL only when *both* sides have it; otherwise both sides read
//! their raw SQL), and each model is fingerprinted with
//! [`freshet_fingerprint::AstFingerprinter`] — SQL + column names + upstream names,
//! exactly what `freshet plan` compares. The SQL is taken with every relation name
//! replaced by the node's unique_id, so a production manifest and a development one
//! do not differ merely because they build into different schemas.
//!
//! A fingerprint only covers a model's own definition, so four more things count as a
//! change, each the way dbt's own `state:modified` counts it: the materialization, the
//! relation it builds (alias / configured schema / database), a macro it calls (or one
//! those call), and a non-model input it reads (a seed's contents, a snapshot, a
//! source's location, a function). New models are changed; removed ones are listed.
//!
//! ## Changed columns
//!
//! The planner core marks *every* output column of a changed model as changed, and
//! with row-level references folded into lineage that is honest but coarse: every
//! downstream that reads the model at all rebuilds. When the only difference between
//! the two versions is inside individual projection expressions — `amount_cents / 100`
//! became `amount_cents / 100.0`, and no filter, join, grouping or other structure
//! moved — only the columns those expressions flow into changed. [`changed_columns`]
//! proves that or gives up; giving up means every column, which is the planner's own
//! default.

use std::collections::BTreeSet;

use freshet_core::Model;
use freshet_fingerprint::{normalize_sql_fallback, try_canonical_sql, AstFingerprinter};

use crate::artifacts::Manifest;
use crate::project::{DbtProject, Lineage, SqlBasis};
use crate::sqlscan;

/// The reason given when only the compiled SQL changed and the source file did not.
pub(crate) const RENDERED_ONLY: &str = "compiled sql only (source unchanged)";

/// A directly-changed model.
#[derive(Clone, Debug)]
pub struct Changed {
    pub model: String,
    /// Why, in the report's words: `new model`, `sql`, `columns`, `materialized: view -> table`, …
    pub because: Vec<String>,
    /// The output columns that changed; `None` means every one.
    pub columns: Option<BTreeSet<String>>,
    /// Why `columns` is `None` for a SQL-only change, when a narrower set was tried.
    pub all_columns_because: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct Diff {
    pub changed: Vec<Changed>,
    /// Planner names (as the `--state` manifest had them) of models that are gone.
    pub removed: Vec<String>,
}

pub(crate) fn diff(cur: &DbtProject, prev: &DbtProject, cur_m: &Manifest, prev_m: &Manifest) -> Diff {
    let macros = changed_macros(cur_m, prev_m);
    let fingerprinter = AstFingerprinter::new();
    let fingerprint = |m: &Model| fingerprinter.fingerprint_model(m).fingerprint;
    let mut out = Diff::default();

    for (name, model) in &cur.models {
        let Some(prev_name) = prev.name_of(&model.unique_id) else {
            out.changed.push(Changed {
                model: name.clone(),
                because: vec!["new model".to_string()],
                columns: None,
                all_columns_because: None,
            });
            continue;
        };
        let before = &prev.models[prev_name];
        let (now, then) = (cur.model(name), prev.model(prev_name));

        let mut because = Vec::new();
        if fingerprint(now) != fingerprint(then) {
            if canonical(&now.sql) != canonical(&then.sql) {
                // Compiled SQL can differ with the source file untouched: an incremental
                // model compiles its is_incremental() branch only where its table exists,
                // and a var or the target can change what renders. Still a change — the
                // SQL dbt would run is different — but not one someone made.
                let rendered_only = model.basis == SqlBasis::Compiled && model.source == before.source;
                because.push(if rendered_only { RENDERED_ONLY } else { "sql" }.to_string());
            }
            if column_names(now) != column_names(then) {
                because.push("columns".to_string());
            }
            if sorted(&now.depends_on) != sorted(&then.depends_on) {
                because.push("depends_on".to_string());
            }
            if because.is_empty() {
                because.push("definition".to_string());
            }
        }
        if model.materialized != before.materialized {
            because.push(format!("materialized: {} -> {}", before.materialized, model.materialized));
        }
        if model.relation != before.relation {
            because.push("relation (alias / schema / database)".to_string());
        }
        let hit_macros: BTreeSet<&String> =
            model.macros.iter().chain(&before.macros).filter(|m| macros.contains(*m)).collect();
        if !hit_macros.is_empty() {
            let list: Vec<&str> = hit_macros.into_iter().map(String::as_str).collect();
            because.push(format!("macro {}", list.join(", ")));
        }
        let inputs: BTreeSet<&String> = model.inputs.keys().chain(before.inputs.keys()).collect();
        for input in inputs {
            if model.inputs.get(input) != before.inputs.get(input) {
                because.push(format!("input {input}"));
            }
        }
        if because.is_empty() {
            continue;
        }

        // Narrow to the changed columns only for a pure SQL edit of a model whose
        // lineage is column-level on both sides.
        let (columns, all_columns_because) = if because == ["sql"]
            && model.lineage == Lineage::Columns
            && before.lineage == Lineage::Columns
        {
            match changed_columns(&then.sql, &now.sql, now) {
                Ok(cols) => (Some(cols), None),
                Err(why) => (None, Some(why.to_string())),
            }
        } else {
            (None, None)
        };
        out.changed.push(Changed { model: name.clone(), because, columns, all_columns_because });
    }

    for (name, model) in &prev.models {
        if cur.name_of(&model.unique_id).is_none() {
            out.removed.push(name.clone());
        }
    }
    out
}

/// The form fingerprints compare SQL in: AST-canonical when it parses, else
/// whitespace-normalized — [`AstFingerprinter`]'s own rule.
fn canonical(sql: &str) -> String {
    try_canonical_sql(sql).unwrap_or_else(|| normalize_sql_fallback(sql))
}

fn sorted(names: &[String]) -> Vec<&String> {
    let mut v: Vec<&String> = names.iter().collect();
    v.sort();
    v
}

/// Output column names in order: the column shape a fingerprint hashes.
fn column_names(model: &Model) -> Vec<&str> {
    model.columns.iter().map(|c| c.name.as_str()).collect()
}

/// Macros whose definition differs between the manifests, plus every macro that calls
/// one of them (in either manifest), to a fixpoint.
fn changed_macros(cur: &Manifest, prev: &Manifest) -> BTreeSet<String> {
    let all: BTreeSet<&String> = cur.macros.keys().chain(prev.macros.keys()).collect();
    let mut changed: BTreeSet<String> = all
        .iter()
        .filter(|uid| cur.macros.get(**uid).map(|m| &m.macro_sql) != prev.macros.get(**uid).map(|m| &m.macro_sql))
        .map(|uid| (*uid).clone())
        .collect();
    if changed.is_empty() {
        return changed;
    }
    loop {
        let before = changed.len();
        for uid in &all {
            if changed.contains(*uid) {
                continue;
            }
            let mut calls =
                cur.macros.get(*uid).into_iter().chain(prev.macros.get(*uid)).flat_map(|m| &m.depends_on.macros);
            if calls.any(|c| changed.contains(c)) {
                changed.insert((*uid).clone());
            }
        }
        if changed.len() == before {
            return changed;
        }
    }
}

/// The output columns a SQL edit can reach, or why that cannot be proven.
///
/// Proven only when: both versions parse; the query's skeleton — everything except the
/// expressions of comparable projection items (see [`sqlscan::shape`]) — is identical,
/// so no FROM, JOIN, WHERE, GROUP BY, CTE or subquery-in-a-filter moved; the changed
/// expressions' names, followed by name through every item that mentions them, never
/// reach a row-deciding clause; and the query uses nothing whose row semantics the scan
/// does not model. Following *by name* only over-approximates: an unrelated item that
/// happens to mention a same-named column counts as reached.
pub(crate) fn changed_columns(prev_sql: &str, cur_sql: &str, cur: &Model) -> Result<BTreeSet<String>, &'static str> {
    const UNREADABLE: &str = "one version of its SQL could not be parsed";
    let prev_q = sqlscan::parse_query(prev_sql).ok_or(UNREADABLE)?;
    let cur_q = sqlscan::parse_query(cur_sql).ok_or(UNREADABLE)?;
    // Only the references are read here, so no CTE needs treating as inlined.
    let rows = sqlscan::scan_rows(&cur_q, &BTreeSet::new());
    if rows.opaque.is_some() {
        return Err("the query uses a construct whose row semantics Freshet does not model");
    }
    let (Some(before), Some(after)) = (sqlscan::shape(&prev_q), sqlscan::shape(&cur_q)) else {
        return Err("the query uses a set operation, which Freshet compares as a whole");
    };
    if before.skeleton != after.skeleton || before.items.len() != after.items.len() {
        return Err("the edit changes the query's structure (a FROM, JOIN, WHERE, GROUP BY or CTE), \
                    not just what a column computes");
    }
    let mut reached: BTreeSet<String> = before
        .items
        .iter()
        .zip(&after.items)
        .filter(|(a, b)| a.1 != b.1)
        .map(|(_, b)| b.0.clone())
        .collect();
    if reached.is_empty() {
        return Err("the edit is outside every column expression");
    }
    loop {
        let size = reached.len();
        for (name, mentions) in &after.flows {
            if !reached.contains(name) && mentions.iter().any(|m| reached.contains(m)) {
                reached.insert(name.clone());
            }
        }
        if reached.len() == size {
            break;
        }
    }
    if rows.refs.iter().any(|r| r.path.iter().any(|part| reached.contains(&part.value))) {
        return Err("a changed column feeds a filter, join or grouping, which decides every column's rows");
    }
    let columns: BTreeSet<String> =
        cur.columns.iter().filter(|c| reached.contains(&c.name)).map(|c| c.name.clone()).collect();
    if columns.is_empty() {
        return Err("the edit reaches no output column by name");
    }
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use freshet_core::{Column, Materialization};

    fn model(cols: &[&str]) -> Model {
        Model {
            name: "m".into(),
            sql: String::new(),
            materialization: Materialization::Table,
            columns: cols.iter().map(|c| Column::new(*c, Vec::new())).collect(),
            depends_on: Vec::new(),
        }
    }

    const STG: &str = "with source as (select * from raw), \
                       renamed as (select id as order_id, user_id as customer_id, \
                                   amount_cents / 100 as amount, status from source) \
                       select * from renamed";

    #[test]
    fn a_single_expression_edit_changes_only_its_column() {
        let edited = STG.replace("/ 100 as", "/ 100.0 as");
        let cols = changed_columns(STG, &edited, &model(&["order_id", "customer_id", "amount", "status"])).unwrap();
        assert_eq!(cols, BTreeSet::from(["amount".to_string()]));
    }

    #[test]
    fn the_change_follows_a_column_through_later_ctes() {
        let prev = "with a as (select id, x * 2 as y from t), b as (select id, y + 1 as z from a) select id, z from b";
        let cur = "with a as (select id, x * 3 as y from t), b as (select id, y + 1 as z from a) select id, z from b";
        assert_eq!(changed_columns(prev, cur, &model(&["id", "z"])).unwrap(), BTreeSet::from(["z".to_string()]));
    }

    #[test]
    fn a_filter_edit_changes_every_column() {
        let edited = STG.replace("from source)", "from source where status <> 'test')");
        assert!(changed_columns(STG, &edited, &model(&["order_id", "customer_id", "amount", "status"])).is_err());
    }

    #[test]
    fn a_changed_column_that_a_later_filter_reads_changes_every_column() {
        let prev = "with a as (select id, x as flag from t) select id from a where flag";
        let cur = "with a as (select id, not x as flag from t) select id from a where flag";
        assert!(changed_columns(prev, cur, &model(&["id"])).is_err());
    }

    #[test]
    fn gaining_a_row_generating_function_changes_every_column() {
        let prev = "select id, tags from t";
        let cur = "select id, explode(tags) as tags from t";
        assert!(changed_columns(prev, cur, &model(&["id", "tags"])).is_err());
        // Editing the arguments of one that was already there is still a row change.
        let prev = "select id, unnest(tags) as tag from t";
        let cur = "select id, unnest(tags || extra) as tag from t";
        assert!(changed_columns(prev, cur, &model(&["id", "tag"])).is_err());
    }

    #[test]
    fn an_aggregate_without_group_by_changes_every_column() {
        // One row instead of one per row of t: `one` changes too.
        let prev = "select v, 1 as one from t";
        let cur = "select max(v) as v, 1 as one from t";
        assert!(changed_columns(prev, cur, &model(&["v", "one"])).is_err());
        // Under a GROUP BY, swapping one aggregate for another keeps the rows.
        let prev = "select k, sum(v) as s, count(*) as n from t group by k";
        let cur = "select k, max(v) as s, count(*) as n from t group by k";
        assert_eq!(
            changed_columns(prev, cur, &model(&["k", "s", "n"])).unwrap(),
            BTreeSet::from(["s".to_string()])
        );
        // A windowed aggregate keeps the rows.
        let prev = "select id, v from t";
        let cur = "select id, sum(v) over (partition by id) as v from t";
        assert_eq!(
            changed_columns(prev, cur, &model(&["id", "v"])).unwrap(),
            BTreeSet::from(["v".to_string()])
        );
    }

    #[test]
    fn a_changed_join_key_changes_every_column() {
        let prev = "with a as (select id, k from t) select a.id, u.v from a join u on a.k = u.k";
        let cur = "with a as (select id, k + 1 as k from t) select a.id, u.v from a join u on a.k = u.k";
        assert!(changed_columns(prev, cur, &model(&["id", "v"])).is_err());
    }
}
