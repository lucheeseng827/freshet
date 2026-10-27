//! freshet-sql — SQL column-lineage analyzer for the backfill planner.
//!
//! Turns raw SQL into the structure `freshet-core` consumes: each model's
//! `depends_on` (model-level edges) and each output column's `upstream`
//! (column-level edges). This is the SQL-comprehension layer the planner needs to
//! prune backfills at column granularity (overlaps the project's lineage idea #9).
//!
//! ## Safety contract (read this before trusting the output)
//!
//! Backfill correctness is asymmetric: **over-attributing** an upstream column is
//! safe (you rebuild a bit too much), **under-attributing** is a silent data bug
//! (you skip a model that needed rebuilding). So:
//!
//! * `depends_on` — every model the query reads, anywhere in it — is **reliable**
//!   and is what guarantees correctness. A CTE hides a model only where SQL itself
//!   would resolve the name to the CTE: a CTE's own body, and the CTEs before it,
//!   still read the real model (`with orders as (select … from orders)` depends on
//!   `orders`).
//! * A column's `upstream` is everything its value **and its presence** can depend
//!   on — not only the columns its expression names, but every column that decides
//!   *which rows exist*: join keys, `WHERE`, `GROUP BY`, `HAVING`, `QUALIFY`,
//!   `DISTINCT`, and the `ORDER BY` of a `LIMIT`. A filter on `b.flag` changes the
//!   rows of a column that never mentions `b`, so that column lists `b.flag`.
//! * A column also covers the **row set** of every model it reads from: a column
//!   that names none of a joined model's columns still gets one of them (its first),
//!   because a model whose rows changed has all of its columns changed, so any one
//!   of them is enough to carry that change downstream.
//! * Anything uncertain — a subquery, an identifier that doesn't resolve, an
//!   unqualified name while an unknown-schema source is in scope, a table function —
//!   yields an **empty** `upstream`. The planner treats empty `upstream` as "this
//!   column may depend on anything this model reads". Uncertainty is never merged
//!   away: a union branch, a CTE column or a row filter that is uncertain makes
//!   every column it feeds uncertain too.
//!
//! ## Scope
//!
//! Handled: SELECT with FROM + JOINs (ON / USING), qualified/unqualified column
//! refs, expressions over columns, `expr AS alias`, `*` and `table.*` (with
//! `EXCLUDE` / `EXCEPT` / `RENAME` / `REPLACE`), CTEs (`WITH`, flattened to real
//! upstreams, with column aliases), derived tables, set operations (merged
//! positionally), aggregation, `DISTINCT [ON]`, windows (inline and named), and
//! `ORDER BY … LIMIT`. Anything else that produces rows — table functions,
//! `UNNEST`, `PIVOT`, `LATERAL VIEW`, recursive CTEs — is analyzed conservatively.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use anyhow::{anyhow, bail, Context, Result};
use freshet_core::{Column, ColumnRef, Materialization, Model, Project};
use sqlparser::ast::{
    Distinct, ExcludeSelectItem, Expr, GroupByExpr, Ident, JoinConstraint, JoinOperator,
    ObjectName, OrderByExpr, Query, RenameSelectItem, Select, SelectItem, SetExpr, SetOperator,
    SetQuantifier, Statement, TableAlias, TableFactor, TableWithJoins, TopQuantity, Value, Visit,
    Visitor, WildcardAdditionalOptions,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Maps a table/model name to its output column names. Seeded with source
/// (external) tables; grown with each model's derived columns as a project builds.
pub type Catalog = BTreeMap<String, Vec<String>>;

/// The lineage derived for one model.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ModelLineage {
    /// Model-level dependencies (referenced tables that are known models).
    pub depends_on: Vec<String>,
    /// Output columns with their (certain-only) upstream column refs.
    pub columns: Vec<Column>,
}

/// Parse `sql` and derive lineage for a single model.
///
/// * `known_models` — names that count as model dependencies (everything else
///   referenced, e.g. seed sources, is treated as external and excluded from
///   `depends_on`).
/// * `catalog` — known output columns per source/model, for wildcard expansion and
///   unqualified-reference resolution.
pub fn analyze(
    sql: &str,
    known_models: &BTreeSet<String>,
    catalog: &Catalog,
) -> Result<ModelLineage> {
    let query = parse_single_query(sql)?;
    // One model: scan on a miss rather than index every name first — callers that
    // analyze model after model (freshet-dbt, the daemon's reparse) would otherwise
    // pay for the whole project on every call.
    lineage_of(&query, known_models, &Fold::Scan, catalog, &Fold::Scan)
}

/// [`analyze`] over a parsed query, with the case-folded name indexes supplied: built
/// once per project by [`build_project`], rather than once per model.
fn lineage_of(
    query: &Query,
    known_models: &BTreeSet<String>,
    models_folded: &Fold,
    catalog: &Catalog,
    catalog_folded: &Fold,
) -> Result<ModelLineage> {
    let cx = Cx {
        catalog,
        catalog_folded,
        models: known_models,
        models_folded,
    };
    let rel = analyze_query(query, &cx, &Locals::new())?;
    let depends_on = referenced_models_in(query, known_models, models_folded);
    Ok(ModelLineage {
        depends_on,
        columns: rel.cols.into_iter().map(LCol::into_column).collect(),
    })
}

/// Build a fully-linked [`Project`] from `(name, sql)` pairs and a seed catalog of
/// source-table schemas. Resolves in dependency order so each model's output
/// columns are known before its downstreams are analyzed.
pub fn build_project(models: &[(String, String)], sources: &Catalog) -> Result<Project> {
    let mut known: BTreeSet<String> = BTreeSet::new();
    for (name, _) in models {
        if !known.insert(name.clone()) {
            bail!("duplicate model name `{name}`");
        }
    }

    let models_folded = Fold::Index(Folded::of(&known));

    // Pass 1: model-level deps only (no catalog needed) → topo order.
    let mut deps: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut parsed: BTreeMap<String, (Query, &String)> = BTreeMap::new();
    for (name, sql) in models {
        let q = parse_single_query(sql).with_context(|| format!("parsing model `{name}`"))?;
        deps.insert(
            name.clone(),
            referenced_models_in(&q, &known, &models_folded),
        );
        parsed.insert(name.clone(), (q, sql));
    }
    let order = topo_order(&known, &deps)?;

    // Pass 2: analyze in order, growing the catalog with each model's columns.
    let mut catalog: Catalog = sources.clone();
    let mut catalog_folded = Fold::Index(Folded::of(catalog.keys()));
    let mut built: Vec<Model> = Vec::with_capacity(models.len());
    for name in &order {
        let (query, sql) = &parsed[name];
        let lineage = lineage_of(query, &known, &models_folded, &catalog, &catalog_folded)
            .with_context(|| format!("analyzing model `{name}`"))?;
        catalog.insert(
            name.clone(),
            lineage.columns.iter().map(|c| c.name.clone()).collect(),
        );
        catalog_folded.insert(name);
        built.push(Model {
            name: name.clone(),
            sql: (*sql).clone(),
            materialization: Materialization::Table,
            columns: lineage.columns,
            depends_on: lineage.depends_on,
        });
    }
    Ok(Project::from_models(built))
}

// ---- parsing helpers -------------------------------------------------------

fn parse_single_query(sql: &str) -> Result<Query> {
    let mut stmts =
        Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| anyhow!("SQL parse error: {e}"))?;
    if stmts.len() != 1 {
        bail!("expected exactly one statement, got {}", stmts.len());
    }
    match stmts.pop().unwrap() {
        Statement::Query(q) => Ok(*q),
        other => bail!("expected a SELECT query, got: {other}"),
    }
}

/// Referenced tables (anywhere in the query) that are known models. Reliable basis
/// for `depends_on`.
///
/// A **top-level** CTE hides a same-named model exactly where SQL resolves the name
/// to the CTE: in the main query, and — unless `WITH RECURSIVE` — only in the CTEs
/// *after* it. Its own body and the CTEs before it read the real model, which is the
/// dbt "import CTE" (`with orders as (select … from orders)`). Getting that wrong is
/// the one error class this planner must never make: the model would carry no edge
/// to `orders`, so a change to `orders` would never rebuild it.
///
/// A qualified name (`analytics.orders`) never resolves to a CTE. We deliberately do
/// **not** exclude *nested* CTE names: a nested CTE named like a real model would
/// otherwise hide that model's dependency in an outer scope. The cost is at-worst
/// over-attribution when a nested CTE shadows a model name (a harmless extra
/// rebuild).
#[cfg(test)]
fn referenced_models(query: &Query, known_models: &BTreeSet<String>) -> Vec<String> {
    referenced_models_in(query, known_models, &Fold::Scan)
}

fn referenced_models_in(
    query: &Query,
    known_models: &BTreeSet<String>,
    folded: &Fold,
) -> Vec<String> {
    let mut found: BTreeSet<String> = BTreeSet::new();
    let mut visit = |node: &dyn VisitRelations, shadow: &[&Ident]| {
        node.each_relation(&mut |name: &ObjectName| {
            if let [single] = name.0.as_slice() {
                if shadow.iter().any(|cte| same_ident(cte, single)) {
                    return;
                }
            }
            if let Some(last) = name.0.last() {
                found.extend(folded.resolve(known_models, last));
            }
        });
    };
    match &query.with {
        None => visit(query, &[]),
        Some(with) => {
            let ctes: Vec<&Ident> = with.cte_tables.iter().map(|c| &c.alias.name).collect();
            for (i, cte) in with.cte_tables.iter().enumerate() {
                let visible = if with.recursive {
                    &ctes[..]
                } else {
                    &ctes[..i]
                };
                visit(&*cte.query, visible);
            }
            let mut rest = query.clone();
            rest.with = None;
            visit(&rest, &ctes[..]);
        }
    }
    found.into_iter().collect()
}

/// Object-safe access to `visit_relations` for the few node types walked above.
trait VisitRelations {
    fn each_relation(&self, f: &mut dyn FnMut(&ObjectName));
}

impl VisitRelations for Query {
    fn each_relation(&self, f: &mut dyn FnMut(&ObjectName)) {
        let _ = sqlparser::ast::visit_relations(self, |name| {
            f(name);
            ControlFlow::<()>::Continue(())
        });
    }
}

/// Two identifiers that SQL resolves to the same name: equal, or equal ignoring case
/// when neither is quoted (unquoted identifiers are case-insensitive).
fn same_ident(a: &Ident, b: &Ident) -> bool {
    a.value == b.value
        || (a.quote_style.is_none()
            && b.quote_style.is_none()
            && a.value.eq_ignore_ascii_case(&b.value))
}

/// A set of names a relation can resolve to: the models, or the catalog's tables.
trait NameSet {
    fn has(&self, name: &str) -> bool;
    fn each(&self) -> Box<dyn Iterator<Item = &String> + '_>;
}

impl NameSet for BTreeSet<String> {
    fn has(&self, name: &str) -> bool {
        self.contains(name)
    }
    fn each(&self) -> Box<dyn Iterator<Item = &String> + '_> {
        Box::new(self.iter())
    }
}

impl<V> NameSet for BTreeMap<String, V> {
    fn has(&self, name: &str) -> bool {
        self.contains_key(name)
    }
    fn each(&self) -> Box<dyn Iterator<Item = &String> + '_> {
        Box::new(self.keys())
    }
}

/// How an unquoted name that misses exactly is matched case-insensitively.
enum Fold {
    /// Through an index built once for a whole project ([`build_project`]).
    Index(Folded),
    /// By scanning the set: no set-up, so right for analyzing one model at a time.
    Scan,
}

impl Fold {
    /// Keep an index current as its set grows. A scan reads the set itself.
    fn insert(&mut self, name: &str) {
        if let Fold::Index(index) = self {
            index.insert(name);
        }
    }

    /// `ident` resolved against `set`: exact first; an unquoted name also matches
    /// case-insensitively. An ambiguous fold returns every match.
    fn resolve(&self, set: &impl NameSet, ident: &Ident) -> Vec<String> {
        if set.has(&ident.value) {
            return vec![ident.value.clone()];
        }
        if ident.quote_style.is_some() {
            return Vec::new();
        }
        match self {
            Fold::Index(index) => index.get(&ident.value),
            Fold::Scan => set
                .each()
                .filter(|n| n.eq_ignore_ascii_case(&ident.value))
                .cloned()
                .collect(),
        }
    }
}

/// A case-folded index of names, for resolving unquoted identifiers, since
/// `FROM Orders` reads the model `orders` on every warehouse that folds identifiers.
/// Built once per project, not per model.
#[derive(Default)]
struct Folded(BTreeMap<String, Vec<String>>);

impl Folded {
    fn of<'a>(names: impl IntoIterator<Item = &'a String>) -> Self {
        let mut folded = Folded::default();
        for n in names {
            folded.insert(n);
        }
        folded
    }

    fn insert(&mut self, name: &str) {
        let slot = self.0.entry(name.to_ascii_lowercase()).or_default();
        if !slot.iter().any(|n| n == name) {
            slot.push(name.to_string());
        }
    }

    fn get(&self, name: &str) -> Vec<String> {
        self.0
            .get(&name.to_ascii_lowercase())
            .cloned()
            .unwrap_or_default()
    }
}

// ---- column lineage --------------------------------------------------------

/// What a column's value — or a relation's set of rows — was proven to depend on.
/// `None` is "not proven": the planner must assume it depends on everything the
/// model reads. Merging never shrinks it: anything merged with `None` is `None`.
type Refs = Option<BTreeSet<ColumnRef>>;

fn merge(into: &mut Refs, from: &Refs) {
    match (into.as_mut(), from) {
        (Some(a), Some(b)) => a.extend(b.iter().cloned()),
        _ => *into = None,
    }
}

fn certain() -> Refs {
    Some(BTreeSet::new())
}

/// A column as analysis sees it.
#[derive(Clone, Debug)]
struct LCol {
    name: String,
    refs: Refs,
}

impl LCol {
    fn into_column(self) -> Column {
        Column::new(
            self.name,
            self.refs
                .map(|r| r.into_iter().collect())
                .unwrap_or_default(),
        )
    }
}

/// A relation analysis produced: a CTE, a derived table, a whole query.
///
/// `rows` is what decides which rows it has. Every column's `refs` covers it — each
/// column holds some ref to every model its rows come from, and every row-deciding
/// ref — so a column read from this relation carries its row set along.
#[derive(Clone, Debug)]
struct Rel {
    cols: Vec<LCol>,
    rows: Refs,
}

impl Rel {
    /// Keep the column names; prove nothing.
    fn uncertain(mut self) -> Self {
        for c in &mut self.cols {
            c.refs = None;
        }
        self.rows = None;
        self
    }

    /// Add refs that decide the rows of the whole relation, so to every column too.
    fn add_row_refs(&mut self, refs: &Refs) {
        merge(&mut self.rows, refs);
        for c in &mut self.cols {
            merge(&mut c.refs, refs);
        }
    }
}

/// The CTEs in scope, innermost last: a nested `WITH` shadows an outer CTE of the
/// same name. Keyed by the declared identifier, quotes included, so a lookup resolves
/// a name exactly as SQL does.
type Locals = Vec<(Ident, Rel)>;

fn local<'l>(locals: &'l Locals, name: &Ident) -> Option<&'l Rel> {
    locals
        .iter()
        .rev()
        .find(|(cte, _)| same_ident(cte, name))
        .map(|(_, rel)| rel)
}

/// What every analysis step needs to look up.
struct Cx<'a> {
    catalog: &'a Catalog,
    catalog_folded: &'a Fold,
    models: &'a BTreeSet<String>,
    models_folded: &'a Fold,
}

/// A source visible in a FROM clause, under some alias.
struct Source {
    alias: String,
    kind: SourceKind,
}

enum SourceKind {
    /// A real table/model: refs attribute to `name`. `known` lists its columns when
    /// the catalog has them (enables wildcard + unqualified resolution).
    Real {
        name: String,
        known: Option<Vec<String>>,
    },
    /// A CTE or derived table: its columns already carry resolved real upstreams.
    Local(Rel),
    /// Rows from something whose columns SQL alone does not tell us: a table
    /// function, `UNNEST`, `PIVOT`, a recursive reference. Nothing through it is
    /// proven.
    Opaque,
}

/// Which scope sources a set of refs reached, by index.
type Touched = BTreeSet<usize>;

/// A resolved expression: its refs and the sources they came through.
struct Resolved {
    refs: Refs,
    touched: Touched,
}

impl Resolved {
    fn certain() -> Self {
        Self {
            refs: certain(),
            touched: Touched::new(),
        }
    }

    fn uncertain() -> Self {
        Self {
            refs: None,
            touched: Touched::new(),
        }
    }

    fn add(&mut self, other: &Resolved) {
        merge(&mut self.refs, &other.refs);
        self.touched.extend(other.touched.iter().copied());
    }
}

/// One projection item before row refs are folded in.
struct ProjCol {
    name: String,
    value: Resolved,
}

/// Analyze a query body (handling WITH, set operations and LIMIT) into a relation.
fn analyze_query(query: &Query, cx: &Cx, outer: &Locals) -> Result<Rel> {
    // Build local scopes from this query's CTEs. Each sees the earlier ones only —
    // a CTE's own name inside its body is the real table of that name.
    let mut locals: Locals = outer.clone();
    if let Some(with) = &query.with {
        let names: Vec<&Ident> = with.cte_tables.iter().map(|c| &c.alias.name).collect();
        for (i, cte) in with.cte_tables.iter().enumerate() {
            let rel = if with.recursive && reads_any(&cte.query, &names[i..]) {
                recursive_cte(&cte.query, &cte.alias, cx, &locals)
            } else {
                rename_columns(analyze_query(&cte.query, cx, &locals)?, &cte.alias)?
            };
            locals.push((cte.alias.name.clone(), rel));
        }
    }

    // Which rows survive a LIMIT depends on the ORDER BY; without a LIMIT the order
    // changes nothing a table keeps. (DISTINCT ON reads the ORDER BY either way.)
    let limited = query.limit.is_some()
        || query.offset.is_some()
        || query.fetch.is_some()
        || matches!(&*query.body, SetExpr::Select(s) if s.top.is_some());
    let order: &[OrderByExpr] = query
        .order_by
        .as_ref()
        .map(|o| o.exprs.as_slice())
        .unwrap_or(&[]);

    let mut rel = analyze_setexpr(&query.body, cx, &locals, order, limited)?;
    // A row count computed from data (`LIMIT (select …)`, `FETCH FIRST (select …)
    // ROWS ONLY`, `TOP (select …)`) or an OFFSET, or ClickHouse's per-group
    // `LIMIT BY`, decides rows in a way the terms above do not describe.
    let top = match &*query.body {
        SetExpr::Select(s) => match s.top.as_ref().and_then(|t| t.quantity.as_ref()) {
            Some(TopQuantity::Expr(e)) => Some(e),
            _ => None,
        },
        _ => None,
    };
    let computed_limit = [
        query.limit.as_ref(),
        query.offset.as_ref().map(|o| &o.value),
        query.fetch.as_ref().and_then(|f| f.quantity.as_ref()),
        top,
    ]
    .into_iter()
    .flatten()
    .any(|e| {
        let c = collect(e);
        c.uncertain || !c.idents.is_empty()
    });
    if !query.limit_by.is_empty() || computed_limit {
        rel = rel.uncertain();
    }
    Ok(rel)
}

/// A `WITH RECURSIVE` CTE that reads itself (or a later CTE). Its rows are a
/// fixpoint this analyzer does not compute, so nothing through it is proven; its
/// column names come from its alias list, else from the anchor branch.
fn recursive_cte(query: &Query, alias: &TableAlias, cx: &Cx, locals: &Locals) -> Rel {
    let cols = if alias.columns.is_empty() {
        let mut anchor: &SetExpr = &query.body;
        while let SetExpr::SetOperation { left, .. } = anchor {
            anchor = left;
        }
        match anchor {
            SetExpr::Select(select) => analyze_select(select, cx, locals, &[], false)
                .map(|r| r.cols)
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    } else {
        alias
            .columns
            .iter()
            .map(|c| LCol {
                name: c.value.clone(),
                refs: None,
            })
            .collect()
    };
    Rel { cols, rows: None }.uncertain()
}

/// Whether `query` reads any of `names` as an unqualified relation.
fn reads_any(query: &Query, names: &[&Ident]) -> bool {
    let mut hit = false;
    query.each_relation(&mut |name: &ObjectName| {
        if let [single] = name.0.as_slice() {
            hit |= names.iter().any(|n| same_ident(n, single));
        }
    });
    hit
}

/// Apply an alias's column list (`cte (a, b) as …`, `(…) as t (a, b)`): renames the
/// first N columns, as SQL does.
fn rename_columns(mut rel: Rel, alias: &TableAlias) -> Result<Rel> {
    if alias.columns.is_empty() {
        return Ok(rel);
    }
    if alias.columns.len() > rel.cols.len() {
        bail!(
            "`{}` has {} column(s) but its alias names {}",
            alias.name.value,
            rel.cols.len(),
            alias.columns.len()
        );
    }
    for (col, name) in rel.cols.iter_mut().zip(&alias.columns) {
        col.name = name.value.clone();
    }
    Ok(rel)
}

fn analyze_setexpr(
    body: &SetExpr,
    cx: &Cx,
    locals: &Locals,
    order: &[OrderByExpr],
    limited: bool,
) -> Result<Rel> {
    match body {
        SetExpr::Select(select) => analyze_select(select, cx, locals, order, limited),
        SetExpr::Query(q) => {
            let mut rel = analyze_query(q, cx, locals)?;
            if limited {
                let refs = order_by_outputs(order, &rel.cols);
                rel.add_row_refs(&refs);
            }
            Ok(rel)
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let l = analyze_setexpr(left, cx, locals, &[], false)?;
            let r = analyze_setexpr(right, cx, locals, &[], false)?;
            let mut rel = set_operation(*op, *set_quantifier, l, r);
            if limited {
                let refs = order_by_outputs(order, &rel.cols);
                rel.add_row_refs(&refs);
            }
            Ok(rel)
        }
        // VALUES / TABLE / INSERT etc. — no column provenance we can prove. A model
        // with no columns is opaque to the planner, which is the safe reading.
        _ => Ok(Rel {
            cols: Vec::new(),
            rows: None,
        }),
    }
}

/// Merge set-operation branches positionally: output names from the left, refs from
/// both sides. Anything but `UNION ALL` compares whole rows (to deduplicate,
/// intersect or subtract), so there every column decides the rows of every other.
fn set_operation(op: SetOperator, quantifier: SetQuantifier, l: Rel, r: Rel) -> Rel {
    let by_name = matches!(
        quantifier,
        SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName
    );
    if l.cols.len() != r.cols.len() || by_name {
        return l.uncertain();
    }
    let mut rows = l.rows;
    merge(&mut rows, &r.rows);
    let mut cols: Vec<LCol> = l
        .cols
        .into_iter()
        .zip(r.cols)
        .map(|(mut lc, rc)| {
            merge(&mut lc.refs, &rc.refs);
            lc
        })
        .collect();
    let union_all = op == SetOperator::Union && quantifier == SetQuantifier::All;
    if !union_all {
        let mut every = rows.clone();
        for c in &cols {
            merge(&mut every, &c.refs);
        }
        for c in &mut cols {
            merge(&mut c.refs, &every);
        }
        rows = every;
    }
    Rel { cols, rows }
}

/// ORDER BY terms of a LIMIT over a relation's *output*: by name or by position.
/// Anything else cannot be resolved here, so proves nothing.
fn order_by_outputs(order: &[OrderByExpr], cols: &[LCol]) -> Refs {
    let mut refs = certain();
    for o in order {
        let col = match &o.expr {
            Expr::Identifier(id) => cols.iter().find(|c| c.name == id.value),
            Expr::Value(Value::Number(n, _)) => n
                .parse::<usize>()
                .ok()
                .and_then(|i| i.checked_sub(1))
                .and_then(|i| cols.get(i)),
            _ => None,
        };
        // An ORDER BY item that isn't a known output column: uncertain (`None`).
        merge(&mut refs, &col?.refs);
    }
    refs
}

fn analyze_select(
    select: &Select,
    cx: &Cx,
    locals: &Locals,
    order: &[OrderByExpr],
    limited: bool,
) -> Result<Rel> {
    let mut join_exprs: Vec<RowTerm> = Vec::new();
    let scope = build_scope(select, cx, locals, &mut join_exprs)?;

    // 1. What each projected column's *value* reads.
    let mut proj: Vec<ProjCol> = Vec::new();
    // Items that also make rows: `explode(tags)` repeats every other column once per
    // element, so what they read decides the rows of the whole SELECT.
    let mut generators: Vec<usize> = Vec::new();
    for (idx, item) in select.projection.iter().enumerate() {
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
            if generates_rows(expr) {
                generators.push(proj.len());
            }
        }
        match item {
            SelectItem::UnnamedExpr(expr) => proj.push(ProjCol {
                name: expr_output_name(expr).unwrap_or_else(|| format!("_col{idx}")),
                value: resolve_expr(expr, &scope, None),
            }),
            SelectItem::ExprWithAlias { expr, alias } => proj.push(ProjCol {
                name: alias.value.clone(),
                value: resolve_expr(expr, &scope, None),
            }),
            SelectItem::Wildcard(opts) => {
                let mut cols = Vec::new();
                for (i, src) in scope.iter().enumerate() {
                    cols.extend(expand_source(i, src)?);
                }
                proj.extend(apply_wildcard_options(cols, opts, &scope)?);
            }
            SelectItem::QualifiedWildcard(name, opts) => {
                let q = last_ident(name);
                let (i, src) = scope
                    .iter()
                    .enumerate()
                    .find(|(_, s)| s.alias == q)
                    .ok_or_else(|| anyhow!("`{q}.*` refers to unknown source `{q}`"))?;
                let cols = expand_source(i, src)?;
                proj.extend(apply_wildcard_options(cols, opts, &scope)?);
            }
        }
    }

    // 2. What decides which rows exist.
    let mut rows = Resolved::certain();
    for &i in &generators {
        rows.add(&proj[i].value);
    }
    for term in &join_exprs {
        let r = match term {
            RowTerm::Expr(e) => resolve_expr(e, &scope, None),
            RowTerm::Using(idents) => {
                let mut r = Resolved::certain();
                for id in idents.iter() {
                    r.add(&resolve_path(std::slice::from_ref(&id.value), &scope, None));
                }
                r
            }
            RowTerm::Unknown => Resolved::uncertain(),
        };
        rows.add(&r);
    }
    for e in [&select.selection, &select.prewhere].into_iter().flatten() {
        rows.add(&resolve_expr(e, &scope, Some(&proj[..])));
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, _) => {
            for e in exprs {
                rows.add(&resolve_grouping(e, &scope, &proj));
            }
        }
        // GROUP BY ALL groups by every non-aggregate output: over-approximate with all.
        GroupByExpr::All(_) => {
            for p in &proj {
                rows.add(&p.value);
            }
        }
    }
    for e in [&select.having, &select.qualify].into_iter().flatten() {
        rows.add(&resolve_expr(e, &scope, Some(&proj[..])));
    }
    match &select.distinct {
        None => {}
        // Deduplication compares every output column.
        Some(Distinct::Distinct) => {
            for p in &proj {
                rows.add(&p.value);
            }
        }
        // One row per key — which one is the ORDER BY's first.
        Some(Distinct::On(exprs)) => {
            for e in exprs {
                rows.add(&resolve_grouping(e, &scope, &proj));
            }
            for o in order {
                rows.add(&resolve_grouping(&o.expr, &scope, &proj));
            }
        }
    }
    if limited {
        for o in order {
            rows.add(&resolve_grouping(&o.expr, &scope, &proj));
        }
    }
    if select.connect_by.is_some()
        || !select.lateral_views.is_empty()
        || select.value_table_mode.is_some()
    {
        rows = Resolved::uncertain();
    }

    // Named windows are referenced by name from inside projected expressions, so the
    // columns they partition and order by never reach those expressions' refs.
    let mut windows = Resolved::certain();
    for w in &select.named_window {
        let c = collect(w);
        windows.add(&resolve_collected(c, &scope, None));
    }

    // 3. Every column: its value, the row terms, and the row set of each source it
    //    does not already reach.
    let finish = |value: &Resolved| -> Refs {
        let mut refs = value.refs.clone();
        merge(&mut refs, &rows.refs);
        merge(&mut refs, &windows.refs);
        let touched: Touched = value
            .touched
            .union(&rows.touched)
            .chain(windows.touched.iter())
            .copied()
            .collect();
        for (i, src) in scope.iter().enumerate() {
            if !touched.contains(&i) {
                merge(&mut refs, &row_set(src, cx));
            }
        }
        refs
    };
    let rel_rows = finish(&Resolved {
        refs: rows.refs.clone(),
        touched: rows.touched.clone(),
    });
    let cols = proj
        .iter()
        .map(|p| LCol {
            name: p.name.clone(),
            refs: finish(&p.value),
        })
        .collect();
    Ok(Rel {
        cols,
        rows: rel_rows,
    })
}

/// Refs that carry a source's row-set changes. A model whose rows changed has every
/// output column changed, so one of its columns is enough. External sources have
/// no tracked changes; anything whose columns we do not know proves nothing.
fn row_set(src: &Source, cx: &Cx) -> Refs {
    match &src.kind {
        SourceKind::Real { name, known } => {
            if !cx.models.contains(name) {
                return certain();
            }
            match known.as_deref() {
                Some([first, ..]) => Some(BTreeSet::from([ColumnRef::new(
                    name.clone(),
                    first.clone(),
                )])),
                _ => None,
            }
        }
        SourceKind::Local(rel) => rel.rows.clone(),
        SourceKind::Opaque => None,
    }
}

/// Expand `source.*` into one output column per source column, each carrying that
/// column's precise upstream. Errors if a source's columns are unknown (we refuse to
/// silently drop columns).
fn expand_source(idx: usize, src: &Source) -> Result<Vec<ProjCol>> {
    let touched = Touched::from([idx]);
    match &src.kind {
        SourceKind::Real { name, known } => {
            let cols = known.as_ref().ok_or_else(|| {
                anyhow!(
                    "cannot expand `*` over `{}`: its schema is unknown (declare it as a source)",
                    name
                )
            })?;
            Ok(cols
                .iter()
                .map(|c| ProjCol {
                    name: c.clone(),
                    value: Resolved {
                        refs: Some(BTreeSet::from([ColumnRef::new(name.clone(), c.clone())])),
                        touched: touched.clone(),
                    },
                })
                .collect())
        }
        SourceKind::Local(rel) => Ok(rel
            .cols
            .iter()
            .map(|c| ProjCol {
                name: c.name.clone(),
                value: Resolved {
                    refs: c.refs.clone(),
                    touched: touched.clone(),
                },
            })
            .collect()),
        SourceKind::Opaque => bail!(
            "cannot expand `*` over `{}`: its columns cannot be derived from the SQL \
             (a table function, UNNEST or similar) — list them explicitly",
            src.alias
        ),
    }
}

/// `* EXCLUDE (…)`, `* EXCEPT (…)`, `* RENAME (…)`, `* REPLACE (…)`.
fn apply_wildcard_options(
    mut cols: Vec<ProjCol>,
    opts: &WildcardAdditionalOptions,
    scope: &[Source],
) -> Result<Vec<ProjCol>> {
    if opts.opt_ilike.is_some() {
        bail!("`* ILIKE …` is not supported: list the columns explicitly");
    }
    let mut dropped: Vec<&Ident> = Vec::new();
    match &opts.opt_exclude {
        Some(ExcludeSelectItem::Single(id)) => dropped.push(id),
        Some(ExcludeSelectItem::Multiple(ids)) => dropped.extend(ids),
        None => {}
    }
    if let Some(except) = &opts.opt_except {
        dropped.push(&except.first_element);
        dropped.extend(&except.additional_elements);
    }
    cols.retain(|c| !dropped.iter().any(|d| d.value == c.name));
    if let Some(replace) = &opts.opt_replace {
        for el in &replace.items {
            let value = resolve_expr(&el.expr, scope, None);
            match cols.iter_mut().find(|c| c.name == el.column_name.value) {
                Some(c) => c.value = value,
                None => bail!(
                    "`REPLACE` names `{}`, which `*` does not produce",
                    el.column_name.value
                ),
            }
        }
    }
    let renames: Vec<(&Ident, &Ident)> = match &opts.opt_rename {
        Some(RenameSelectItem::Single(r)) => vec![(&r.ident, &r.alias)],
        Some(RenameSelectItem::Multiple(rs)) => rs.iter().map(|r| (&r.ident, &r.alias)).collect(),
        None => Vec::new(),
    };
    for (from, to) in renames {
        if let Some(c) = cols.iter_mut().find(|c| c.name == from.value) {
            c.name = to.value.clone();
        }
    }
    Ok(cols)
}

/// A join's contribution to which rows exist.
enum RowTerm<'q> {
    Expr(&'q Expr),
    Using(&'q [Ident]),
    /// `NATURAL` joins on columns we would have to infer.
    Unknown,
}

fn build_scope<'q>(
    select: &'q Select,
    cx: &Cx,
    locals: &Locals,
    rows: &mut Vec<RowTerm<'q>>,
) -> Result<Vec<Source>> {
    let mut scope = Vec::new();
    for twj in &select.from {
        add_table_with_joins(twj, cx, locals, &mut scope, rows)?;
    }
    Ok(scope)
}

fn add_table_with_joins<'q>(
    twj: &'q TableWithJoins,
    cx: &Cx,
    locals: &Locals,
    scope: &mut Vec<Source>,
    rows: &mut Vec<RowTerm<'q>>,
) -> Result<()> {
    add_table_factor(&twj.relation, cx, locals, scope, rows)?;
    for join in &twj.joins {
        add_table_factor(&join.relation, cx, locals, scope, rows)?;
        let constraint = match &join.join_operator {
            JoinOperator::Inner(c)
            | JoinOperator::LeftOuter(c)
            | JoinOperator::RightOuter(c)
            | JoinOperator::FullOuter(c)
            | JoinOperator::LeftSemi(c)
            | JoinOperator::RightSemi(c)
            | JoinOperator::LeftAnti(c)
            | JoinOperator::RightAnti(c) => Some(c),
            JoinOperator::AsOf {
                match_condition,
                constraint,
            } => {
                rows.push(RowTerm::Expr(match_condition));
                Some(constraint)
            }
            JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => None,
        };
        match constraint {
            Some(JoinConstraint::On(e)) => rows.push(RowTerm::Expr(e)),
            Some(JoinConstraint::Using(ids)) => rows.push(RowTerm::Using(ids)),
            Some(JoinConstraint::Natural) => rows.push(RowTerm::Unknown),
            Some(JoinConstraint::None) | None => {}
        }
    }
    Ok(())
}

fn add_table_factor<'q>(
    tf: &'q TableFactor,
    cx: &Cx,
    locals: &Locals,
    scope: &mut Vec<Source>,
    rows: &mut Vec<RowTerm<'q>>,
) -> Result<()> {
    match tf {
        TableFactor::Table {
            name, alias, args, ..
        } => {
            let Some(last) = name.0.last() else {
                bail!("empty table name");
            };
            let alias_name = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| last.value.clone());
            // A table-valued function call (`FROM f(…)`) is not the table `f`.
            if args.is_some() {
                scope.push(Source {
                    alias: alias_name,
                    kind: SourceKind::Opaque,
                });
                return Ok(());
            }
            // Only an unqualified name can be a CTE.
            let local = match name.0.as_slice() {
                [single] => local(locals, single).cloned(),
                _ => None,
            };
            let kind = match local {
                // Reference to a CTE: import its already-resolved columns.
                Some(rel) => SourceKind::Local(match alias {
                    Some(a) => rename_columns(rel, a)?,
                    None => rel,
                }),
                None => {
                    let models = cx.models_folded.resolve(cx.models, last);
                    let tables = cx.catalog_folded.resolve(cx.catalog, last);
                    // Two models that differ only in case, both matching an unquoted
                    // name: whichever it reads, lineage cannot say.
                    let ambiguous = models.len() > 1 || (models.is_empty() && tables.len() > 1);
                    let name = models
                        .first()
                        .or(tables.first())
                        .cloned()
                        .unwrap_or_else(|| last.value.clone());
                    let known = cx.catalog.get(&name).cloned();
                    match (alias, known) {
                        _ if ambiguous => SourceKind::Opaque,
                        // `FROM t AS x (a, b)` renames t's columns: a local view of it.
                        (Some(a), Some(cols)) if !a.columns.is_empty() => {
                            let src = Source {
                                alias: String::new(),
                                kind: SourceKind::Real {
                                    name: name.clone(),
                                    known: Some(cols.clone()),
                                },
                            };
                            let set = row_set(&src, cx);
                            let rel = Rel {
                                cols: cols
                                    .iter()
                                    .map(|c| LCol {
                                        name: c.clone(),
                                        refs: Some(BTreeSet::from([ColumnRef::new(
                                            name.clone(),
                                            c.clone(),
                                        )])),
                                    })
                                    .collect(),
                                rows: set,
                            };
                            SourceKind::Local(rename_columns(rel, a)?)
                        }
                        (Some(a), None) if !a.columns.is_empty() => SourceKind::Opaque,
                        (_, known) => SourceKind::Real { name, known },
                    }
                }
            };
            scope.push(Source {
                alias: alias_name,
                kind,
            });
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            // A LATERAL subquery reads earlier FROM items, which its own scope cannot
            // resolve — so its refs come back unproven, which is the safe answer.
            let rel = analyze_query(subquery, cx, locals)?;
            let (alias_name, rel) = match alias {
                Some(a) => (a.name.value.clone(), rename_columns(rel, a)?),
                None => (String::new(), rel),
            };
            scope.push(Source {
                alias: alias_name,
                kind: SourceKind::Local(rel),
            });
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            let start = scope.len();
            add_table_with_joins(table_with_joins, cx, locals, scope, rows)?;
            // `(a JOIN b) AS t` hides a and b behind t, whose columns we do not
            // assemble: nothing through it is proven.
            if alias.is_some() {
                for src in &mut scope[start..] {
                    src.kind = SourceKind::Opaque;
                }
            }
        }
        // Table functions, UNNEST, PIVOT, MATCH_RECOGNIZE, JSON_TABLE, …: rows we
        // cannot describe. Model references inside them still reach `depends_on`.
        other => scope.push(Source {
            alias: table_factor_alias(other).unwrap_or_default(),
            kind: SourceKind::Opaque,
        }),
    }
    Ok(())
}

fn table_factor_alias(tf: &TableFactor) -> Option<String> {
    let alias = match tf {
        TableFactor::TableFunction { alias, .. }
        | TableFactor::Function { alias, .. }
        | TableFactor::UNNEST { alias, .. }
        | TableFactor::JsonTable { alias, .. }
        | TableFactor::Pivot { alias, .. }
        | TableFactor::Unpivot { alias, .. }
        | TableFactor::MatchRecognize { alias, .. } => alias,
        _ => &None,
    };
    alias.as_ref().map(|a| a.name.value.clone())
}

/// A GROUP BY / ORDER BY / DISTINCT ON term: a position (`GROUP BY 1`) names an
/// output column; anything else resolves like any expression, falling back to output
/// aliases.
fn resolve_grouping(expr: &Expr, scope: &[Source], proj: &[ProjCol]) -> Resolved {
    if let Expr::Value(Value::Number(n, _)) = expr {
        return match n
            .parse::<usize>()
            .ok()
            .and_then(|i| i.checked_sub(1))
            .and_then(|i| proj.get(i))
        {
            Some(p) => Resolved {
                refs: p.value.refs.clone(),
                touched: p.value.touched.clone(),
            },
            None => Resolved::uncertain(),
        };
    }
    resolve_expr(expr, scope, Some(proj))
}

/// Resolve all column refs an expression depends on, honoring the safety contract:
/// returns the precise set, or `None` refs if anything is uncertain. `outputs` lets
/// an unqualified name that no source has fall back to a projected alias (clauses
/// that may name them: GROUP BY, HAVING, QUALIFY, ORDER BY).
fn resolve_expr(expr: &Expr, scope: &[Source], outputs: Option<&[ProjCol]>) -> Resolved {
    resolve_collected(collect(expr), scope, outputs)
}

fn resolve_collected(c: RefCollector, scope: &[Source], outputs: Option<&[ProjCol]>) -> Resolved {
    if c.uncertain {
        return Resolved::uncertain();
    }
    let mut out = Resolved::certain();
    for path in &c.idents {
        out.add(&resolve_path(path, scope, outputs));
        if out.refs.is_none() {
            return Resolved::uncertain();
        }
    }
    out
}

fn collect<V: Visit>(node: &V) -> RefCollector {
    let mut collector = RefCollector::default();
    let _ = node.visit(&mut collector);
    collector
}

/// Resolve one identifier path (`[col]` or `[.., table, col]`) against the scope.
fn resolve_path(path: &[String], scope: &[Source], outputs: Option<&[ProjCol]>) -> Resolved {
    match resolve_ref(path, scope) {
        Lookup::Found(r) => r,
        Lookup::Uncertain => Resolved::uncertain(),
        Lookup::NotFound => match (outputs, path) {
            (Some(outs), [name]) => match outs.iter().find(|p| &p.name == name) {
                Some(p) => Resolved {
                    refs: p.value.refs.clone(),
                    touched: p.value.touched.clone(),
                },
                None => Resolved::uncertain(),
            },
            _ => Resolved::uncertain(),
        },
    }
}

enum Lookup {
    Found(Resolved),
    /// No source has it — the caller may try an output alias.
    NotFound,
    /// It may come from somewhere we cannot see into.
    Uncertain,
}

fn resolve_ref(path: &[String], scope: &[Source]) -> Lookup {
    let Some(col) = path.last() else {
        return Lookup::Uncertain;
    };
    if path.len() >= 2 {
        let tbl = &path[path.len() - 2];
        let Some((i, src)) = scope.iter().enumerate().find(|(_, s)| &s.alias == tbl) else {
            // An outer query's alias, a struct field, a schema-qualified column: not
            // provable here.
            return Lookup::Uncertain;
        };
        return match &src.kind {
            SourceKind::Real {
                name,
                known: Some(cols),
            } if cols.contains(col) => Lookup::Found(Resolved {
                refs: Some(BTreeSet::from([ColumnRef::new(name.clone(), col.clone())])),
                touched: Touched::from([i]),
            }),
            SourceKind::Local(rel) => match rel.cols.iter().find(|c| &c.name == col) {
                Some(LCol { refs: Some(r), .. }) => Lookup::Found(Resolved {
                    refs: Some(r.clone()),
                    touched: Touched::from([i]),
                }),
                _ => Lookup::Uncertain,
            },
            _ => Lookup::Uncertain,
        };
    }

    // Unqualified: if any in-scope source's columns are unknown, we cannot be sure
    // the column doesn't come from it → conservative.
    if scope.iter().any(|s| {
        matches!(
            &s.kind,
            SourceKind::Real { known: None, .. } | SourceKind::Opaque
        )
    }) {
        return Lookup::Uncertain;
    }
    let mut found = Resolved::certain();
    let mut any = false;
    for (i, src) in scope.iter().enumerate() {
        match &src.kind {
            SourceKind::Real {
                name,
                known: Some(cols),
            } if cols.contains(col) => {
                any = true;
                found.add(&Resolved {
                    refs: Some(BTreeSet::from([ColumnRef::new(name.clone(), col.clone())])),
                    touched: Touched::from([i]),
                });
            }
            SourceKind::Local(rel) => {
                if let Some(c) = rel.cols.iter().find(|c| &c.name == col) {
                    any = true;
                    found.add(&Resolved {
                        refs: c.refs.clone(),
                        touched: Touched::from([i]),
                    });
                }
            }
            _ => {}
        }
    }
    if !any {
        Lookup::NotFound
    } else if found.refs.is_none() {
        Lookup::Uncertain
    } else {
        Lookup::Found(found)
    }
}

/// Set-returning functions: called in a projection, each makes zero or more rows per
/// input row. Names across the warehouses Freshet targets — Postgres/DuckDB/Redshift
/// (`unnest`, `generate_series`, the JSON expanders), Databricks/Spark and Hive
/// (`explode`, `inline`, `stack`), ClickHouse (`arrayJoin`). A user-defined table
/// function called in a projection is not recognized; its rows go unmodelled.
const SET_RETURNING: &[&str] = &[
    "unnest",
    "generate_series",
    "generate_subscripts",
    "regexp_matches",
    "regexp_split_to_table",
    "string_to_table",
    "json_array_elements",
    "json_array_elements_text",
    "jsonb_array_elements",
    "jsonb_array_elements_text",
    "json_each",
    "json_each_text",
    "jsonb_each",
    "jsonb_each_text",
    "json_object_keys",
    "jsonb_object_keys",
    "json_populate_recordset",
    "jsonb_populate_recordset",
    "json_to_recordset",
    "jsonb_to_recordset",
    "jsonb_path_query",
    "explode",
    "explode_outer",
    "posexplode",
    "posexplode_outer",
    "inline",
    "inline_outer",
    "stack",
    "arrayjoin",
];

/// Whether `name` (a function's last identifier, any case) is a set-returning
/// function: called in a projection, it makes zero or more rows per input row.
pub fn is_set_returning_function(name: &str) -> bool {
    SET_RETURNING.contains(&name.to_ascii_lowercase().as_str())
}

/// Aggregate functions. An aggregate in a SELECT without GROUP BY collapses the query
/// to one row, so gaining or losing one is a change to the rows, not to a value.
/// User-defined aggregates are not recognized.
const AGGREGATES: &[&str] = &[
    "count",
    "count_if",
    "sum",
    "avg",
    "min",
    "max",
    "any_value",
    "arbitrary",
    "first",
    "last",
    "array_agg",
    "string_agg",
    "listagg",
    "group_concat",
    "json_agg",
    "jsonb_agg",
    "json_object_agg",
    "jsonb_object_agg",
    "object_agg",
    "collect_list",
    "collect_set",
    "bool_and",
    "bool_or",
    "every",
    "bit_and",
    "bit_or",
    "bit_xor",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "variance",
    "var_pop",
    "var_samp",
    "median",
    "mode",
    "percentile_cont",
    "percentile_disc",
    "approx_count_distinct",
    "approx_distinct",
    "approx_percentile",
    "corr",
    "covar_pop",
    "covar_samp",
    "regr_slope",
    "regr_intercept",
    "hll_count",
    "countif",
    "logical_and",
    "logical_or",
];

/// Whether `name` (a function's last identifier, any case) is a known aggregate.
pub fn is_aggregate_function(name: &str) -> bool {
    AGGREGATES.contains(&name.to_ascii_lowercase().as_str())
}

/// Whether `expr` calls a set-returning function anywhere in it.
fn generates_rows(expr: &Expr) -> bool {
    struct Finder(bool);
    impl Visitor for Finder {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if let Expr::Function(f) = expr {
                if let Some(name) = f.name.0.last() {
                    if is_set_returning_function(&name.value) {
                        self.0 = true;
                        return ControlFlow::Break(());
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut finder = Finder(false);
    let _ = expr.visit(&mut finder);
    finder.0
}

/// Output name for an unaliased projection item, when it's a plain column ref.
fn expr_output_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::CompoundIdentifier(ids) => ids.last().map(|i| i.value.clone()),
        _ => None,
    }
}

fn last_ident(name: &ObjectName) -> String {
    name.0.last().map(|i| i.value.clone()).unwrap_or_default()
}

/// Visitor collecting identifier paths in an expression and flagging any subquery
/// (which would reference a different scope → forces the conservative fallback).
#[derive(Default)]
struct RefCollector {
    idents: Vec<Vec<String>>,
    uncertain: bool,
}

impl Visitor for RefCollector {
    type Break = ();
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(id) => self.idents.push(vec![id.value.clone()]),
            Expr::CompoundIdentifier(ids) => self
                .idents
                .push(ids.iter().map(|i| i.value.clone()).collect()),
            Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => {
                self.uncertain = true
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    // A subquery anywhere — including in a FROM or a function argument — is another
    // scope. Its relations still reach `depends_on`; its columns prove nothing here.
    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.uncertain = true;
        ControlFlow::Continue(())
    }
}

// ---- topo sort (on the raw dep map, before a Project exists) ----------------

fn topo_order(
    known: &BTreeSet<String>,
    deps: &BTreeMap<String, Vec<String>>,
) -> Result<Vec<String>> {
    let mut indegree: BTreeMap<&str, usize> = known.iter().map(|k| (k.as_str(), 0)).collect();
    let mut downstream: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (m, ups) in deps {
        for up in ups {
            if known.contains(up) {
                *indegree.get_mut(m.as_str()).unwrap() += 1;
                downstream.entry(up.as_str()).or_default().push(m.as_str());
            }
        }
    }
    let mut queue: Vec<&str> = indegree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(k, _)| *k)
        .collect();
    queue.sort();
    let mut order = Vec::new();
    let mut i = 0;
    while i < queue.len() {
        let n = queue[i];
        i += 1;
        order.push(n.to_string());
        if let Some(ds) = downstream.get(n) {
            for d in ds {
                let e = indegree.get_mut(d).unwrap();
                *e -= 1;
                if *e == 0 {
                    queue.push(d);
                }
            }
        }
    }
    if order.len() == known.len() {
        Ok(order)
    } else {
        bail!(
            "dependency cycle among models: {:?}",
            indegree
                .into_iter()
                .filter(|(_, d)| *d > 0)
                .map(|(k, _)| k)
                .collect::<Vec<_>>()
        )
    }
}

#[cfg(test)]
mod tests;
