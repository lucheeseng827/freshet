//! Row-deciding references and projection shape, read from the same AST lineage uses.
//!
//! Column lineage answers "which upstream columns flow into this column's value". The
//! planner uses it as its only test of "is this downstream impacted", so a reference
//! that decides *which rows* come out — `WHERE`, `JOIN … ON`, `GROUP BY`, `HAVING`,
//! `QUALIFY`, `DISTINCT` — must count too: `select id from orders where status = 'paid'`
//! has to rebuild when `status` changes, although no output column is computed from it.
//! The planner core's freshet-sql, as this crate was built against, left those clauses out of
//! lineage, which is an under-rebuild — the one error class the planner must never make.
//!
//! [`scan_rows`] finds those references so project building can fold them into every
//! output column's upstream (a filter decides the rows of every column). It does not
//! depend on freshet-sql reporting them; where freshet-sql also does, the result is a
//! union, and a reference only ever widens a rebuild. [`shape`] describes a query's
//! projection so the column diff can tell an edit to one expression from an edit to the
//! query's structure.
//!
//! Both are conservative the same way the rest of the crate is: a construct whose row
//! semantics they do not model (a cross join, `NATURAL JOIN`, a table function, a
//! positional column rename) is *reported*, and the caller falls back to model-level
//! lineage rather than trusting a partial answer.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    Distinct, Expr, GroupByExpr, Ident, JoinConstraint, JoinOperator, NamedWindowDefinition,
    NamedWindowExpr, Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier, Statement,
    TableAlias, TableFactor, TopQuantity, Value, Visit, VisitMut, Visitor, VisitorMut,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Parse `sql` as exactly one query — the same acceptance rule freshet-sql applies, so
/// anything freshet-sql analyzed parses here too.
pub(crate) fn parse_query(sql: &str) -> Option<Query> {
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    match stmts.pop()? {
        Statement::Query(q) => Some(*q),
        _ => None,
    }
}

/// Every identifier path in `expr`, including those inside its subqueries.
fn ident_paths(expr: &Expr) -> Vec<Vec<Ident>> {
    #[derive(Default)]
    struct Paths(Vec<Vec<Ident>>);
    impl Visitor for Paths {
        type Break = ();
        fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
            match e {
                Expr::Identifier(id) => self.0.push(vec![id.clone()]),
                Expr::CompoundIdentifier(ids) => self.0.push(ids.clone()),
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }
    let mut paths = Paths::default();
    let _ = expr.visit(&mut paths);
    paths.0
}

/// Every name any identifier in `expr` mentions, qualifiers included.
fn ident_components(expr: &Expr) -> BTreeSet<String> {
    ident_paths(expr).into_iter().flatten().map(|i| i.value).collect()
}

/// `GROUP BY 2` / `ORDER BY 2`: a 1-based position in the projection.
fn ordinal(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Value(Value::Number(n, _)) => n.parse::<usize>().ok().filter(|k| *k >= 1),
        _ => None,
    }
}

/// The output name freshet-sql gives a projection item (`_col{idx}` when there is none).
fn item_name(item: &SelectItem, idx: usize) -> Option<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
        SelectItem::UnnamedExpr(Expr::Identifier(id)) => Some(id.value.clone()),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(ids)) => ids.last().map(|i| i.value.clone()),
        SelectItem::UnnamedExpr(_) => Some(format!("_col{idx}")),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => None,
    }
}

/// Whether an item only passes an input column through under its own name — the case
/// in which a later reference to that name still means the input column.
fn is_pass_through(item: &SelectItem) -> bool {
    let last = |e: &Expr| match e {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::CompoundIdentifier(ids) => ids.last().map(|i| i.value.clone()),
        _ => None,
    };
    match item {
        SelectItem::UnnamedExpr(e) => last(e).is_some(),
        SelectItem::ExprWithAlias { expr, alias } => last(expr).as_deref() == Some(alias.value.as_str()),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => true,
    }
}

fn alias_target<'a>(select: &'a Select, name: &str) -> Option<&'a Expr> {
    select.projection.iter().find_map(|item| match item {
        SelectItem::ExprWithAlias { expr, alias } if alias.value == name => Some(expr),
        _ => None,
    })
}

/// Words that parse as bare identifiers but name a date part or a niladic function,
/// not a column: `dateadd(day, -3, current_date)`. Only consulted for an *unquoted*
/// identifier that matched no known column — a real column named `day` still resolves.
pub(crate) fn is_keyword_value(name: &str) -> bool {
    const WORDS: &[&str] = &[
        "year", "years", "yy", "yyyy", "quarter", "quarters", "qtr", "month", "months", "mm",
        "week", "weeks", "wk", "isoweek", "weekofyear", "weekday", "dayofweek", "dow",
        "dayofyear", "doy", "day", "days", "dd", "hour", "hours", "hh", "minute", "minutes",
        "mi", "second", "seconds", "ss", "millisecond", "milliseconds", "ms", "microsecond",
        "microseconds", "us", "nanosecond", "nanoseconds", "ns", "epoch", "isoyear",
        "current_date", "current_time", "current_timestamp", "localtime", "localtimestamp",
        "current_user", "session_user", "current_role", "sysdate", "systimestamp",
    ];
    WORDS.iter().any(|w| w.eq_ignore_ascii_case(name))
}

// ------------------------------------------------------------------ row scan --

/// A column reference in a row-deciding position.
#[derive(Clone, Debug)]
pub(crate) struct RowRef {
    pub path: Vec<Ident>,
    /// `false` for a name that is also an output alias of the same SELECT (`GROUP BY
    /// total` where `total` is an alias). The alias's own expression is recorded as
    /// required references; the name itself need only resolve if it also names an
    /// input column.
    pub required: bool,
}

/// What [`scan_rows`] found.
#[derive(Clone, Debug, Default)]
pub(crate) struct RowScan {
    pub refs: Vec<RowRef>,
    /// Output names that a CTE, derived table or set operation *computes* rather than
    /// passes through. A row-level reference to one of these cannot be traced to an
    /// upstream column by name.
    pub derived: BTreeSet<String>,
    /// The first construct whose effect on which rows come out is not modelled here.
    pub opaque: Option<String>,
}

/// Find every reference that decides which rows `query` returns.
///
/// `inlined` names CTEs that are another model's SQL, inlined by dbt — an ephemeral
/// model compiles into its consumers as `__dbt__cte__<alias>`. Their output names are
/// that model's own columns, which lineage already knows, so they are not derived names.
pub(crate) fn scan_rows(query: &Query, inlined: &BTreeSet<String>) -> RowScan {
    let mut scanner = RowScanner { out: RowScan::default(), seen_root: false, inlined, output_bodies: Vec::new() };
    let _ = query.visit(&mut scanner);
    scanner.out
}

struct RowScanner<'a> {
    out: RowScan,
    seen_root: bool,
    inlined: &'a BTreeSet<String>,
    /// Bodies of `inlined` CTEs seen so far, by address: the AST is not moved while it
    /// is visited, and a CTE's body is visited after the query that declares it.
    output_bodies: Vec<*const Query>,
}

impl Visitor for RowScanner<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        // The first query visited is the statement itself; its output columns are the
        // model's, and those names are not "derived" in the sense above.
        let is_root = !std::mem::replace(&mut self.seen_root, true);
        let outputs_are_known = is_root || self.output_bodies.iter().any(|b| std::ptr::eq(*b, query));
        if let Some(with) = &query.with {
            for cte in with.cte_tables.iter().filter(|c| self.inlined.contains(&c.alias.name.value)) {
                self.output_bodies.push(&*cte.query);
            }
        }
        self.query(query, outputs_are_known);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, tf: &TableFactor) -> ControlFlow<()> {
        self.table_factor(tf);
        ControlFlow::Continue(())
    }
}

impl RowScanner<'_> {
    fn opaque(&mut self, what: &str) {
        if self.out.opaque.is_none() {
            self.out.opaque = Some(what.to_string());
        }
    }

    fn push(&mut self, path: Vec<Ident>, required: bool) {
        self.out.refs.push(RowRef { path, required });
    }

    /// A row-deciding expression. A bare name that is one of `select`'s output aliases
    /// also stands for what the alias computes.
    fn row_expr(&mut self, expr: &Expr, select: Option<&Select>) {
        for path in ident_paths(expr) {
            if let ([only], Some(select)) = (path.as_slice(), select) {
                if let Some(target) = alias_target(select, &only.value) {
                    for inner in ident_paths(target) {
                        self.push(inner, true);
                    }
                    self.push(path, false);
                    continue;
                }
            }
            self.push(path, true);
        }
    }

    /// `GROUP BY` / `ORDER BY` accept a projection position as well as an expression.
    fn row_expr_or_ordinal(&mut self, expr: &Expr, select: Option<&Select>) {
        let Some(k) = ordinal(expr) else {
            return self.row_expr(expr, select);
        };
        match select.and_then(|s| s.projection.get(k - 1)) {
            Some(SelectItem::UnnamedExpr(e)) | Some(SelectItem::ExprWithAlias { expr: e, .. }) => {
                for path in ident_paths(e) {
                    self.push(path, true);
                }
            }
            _ => self.opaque("a positional GROUP BY / ORDER BY over `*` or a set operation"),
        }
    }

    /// Every expression of a projection, as row-deciding (`DISTINCT`, `GROUP BY ALL`,
    /// and the whole-row comparison of `UNION` / `INTERSECT` / `EXCEPT`).
    fn whole_projection(&mut self, select: &Select, what: &str) {
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    for path in ident_paths(e) {
                        self.push(path, true);
                    }
                }
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => self.opaque(what),
            }
        }
    }

    fn query(&mut self, query: &Query, is_root: bool) {
        if let Some(with) = &query.with {
            if with.recursive {
                self.opaque("a recursive CTE");
            }
            if with.cte_tables.iter().any(|cte| !cte.alias.columns.is_empty()) {
                self.opaque("a CTE that renames its columns by position");
            }
        }
        let body_select = match query.body.as_ref() {
            SetExpr::Select(s) => Some(s.as_ref()),
            _ => None,
        };
        if let Some(order_by) = &query.order_by {
            for e in &order_by.exprs {
                self.row_expr_or_ordinal(&e.expr, body_select);
            }
        }
        for e in query.limit.iter().chain(&query.limit_by) {
            self.row_expr(e, body_select);
        }
        if let Some(offset) = &query.offset {
            self.row_expr(&offset.value, None);
        }
        if let Some(quantity) = query.fetch.as_ref().and_then(|f| f.quantity.as_ref()) {
            self.row_expr(quantity, None);
        }
        self.body(&query.body, is_root);
    }

    fn body(&mut self, body: &SetExpr, is_root: bool) {
        match body {
            SetExpr::Select(select) => self.select(select, is_root),
            // A parenthesized query is visited on its own.
            SetExpr::Query(_) => {}
            SetExpr::SetOperation { op, set_quantifier, left, right } => {
                let keeps_every_row = matches!(op, SetOperator::Union)
                    && matches!(set_quantifier, SetQuantifier::All | SetQuantifier::AllByName);
                let mut branches = Vec::new();
                collect_branch_selects(body, &mut branches);
                if !keeps_every_row {
                    for select in &branches {
                        self.whole_projection(select, "a set operation that compares whole rows over `*`");
                    }
                }
                if !is_root {
                    // Branches merge by position under the left branch's names, so no
                    // output name of a set operation means one input column.
                    for select in &branches {
                        for (idx, item) in select.projection.iter().enumerate() {
                            match item_name(item, idx) {
                                Some(name) => {
                                    self.out.derived.insert(name);
                                }
                                None => self.opaque("a set operation over `*` inside a CTE or subquery"),
                            }
                        }
                    }
                }
                self.body(left, is_root);
                self.body(right, is_root);
            }
            SetExpr::Values(values) => {
                for row in &values.rows {
                    for e in row {
                        self.row_expr(e, None);
                    }
                }
            }
            SetExpr::Insert(_) | SetExpr::Update(_) => self.opaque("a data-modifying statement"),
            SetExpr::Table(_) => self.opaque("a TABLE statement"),
        }
    }

    fn select(&mut self, select: &Select, is_root: bool) {
        if select.from.len() > 1 {
            self.opaque("a comma join");
        }
        for twj in &select.from {
            for join in &twj.joins {
                self.join(&join.join_operator);
            }
        }
        if !select.lateral_views.is_empty() {
            self.opaque("LATERAL VIEW");
        }
        if select.connect_by.is_some() {
            self.opaque("CONNECT BY");
        }
        if select.value_table_mode.is_some() {
            self.opaque("SELECT AS STRUCT / VALUE");
        }
        for e in [&select.selection, &select.prewhere, &select.having, &select.qualify]
            .into_iter()
            .flatten()
        {
            self.row_expr(e, Some(select));
        }
        match &select.group_by {
            GroupByExpr::All(_) => self.whole_projection(select, "GROUP BY ALL over `*`"),
            GroupByExpr::Expressions(exprs, _) => {
                for e in exprs {
                    self.row_expr_or_ordinal(e, Some(select));
                }
            }
        }
        match &select.distinct {
            Some(Distinct::Distinct) => self.whole_projection(select, "SELECT DISTINCT *"),
            Some(Distinct::On(exprs)) => {
                for e in exprs {
                    self.row_expr(e, Some(select));
                }
            }
            None => {}
        }
        if let Some(TopQuantity::Expr(e)) = select.top.as_ref().and_then(|t| t.quantity.as_ref()) {
            self.row_expr(e, None);
        }
        for e in select.cluster_by.iter().chain(&select.distribute_by).chain(&select.sort_by) {
            self.row_expr(e, Some(select));
        }
        // A named window feeds window functions in the projection, whose lineage
        // freshet-sql reads from the call site only — `OVER w` names no columns there.
        for NamedWindowDefinition(_, window) in &select.named_window {
            if let NamedWindowExpr::WindowSpec(spec) = window {
                for e in spec.partition_by.iter().chain(spec.order_by.iter().map(|o| &o.expr)) {
                    self.row_expr(e, None);
                }
            }
        }
        for (idx, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::Wildcard(opts) | SelectItem::QualifiedWildcard(_, opts) => {
                    if opts.opt_replace.is_some() || opts.opt_rename.is_some() {
                        self.opaque("a wildcard with REPLACE or RENAME");
                    }
                }
                _ if !is_root && !is_pass_through(item) => {
                    if let Some(name) = item_name(item, idx) {
                        self.out.derived.insert(name);
                    }
                }
                _ => {}
            }
        }
    }

    fn join(&mut self, op: &JoinOperator) {
        match op {
            JoinOperator::Inner(c)
            | JoinOperator::LeftOuter(c)
            | JoinOperator::RightOuter(c)
            | JoinOperator::FullOuter(c)
            | JoinOperator::LeftSemi(c)
            | JoinOperator::RightSemi(c)
            | JoinOperator::LeftAnti(c)
            | JoinOperator::RightAnti(c) => self.constraint(c),
            JoinOperator::AsOf { match_condition, constraint } => {
                self.row_expr(match_condition, None);
                self.constraint(constraint);
            }
            JoinOperator::CrossJoin => self.opaque("a CROSS JOIN"),
            JoinOperator::CrossApply | JoinOperator::OuterApply => self.opaque("CROSS / OUTER APPLY"),
        }
    }

    fn constraint(&mut self, c: &JoinConstraint) {
        match c {
            JoinConstraint::On(e) => self.row_expr(e, None),
            JoinConstraint::Using(ids) => {
                for id in ids {
                    self.push(vec![id.clone()], true);
                }
            }
            JoinConstraint::Natural => self.opaque("a NATURAL JOIN"),
            JoinConstraint::None => self.opaque("a join without ON or USING"),
        }
    }

    fn table_factor(&mut self, tf: &TableFactor) {
        match tf {
            TableFactor::Table { args: Some(_), .. } => self.opaque("a table-valued function"),
            TableFactor::Table { alias, .. } => self.alias(alias),
            TableFactor::Derived { lateral, alias, .. } => {
                if *lateral {
                    self.opaque("a LATERAL subquery");
                }
                self.alias(alias);
            }
            TableFactor::NestedJoin { table_with_joins, alias } => {
                for join in &table_with_joins.joins {
                    self.join(&join.join_operator);
                }
                self.alias(alias);
            }
            TableFactor::TableFunction { .. } | TableFactor::Function { .. } => {
                self.opaque("a table function")
            }
            TableFactor::UNNEST { .. } => self.opaque("UNNEST"),
            TableFactor::JsonTable { .. } => self.opaque("JSON_TABLE"),
            TableFactor::Pivot { .. } => self.opaque("PIVOT"),
            TableFactor::Unpivot { .. } => self.opaque("UNPIVOT"),
            TableFactor::MatchRecognize { .. } => self.opaque("MATCH_RECOGNIZE"),
        }
    }

    fn alias(&mut self, alias: &Option<TableAlias>) {
        if alias.as_ref().is_some_and(|a| !a.columns.is_empty()) {
            self.opaque("a table alias that renames columns by position");
        }
    }
}

/// The SELECTs whose projections make up a set operation's output, in order.
fn collect_branch_selects<'a>(body: &'a SetExpr, out: &mut Vec<&'a Select>) {
    match body {
        SetExpr::Select(s) => out.push(s),
        SetExpr::Query(q) => collect_branch_selects(&q.body, out),
        SetExpr::SetOperation { left, right, .. } => {
            collect_branch_selects(left, out);
            collect_branch_selects(right, out);
        }
        _ => {}
    }
}

// --------------------------------------------------------------------- shape --

/// Whether `expr` calls, outside any window (`OVER`), a function `is` accepts by name.
fn calls(expr: &Expr, is: fn(&str) -> bool) -> bool {
    struct Finder {
        is: fn(&str) -> bool,
        hit: bool,
    }
    impl Visitor for Finder {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            if let Expr::Function(f) = expr {
                if f.over.is_none() && f.name.0.last().is_some_and(|n| (self.is)(&n.value)) {
                    self.hit = true;
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut finder = Finder { is, hit: false };
    let _ = expr.visit(&mut finder);
    finder.hit
}

/// A query's projection, laid out for comparing two versions of it.
#[derive(Clone, Debug)]
pub(crate) struct Shape {
    /// The query with every *comparable* projection item's expression replaced by
    /// `NULL` (its output name kept). Two versions with equal skeletons differ only
    /// inside those expressions.
    pub skeleton: String,
    /// The comparable items, in a walk order fixed by the skeleton: output name and
    /// expression text.
    pub items: Vec<(String, String)>,
    /// Every value-producing item (comparable or not): its output name and every name
    /// its expression mentions. What a change to one item can flow into.
    pub flows: Vec<(String, BTreeSet<String>)>,
}

/// Lay out `query`'s projection, or `None` for a query the column diff does not reason
/// about (set operations, `VALUES`).
///
/// An item is *comparable* when a change to its expression changes that one column's
/// values and nothing else: it sits in a value-producing SELECT (the query body, a CTE,
/// a derived table — not a subquery inside an expression, whose output decides rows),
/// the SELECT does not deduplicate or group by position, and the item's name is not
/// used by that SELECT's own filters or groupings. Every other item stays in the
/// skeleton, so an edit to it reads as a structural change.
pub(crate) fn shape(query: &Query) -> Option<Shape> {
    let mut masked = query.clone();
    let mut masker = Masker { depth: 0, items: Vec::new(), flows: Vec::new(), unsupported: false };
    let _ = VisitMut::visit(&mut masked, &mut masker);
    if masker.unsupported {
        return None;
    }
    Some(Shape { skeleton: masked.to_string(), items: masker.items, flows: masker.flows })
}

struct Masker {
    /// How many expressions enclose the current node: a query at depth 0 produces
    /// values (body, CTE, derived table); any deeper one sits inside an expression.
    depth: usize,
    items: Vec<(String, String)>,
    flows: Vec<(String, BTreeSet<String>)>,
    unsupported: bool,
}

impl VisitorMut for Masker {
    type Break = ();

    fn pre_visit_expr(&mut self, _: &mut Expr) -> ControlFlow<()> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, _: &mut Expr) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        if self.depth == 0 {
            self.value_query(query);
        }
        ControlFlow::Continue(())
    }
}

impl Masker {
    fn value_query(&mut self, query: &mut Query) {
        // A query-level ORDER BY / LIMIT can name the body's output columns.
        let mut outer_names = BTreeSet::new();
        let mut positional = false;
        if let Some(order_by) = &query.order_by {
            for e in &order_by.exprs {
                positional |= ordinal(&e.expr).is_some();
                outer_names.extend(ident_components(&e.expr));
            }
        }
        match query.body.as_mut() {
            SetExpr::Select(select) => self.select(select, &outer_names, positional),
            // A parenthesized query is visited on its own.
            SetExpr::Query(_) => {}
            _ => self.unsupported = true,
        }
    }

    fn select(&mut self, select: &mut Select, outer_names: &BTreeSet<String>, positional: bool) {
        let mut row_names = outer_names.clone();
        let mut row_exprs: Vec<&Expr> = [&select.selection, &select.prewhere, &select.having, &select.qualify]
            .into_iter()
            .flatten()
            .collect();
        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
            row_exprs.extend(exprs);
        }
        if let Some(Distinct::On(exprs)) = &select.distinct {
            row_exprs.extend(exprs);
        }
        row_exprs.extend(select.cluster_by.iter().chain(&select.distribute_by).chain(&select.sort_by));
        for e in &row_exprs {
            row_names.extend(ident_components(e));
        }
        let frozen = positional
            || select.distinct.is_some()
            || match &select.group_by {
                GroupByExpr::All(_) => true,
                GroupByExpr::Expressions(exprs, _) => exprs.iter().any(|e| ordinal(e).is_some()),
            };
        let grouped = !matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if exprs.is_empty());
        for (idx, item) in select.projection.iter_mut().enumerate() {
            let Some(name) = item_name(item, idx) else { continue };
            let expr = match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
                _ => continue,
            };
            self.flows.push((name.clone(), ident_components(expr)));
            // An item that makes rows (`explode(tags)`), or an aggregate in a SELECT with
            // no GROUP BY (it collapses the query to one row), stays in the skeleton:
            // gaining or losing one changes the rows, which no column diff may hide.
            let decides_rows = calls(expr, freshet_sql::is_set_returning_function)
                || (!grouped && calls(expr, freshet_sql::is_aggregate_function));
            if frozen || decides_rows || row_names.contains(&name) {
                continue;
            }
            self.items.push((name.clone(), expr.to_string()));
            *item = SelectItem::ExprWithAlias { expr: Expr::Value(Value::Null), alias: Ident::new(name) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(sql: &str) -> RowScan {
        scan_rows(&parse_query(sql).expect("parses"), &BTreeSet::new())
    }

    fn names(scan: &RowScan) -> Vec<String> {
        let mut out: Vec<String> =
            scan.refs.iter().filter(|r| r.required).map(|r| r.path.last().unwrap().value.clone()).collect();
        out.sort();
        out.dedup();
        out
    }

    #[test]
    fn filters_joins_and_groupings_are_row_level_and_projections_are_not() {
        let s = scan(
            "select o.id, sum(o.amount) as total from orders o \
             join customers c on o.customer_id = c.id \
             where c.active group by o.id having count(*) > 1",
        );
        assert_eq!(names(&s), vec!["active", "customer_id", "id"]);
        assert!(s.opaque.is_none());
        assert!(s.derived.is_empty(), "the root query's outputs are not derived names");
    }

    #[test]
    fn a_subquery_in_a_filter_contributes_everything_it_mentions() {
        let s = scan("select id from orders where id in (select order_id from refunds where approved)");
        assert_eq!(names(&s), vec!["approved", "id", "order_id"]);
    }

    #[test]
    fn a_grouping_by_position_or_alias_reads_what_the_item_computes() {
        let s = scan("select date(created_at) as day_of, count(*) from events group by 1");
        assert_eq!(names(&s), vec!["created_at"]);
        let s = scan("select date(created_at) as day_of, count(*) from events group by day_of");
        assert_eq!(names(&s), vec!["created_at"]);
        assert!(s.refs.iter().any(|r| !r.required && r.path[0].value == "day_of"));
    }

    #[test]
    fn deduplication_makes_every_projected_column_row_level() {
        assert_eq!(names(&scan("select distinct region, segment from t")), vec!["region", "segment"]);
        assert_eq!(names(&scan("select a from t union select b from u")), vec!["a", "b"]);
        assert!(names(&scan("select a from t union all select b from u")).is_empty());
    }

    #[test]
    fn a_name_a_cte_computes_is_derived_and_one_it_passes_through_is_not() {
        let s = scan(
            "with r as (select id, user_id as customer_id, amount * 2 as doubled, status from src) \
             select * from r",
        );
        let derived: Vec<&str> = s.derived.iter().map(String::as_str).collect();
        assert_eq!(derived, vec!["customer_id", "doubled"]);
    }

    #[test]
    fn an_inlined_ephemeral_model_defines_known_columns_not_derived_ones() {
        let q = parse_query(
            "with __dbt__cte__flags as (select id, status = 'x' as is_x from t) \
             select count(id) as n from __dbt__cte__flags where is_x",
        )
        .unwrap();
        assert!(scan_rows(&q, &BTreeSet::new()).derived.contains("is_x"));
        let inlined = BTreeSet::from(["__dbt__cte__flags".to_string()]);
        assert!(!scan_rows(&q, &inlined).derived.contains("is_x"));
    }

    #[test]
    fn constructs_whose_row_effects_are_not_modelled_are_reported() {
        for sql in [
            "select a.x from a cross join b",
            "select a.x from a, b where a.k = b.k",
            "select x from a natural join b",
            "select t.c from (select x from a) as t(c)",
            "with t(c) as (select x from a) select c from t",
            "select * from a join lateral (select * from b where b.k = a.k) s on true",
        ] {
            assert!(scan(sql).opaque.is_some(), "not reported as opaque: {sql}");
        }
    }

    #[test]
    fn date_parts_and_niladic_functions_parse_as_names_the_keyword_list_covers() {
        let s = scan("select a from t where x > dateadd(day, -3, current_date) and datediff(week, y, z) > 1");
        for n in names(&s) {
            assert!(n == "x" || n == "y" || n == "z" || is_keyword_value(&n), "unexpected name `{n}`");
        }
        assert!(!is_keyword_value("amount"));
    }

    fn shaped(sql: &str) -> Shape {
        shape(&parse_query(sql).expect("parses")).expect("a shape")
    }

    #[test]
    fn a_projection_edit_leaves_the_skeleton_alone_and_a_filter_edit_does_not() {
        let base = shaped("with r as (select id, amount / 100 as amount from src) select * from r");
        let item = shaped("with r as (select id, amount / 100.0 as amount from src) select * from r");
        let filter = shaped("with r as (select id, amount / 100 as amount from src where id > 0) select * from r");
        assert_eq!(base.skeleton, item.skeleton);
        assert_ne!(base.skeleton, filter.skeleton);
        let changed: Vec<&str> = base
            .items
            .iter()
            .zip(&item.items)
            .filter(|(a, b)| a.1 != b.1)
            .map(|(_, b)| b.0.as_str())
            .collect();
        assert_eq!(changed, vec!["amount"]);
    }

    #[test]
    fn an_item_a_filter_or_grouping_uses_is_not_comparable() {
        // `region` is grouped on, so an edit to it must read as structural.
        let a = shaped("select upper(region) as region, sum(x) as s from t group by region");
        let b = shaped("select lower(region) as region, sum(x) as s from t group by region");
        assert_ne!(a.skeleton, b.skeleton);
        // A deduplicating SELECT freezes every item.
        assert!(shaped("select distinct x + 1 as y from t").items.is_empty());
    }

    #[test]
    fn a_subquery_inside_an_expression_stays_in_the_skeleton() {
        let a = shaped("select id from t where id in (select k from u)");
        let b = shaped("select id from t where id in (select k + 1 as k from u)");
        assert_ne!(a.skeleton, b.skeleton, "a change inside a filter's subquery is a row change");
    }

    #[test]
    fn set_operations_are_outside_what_the_shape_reasons_about() {
        assert!(shape(&parse_query("select a from t union all select b from u").unwrap()).is_none());
    }
}
