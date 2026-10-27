//! Incremental reparse: re-analyze only the models an edit can have changed.
//!
//! A reload hands the daemon the whole project again, but an editor save changes one
//! file. `build_project` analyzes every model on every call; this module analyzes the
//! **dirty** models (SQL text changed) and their **downstream closure**, and carries
//! every other model's lineage over from the previous build.
//!
//! ## Why the closure, and why it is enough
//!
//! A model's lineage is a function of its SQL, the set of model names, and the catalog
//! entries it looks up: the output columns of the tables it selects from. An edit can
//! change a model's output columns (a rename, a dropped column, a `select *` over an
//! upstream that changed), and a model selecting from it then looks up a different
//! entry, so it is re-analyzed too — transitively, the downstream closure. A model
//! outside the closure selects only from models whose columns did not move, so
//! analyzing it again would reproduce the lineage it already has.
//!
//! ## When it is not enough
//!
//! Over-rebuild is safe; under-rebuild is a data bug, and an incremental project that
//! differs from a full build is exactly that. So the incremental path runs only when
//! it can show its result is exact, and otherwise hands the reload to
//! `build_project_with_hints`, the call the daemon made before this existed:
//!
//! * there is no previous build (the first load);
//! * the set of model names changed — a name is what turns a table reference into a
//!   `depends_on` edge, so adding or removing one re-wires models whose SQL did not
//!   change;
//! * the sources changed — any model may read any source;
//! * a changed model does not parse or analyze, or the edit made a cycle — the full
//!   build is what reports those, in its own words;
//! * a model that reads a model outside its `depends_on` could see the edit (below).
//!
//! Materialization hints and the incremental config never need a reparse: hints are
//! re-applied to the carried-over models, and the config is not an input to lineage.
//!
//! ### Models that read outside their `depends_on`
//!
//! freshet-sql leaves a *top-level* CTE's name out of `depends_on`, because the CTE
//! shadows a same-named model for the whole query. But a reference to that name inside
//! a CTE body, before the CTE is defined — the "import CTE", `with orders as (select *
//! from orders)` — is resolved against the catalog, which is to say against the real
//! `orders` model, and whether the catalog holds `orders` yet depends on where the two
//! models fall in `build_project`'s topological order. Such a model has no edge from
//! `orders`, so it is not in the closure of an edit to `orders`, yet its lineage moves
//! with one; it can even move when an unrelated edit reshuffles the topological order.
//! Replaying that order exactly is not worth its fragility, so these models are found
//! from their SQL ([`tables_read`]) and the incremental path declines whenever the
//! edit could change what they see.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::ControlFlow;
use std::sync::Arc;

use freshet_core::{Column, Materialization, Model, Project};
use freshet_embed::build_project_with_hints;
use freshet_sql::{analyze, Catalog, ModelLineage};
use sqlparser::ast::{visit_relations, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// What a successful reload was built from, kept so the next reload can reparse
/// against it.
///
/// Immutable once built. A reload that fails leaves the previous one in place, and a
/// reload running alongside another reads a consistent one.
pub(crate) struct Built {
    /// The project this reload produced: the same `Arc` the cache serves.
    pub(crate) project: Arc<Project>,
    /// The sources it was analyzed against.
    sources: Catalog,
    /// Per model, every table name its SQL reads ([`tables_read`]). A model missing
    /// here did not parse on its own, which only a freshet-sql parsing differently from
    /// this crate could cause; the next reload then takes the full path. Shared, because
    /// a reload that edits no SQL hands it on unchanged.
    reads: Arc<BTreeMap<String, BTreeSet<String>>>,
}

/// Why a reload took the full path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FullRebuild {
    FirstLoad,
    ModelSetChanged,
    SourcesChanged,
    ReparseFailed,
    DependencyCycle,
    ReadsOutsideDependsOn,
    Unverifiable,
}

impl FullRebuild {
    /// The operator-facing explanation carried on `Response::Reloaded`.
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::FirstLoad => "first load: there is no previous build to reparse against",
            Self::ModelSetChanged => "the set of model names changed",
            Self::SourcesChanged => "the sources changed",
            Self::ReparseFailed => "a changed model failed to parse or analyze",
            Self::DependencyCycle => "the edit introduced a dependency cycle",
            Self::ReadsOutsideDependsOn => {
                "a model reads a model outside its depends_on (a top-level CTE shadows a model \
                 name), and this edit could change what it sees"
            }
            Self::Unverifiable => "the incremental result could not be shown to match a full build",
        }
    }
}

/// One successful reload.
pub(crate) struct Outcome {
    pub(crate) built: Built,
    /// How many models this reload analyzed.
    pub(crate) reparsed: usize,
    /// `Some` when the full path ran, saying why.
    pub(crate) full_rebuild: Option<FullRebuild>,
}

/// Build the project for `models`, incrementally against `prev` wherever that is exact.
///
/// An `Err` is always the full build's error: the incremental path hands every failure
/// to it rather than wording one of its own.
pub(crate) fn reload(
    prev: Option<&Built>,
    models: &[(String, String)],
    sources: &Catalog,
    hints: &BTreeMap<String, Materialization>,
) -> anyhow::Result<Outcome> {
    let why = match prev {
        None => FullRebuild::FirstLoad,
        Some(prev) => match incremental(prev, models, sources, hints) {
            Ok((built, reparsed)) => return Ok(Outcome { built, reparsed, full_rebuild: None }),
            Err(why) => why,
        },
    };
    let project = build_project_with_hints(models, sources, hints)?;
    // Parsing each model once more is the price of knowing what it reads; a model whose
    // SQL the previous build already parsed is not parsed again.
    let reads = models
        .iter()
        .filter_map(|(name, sql)| {
            let unchanged = prev.and_then(|p| {
                let same_sql = p.project.get(name).is_some_and(|m| m.sql == *sql);
                same_sql.then(|| p.reads.get(name).cloned()).flatten()
            });
            unchanged.or_else(|| tables_read(sql)).map(|reads| (name.clone(), reads))
        })
        .collect();
    let reparsed = project.len();
    Ok(Outcome {
        built: Built { project: Arc::new(project), sources: sources.clone(), reads: Arc::new(reads) },
        reparsed,
        full_rebuild: Some(why),
    })
}

/// The incremental path: the new build and how many models it analyzed, or why it
/// declined.
fn incremental(
    prev: &Built,
    models: &[(String, String)],
    sources: &Catalog,
    hints: &BTreeMap<String, Materialization>,
) -> Result<(Built, usize), FullRebuild> {
    let old = &*prev.project;

    // The same names, each once. A duplicate is a build error, and the full build is
    // what reports it.
    let known: BTreeSet<String> = models.iter().map(|(name, _)| name.clone()).collect();
    if known.len() != models.len() || !known.iter().eq(old.names()) {
        return Err(FullRebuild::ModelSetChanged);
    }
    if *sources != prev.sources {
        return Err(FullRebuild::SourcesChanged);
    }
    // `reads` is keyed by model name, so equal lengths mean every model has an entry.
    if prev.reads.len() != old.len() {
        return Err(FullRebuild::Unverifiable);
    }
    let model = |name: &str| old.get(name).expect("same model names as the previous build");
    // Mirrors `build_project_with_hints`: `build_project` makes every model a `Table`,
    // and a hint overrides that for a model that exists.
    let materialization = |name: &str| hints.get(name).copied().unwrap_or(Materialization::Table);
    let sql: BTreeMap<&str, &str> = models.iter().map(|(n, s)| (n.as_str(), s.as_str())).collect();
    let dirty: BTreeSet<&str> =
        sql.iter().filter(|(name, sql)| model(name).sql != **sql).map(|(name, _)| *name).collect();

    // No SQL moved, so no lineage did, and no topological order: only hints can differ.
    // The common editor save, so it costs a comparison of the SQL and nothing more, and
    // hands the previous project on untouched when the hints are unchanged too.
    if dirty.is_empty() {
        let same_hints = old.names().all(|name| model(name).materialization == materialization(name));
        let project = if same_hints {
            prev.project.clone()
        } else {
            let rehinted = old
                .names()
                .map(|name| Model { materialization: materialization(name), ..model(name).clone() });
            Arc::new(Project::from_models(rehinted))
        };
        let built = Built { project, sources: prev.sources.clone(), reads: prev.reads.clone() };
        return Ok((built, 0));
    }

    // What each dirty model reads now. One that does not parse fails `analyze` too.
    let mut fresh_reads: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
    for &name in &dirty {
        fresh_reads.insert(name, tables_read(sql[name]).ok_or(FullRebuild::ReparseFailed)?);
    }

    // Dirty plus downstream closure, over the previous build's edges. That is the new
    // graph's closure too: a clean model's `depends_on` cannot have moved (same SQL,
    // same names), and every edge a dirty model gained starts inside the set already.
    let mut affected: BTreeSet<&str> = dirty.clone();
    let mut queue: VecDeque<&str> = dirty.iter().copied().collect();
    while let Some(name) = queue.pop_front() {
        for down in old.downstream_of(name) {
            if affected.insert(down.as_str()) {
                queue.push_back(down.as_str());
            }
        }
    }

    // The catalog every reparsed model is analyzed against: the sources, overlaid with
    // each model's output columns as of the previous build. `build_project` instead grows
    // its catalog in topological order, so it holds only the models *before* the one
    // being analyzed. The two agree on every entry a model reading only its
    // `depends_on` can look up, and that is the only kind of model this path analyzes.
    let mut catalog = sources.clone();
    for name in old.names() {
        catalog.insert(name.clone(), column_names(&model(name).columns));
    }

    // Pass 1: the dirty models, for their new `depends_on` (which the catalog does not
    // influence) and so the order pass 2 has to run in.
    let mut first_pass: BTreeMap<&str, ModelLineage> = BTreeMap::new();
    for &name in &dirty {
        let lineage = analyze(sql[name], &known, &catalog).map_err(|_| FullRebuild::ReparseFailed)?;
        first_pass.insert(name, lineage);
    }

    // Models that read a model outside their `depends_on` (module docs). Re-analyzing
    // one would need `build_project`'s positional catalog, so none may be affected; one
    // left alone keeps its lineage only if the topological order it depends on cannot
    // have moved, i.e. no edge did.
    let mut readers_outside: Vec<Vec<String>> = Vec::new();
    for name in old.names() {
        let reads = match fresh_reads.get(name.as_str()) {
            Some(reads) => reads,
            None => prev.reads.get(name).ok_or(FullRebuild::Unverifiable)?,
        };
        let deps = depends_on(name, &first_pass, old);
        let outside: Vec<String> =
            reads.iter().filter(|t| known.contains(*t) && !deps.contains(t)).cloned().collect();
        if outside.is_empty() {
            continue;
        }
        if affected.contains(name.as_str()) {
            return Err(FullRebuild::ReadsOutsideDependsOn);
        }
        readers_outside.push(outside);
    }
    let edges_moved =
        dirty.iter().any(|name| depends_on(name, &first_pass, old) != model(name).depends_on);
    if edges_moved && !readers_outside.is_empty() {
        return Err(FullRebuild::ReadsOutsideDependsOn);
    }

    let mut edges: Vec<(&str, &str)> = Vec::new();
    for &name in &affected {
        for up in depends_on(name, &first_pass, old) {
            if let Some(&up) = affected.get(up.as_str()) {
                edges.push((up, name));
            }
        }
    }
    let order = topo_order(&affected, &edges).ok_or(FullRebuild::DependencyCycle)?;

    // Pass 2, in topological order, so each model sees its upstreams' new columns.
    let mut fresh: BTreeMap<&str, ModelLineage> = BTreeMap::new();
    for name in order {
        let deps = depends_on(name, &first_pass, old);
        let unaffected_upstreams = deps.iter().all(|up| !affected.contains(up.as_str()));
        let lineage = match first_pass.get(name) {
            // Pass 1 looked up only upstreams whose columns did not move: it stands.
            Some(lineage) if unaffected_upstreams => lineage.clone(),
            _ => {
                let lineage =
                    analyze(sql[name], &known, &catalog).map_err(|_| FullRebuild::ReparseFailed)?;
                // `depends_on` is a function of the SQL and the model names alone, which
                // is what made the order above valid. If it moved with the catalog, this
                // path is wrong about freshet-sql, and the full build is the answer.
                if lineage.depends_on != deps {
                    return Err(FullRebuild::Unverifiable);
                }
                lineage
            }
        };
        catalog.insert(name.to_string(), column_names(&lineage.columns));
        fresh.insert(name, lineage);
    }

    // A reader outside its `depends_on` that was left alone sees an affected model's new
    // output columns wherever the full build has analyzed that model first, which this
    // path cannot tell; it is exact only if those columns did not move.
    for outside in &readers_outside {
        for read in outside {
            let Some(lineage) = fresh.get(read.as_str()) else { continue };
            let now = lineage.columns.iter().map(|c| &c.name);
            if !now.eq(model(read).columns.iter().map(|c| &c.name)) {
                return Err(FullRebuild::ReadsOutsideDependsOn);
            }
        }
    }

    let reparsed = affected.len();
    let project = Project::from_models(old.names().map(|name| match fresh.remove(name.as_str()) {
        Some(ModelLineage { depends_on, columns }) => Model {
            name: name.clone(),
            sql: sql[name.as_str()].to_string(),
            materialization: materialization(name),
            columns,
            depends_on,
        },
        None => Model { materialization: materialization(name), ..model(name).clone() },
    }));
    let mut reads = (*prev.reads).clone();
    reads.extend(fresh_reads.into_iter().map(|(name, r)| (name.to_string(), r)));
    Ok((Built { project: Arc::new(project), sources: sources.clone(), reads: Arc::new(reads) }, reparsed))
}

/// A model's `depends_on` in the new graph: pass 1's for a dirty model, the previous
/// build's for any other (its SQL and the model names are unchanged, and those are all
/// `depends_on` is a function of).
fn depends_on<'a>(
    name: &str,
    first_pass: &'a BTreeMap<&str, ModelLineage>,
    old: &'a Project,
) -> &'a [String] {
    match first_pass.get(name) {
        Some(lineage) => &lineage.depends_on,
        None => old.get(name).map_or(&[], |m| m.depends_on.as_slice()),
    }
}

/// Kahn's algorithm over `nodes`; `None` on a cycle.
fn topo_order<'a>(nodes: &BTreeSet<&'a str>, edges: &[(&'a str, &'a str)]) -> Option<Vec<&'a str>> {
    let mut indegree: BTreeMap<&str, usize> = nodes.iter().map(|&n| (n, 0)).collect();
    let mut downstream: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for &(up, down) in edges {
        *indegree.get_mut(down)? += 1;
        downstream.entry(up).or_default().push(down);
    }
    let mut ready: VecDeque<&str> = indegree.iter().filter(|(_, &d)| d == 0).map(|(&n, _)| n).collect();
    let mut order = Vec::with_capacity(nodes.len());
    while let Some(name) = ready.pop_front() {
        order.push(name);
        for &down in downstream.get(name).into_iter().flatten() {
            let remaining = indegree.get_mut(down)?;
            *remaining -= 1;
            if *remaining == 0 {
                ready.push_back(down);
            }
        }
    }
    (order.len() == nodes.len()).then_some(order)
}

/// Every table name `sql` reads, keyed the way freshet-sql keys `depends_on` (the last
/// identifier of each relation), from anywhere in the query: CTE bodies and subqueries
/// included.
///
/// Every catalog lookup `analyze` makes is keyed by one of these names, which is what
/// makes "a known model in here but not in `depends_on`" a sound test for a model that
/// reads outside its dependencies. It is an over-approximation — a top-level CTE named
/// like a model is flagged even where every reference to it resolves to the CTE — and
/// the cost of that is only a full rebuild. `None` when `sql` is not exactly one query,
/// which freshet-sql rejects too.
fn tables_read(sql: &str) -> Option<BTreeSet<String>> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let Some(Statement::Query(query)) = statements.pop() else {
        return None;
    };
    let mut names = BTreeSet::new();
    let _ = visit_relations(&*query, |relation| {
        names.insert(relation.0.last().map(|ident| ident.value.clone()).unwrap_or_default());
        ControlFlow::<()>::Continue(())
    });
    Some(names)
}

fn column_names(columns: &[Column]) -> Vec<String> {
    columns.iter().map(|c| c.name.clone()).collect()
}

/// Whether two projects are the same to everything that plans from them.
///
/// `Model` has no `PartialEq`, so this compares field by field, and destructures
/// exhaustively: a field added upstream fails to compile here instead of being left out
/// of the comparison, which would let a changed project keep its revision — the lost
/// update the stale-plan guard exists to prevent.
///
/// The same allocation is the same project without looking: a reload that changed
/// nothing hands the previous `Arc` on, and then this costs nothing.
pub(crate) fn same_project(a: &Project, b: &Project) -> bool {
    if std::ptr::eq(a, b) {
        return true;
    }
    a.len() == b.len()
        && a.names().zip(b.names()).all(|(x, y)| match (a.get(x), b.get(y)) {
            (Some(x), Some(y)) => same_model(x, y),
            _ => false,
        })
}

fn same_model(a: &Model, b: &Model) -> bool {
    let Model { name, sql, materialization, columns, depends_on } = a;
    *name == b.name
        && *sql == b.sql
        && *materialization == b.materialization
        && *columns == b.columns
        && *depends_on == b.depends_on
}

#[cfg(test)]
mod tests;
