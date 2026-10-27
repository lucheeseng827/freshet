//! A dbt manifest, as a planner [`Project`].
//!
//! Two sources of truth, used for what each is good at:
//!
//! * **Edges come from the manifest.** A model's `depends_on` is the manifest's own
//!   `depends_on.nodes`, restricted to models. dbt resolved every `ref()` to write it,
//!   so it is right even for a model whose SQL Freshet cannot read — and correctness of
//!   the plan rests on it.
//! * **Column lineage comes from the SQL**, through [`freshet_sql::analyze`], in
//!   topological order so every upstream's columns are known before a downstream is
//!   read. It only ever *narrows* what a change reaches, so whenever it cannot be
//!   trusted it is dropped to model-level lineage (every column may depend on anything
//!   the model reads), never guessed.
//!
//! ## Relation names, not model names
//!
//! Compiled SQL reads `"db"."analytics"."stg_orders_v2"` — the model's *alias* — and
//! freshet-sql resolves a relation by its last identifier. So SQL is analyzed with the
//! catalog keyed by relation name (a model's `alias`, a source's `identifier`), and
//! every column reference is mapped back to a model name afterwards. Skipping that
//! mapping is an under-rebuild: a reference to `stg_orders_v2` never matches the model
//! `stg_orders`, so its downstream is silently left out. A relation name two relations
//! share (the same alias in two schemas) is not guessed at: no catalog entry, and
//! model-level lineage for every model reading it.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use freshet_core::{Column, ColumnRef, Materialization, Model, Project};
use freshet_sql::Catalog;
use serde::Serialize;

use crate::artifacts::{ColumnInfo, Manifest, Node};
use crate::jinja::{self, Call};
use crate::sqlscan;

/// The one column given to a model whose columns cannot be listed. Never zero columns:
/// a downstream with no columns gives the planner nothing to test, and a model with
/// none cannot say which of its columns changed.
pub const UNKNOWN_COLUMNS: &str = "*";

/// Which SQL a model was read from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlBasis {
    /// `compiled_code`: the SQL dbt rendered.
    Compiled,
    /// `raw_code` with `ref()` / `source()` rewritten; any other Jinja makes it unreadable.
    Raw,
}

/// How precisely a model's columns are traced to what they read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lineage {
    /// Each output column lists the upstream columns (and the row-deciding references)
    /// it depends on.
    Columns,
    /// Every column may depend on anything the model reads, so the model rebuilds
    /// whenever any upstream model does. Safe, never precise.
    ModelLevel { reason: String },
}

/// One dbt model as the planner sees it.
#[derive(Clone, Debug)]
pub struct DbtModel {
    pub unique_id: String,
    /// The planner's name for it: the dbt `name`, `name.vN` for a versioned model.
    pub name: String,
    /// `config.materialized`, verbatim.
    pub materialized: String,
    pub basis: SqlBasis,
    pub lineage: Lineage,
    /// Whether the manifest had compiled SQL for it (whatever basis was used).
    pub has_compiled_sql: bool,
    /// How many models it reads. Lineage precision only matters when this is non-zero.
    pub upstream_models: usize,
    /// alias / configured schema / configured database: where dbt builds it.
    pub(crate) relation: String,
    /// The model's source file (`raw_code`), whitespace-normalized — to tell a change
    /// someone made from compiled SQL that merely rendered differently.
    pub(crate) source: String,
    pub(crate) macros: BTreeSet<String>,
    /// Every non-model node it reads (source, seed, snapshot, function) and a digest of
    /// that node's definition — a changed input is a changed model.
    pub(crate) inputs: BTreeMap<String, String>,
}

impl DbtModel {
    /// Ephemeral models are inlined into their consumers as CTEs; dbt never runs them.
    pub fn is_ephemeral(&self) -> bool {
        self.materialized == "ephemeral"
    }
}

/// A manifest's models, linked into a [`Project`].
#[derive(Clone, Debug)]
pub struct DbtProject {
    pub project: Project,
    pub models: BTreeMap<String, DbtModel>,
    by_uid: BTreeMap<String, String>,
}

impl DbtProject {
    /// Read every model from its compiled SQL when the manifest has it, else its raw SQL.
    pub fn build(manifest: &Manifest) -> Self {
        Self::build_with(manifest, &default_basis)
    }

    /// Build with the basis decided per model — the diff reads both manifests the same way.
    pub(crate) fn build_with(manifest: &Manifest, basis: &dyn Fn(&Node) -> SqlBasis) -> Self {
        Builder::new(manifest).build(basis)
    }

    /// The planner name of the model with this `unique_id`.
    pub fn name_of(&self, unique_id: &str) -> Option<&str> {
        self.by_uid.get(unique_id).map(String::as_str)
    }

    pub(crate) fn model(&self, name: &str) -> &Model {
        self.project.get(name).expect("every DbtModel is in the project")
    }
}

fn is_ephemeral_node(node: &Node) -> bool {
    node.resource_type == "model" && node.config.materialized.as_deref() == Some("ephemeral")
}

pub(crate) fn has_compiled_sql(node: &Node) -> bool {
    node.compiled_code.as_deref().is_some_and(|c| !c.trim().is_empty())
}

fn default_basis(node: &Node) -> SqlBasis {
    if has_compiled_sql(node) {
        SqlBasis::Compiled
    } else {
        SqlBasis::Raw
    }
}

/// `table` → Table, `view` / `ephemeral` → View, `incremental` → Incremental.
///
/// Anything else (`snapshot`, `seed`, `materialized_view`, a custom materialization)
/// plans as a Table, the conservative reading on every axis the planner has: a Table
/// always rebuilds whole (never partition-scoped), is costed at the full weight (1 CU,
/// never the 0.05 CU of a view), and column pruning treats every materialization alike.
/// Over-estimating a rebuild is safe; under-estimating one is not.
pub fn materialization(materialized: &str) -> Materialization {
    match materialized {
        "table" => Materialization::Table,
        "view" | "ephemeral" => Materialization::View,
        "incremental" => Materialization::Incremental,
        _ => Materialization::Table,
    }
}

/// Documented column names, in the manifest's (sorted) order.
fn documented(columns: &BTreeMap<String, ColumnInfo>) -> Vec<String> {
    let mut names: Vec<String> = columns
        .iter()
        .map(|(key, info)| if info.name.is_empty() { key.clone() } else { info.name.clone() })
        .collect();
    names.dedup();
    names
}

fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

// ------------------------------------------------------------------ registry --

/// A relation dbt renders into SQL.
#[derive(Clone, Debug)]
struct Relation {
    uid: String,
    /// The planner name, for a model.
    model: Option<String>,
}

/// Every relation in a manifest, by the name compiled SQL uses for it.
struct Registry {
    by_key: BTreeMap<String, Vec<Relation>>,
    key_of: BTreeMap<String, String>,
    /// Fully rendered relation name (`"db"."schema"."alias"`) → unique_id.
    by_relation_name: HashMap<String, String>,
}

impl Registry {
    fn new(manifest: &Manifest, names: &BTreeMap<String, String>) -> Self {
        let mut reg = Registry { by_key: BTreeMap::new(), key_of: BTreeMap::new(), by_relation_name: HashMap::new() };
        for (uid, node) in &manifest.nodes {
            if !matches!(node.resource_type.as_str(), "model" | "seed" | "snapshot") {
                continue;
            }
            let key = node.alias.clone().filter(|a| !a.is_empty()).unwrap_or_else(|| node.name.clone());
            reg.add(uid, key, names.get(uid).cloned(), node.relation_name.as_deref());
        }
        for (uid, source) in &manifest.sources {
            let key = source.identifier.clone().filter(|i| !i.is_empty()).unwrap_or_else(|| source.name.clone());
            reg.add(uid, key, None, source.relation_name.as_deref());
        }
        reg
    }

    fn add(&mut self, uid: &str, key: String, model: Option<String>, relation_name: Option<&str>) {
        self.by_key.entry(key.clone()).or_default().push(Relation { uid: uid.to_string(), model });
        self.key_of.insert(uid.to_string(), key);
        if let Some(rn) = relation_name.filter(|rn| !rn.is_empty()) {
            self.by_relation_name.insert(rn.to_string(), uid.to_string());
        }
    }

    /// The single relation behind `key`; `None` when none or several share it.
    fn unique(&self, key: &str) -> Option<&Relation> {
        match self.by_key.get(key).map(Vec::as_slice) {
            Some([one]) => Some(one),
            _ => None,
        }
    }

    fn is_ambiguous(&self, key: &str) -> bool {
        self.by_key.get(key).is_some_and(|rs| rs.len() > 1)
    }
}

/// The node a `ref()` / `source()` call in `node`'s raw SQL renders to: the one entry
/// of its `depends_on` that matches. `None` when no entry or several do.
fn resolve_call(manifest: &Manifest, node: &Node, call: &Call<'_>) -> Option<String> {
    let mut hits = node.depends_on.nodes.iter().filter(|uid| match call {
        Call::Ref { package, name } => manifest.nodes.get(*uid).is_some_and(|n| {
            matches!(n.resource_type.as_str(), "model" | "seed" | "snapshot")
                && n.name == *name
                && package.is_none_or(|p| n.package_name == p)
        }),
        Call::Source { source, table } => {
            manifest.sources.get(*uid).is_some_and(|s| s.source_name == *source && s.name == *table)
        }
    });
    let first = hits.next()?;
    if hits.any(|other| other != first) {
        return None;
    }
    Some(first.clone())
}

/// Replace every fully rendered relation name in `sql` with the node's quoted
/// `unique_id`.
///
/// Compiled SQL names relations the way the target renders them — `"dev"."dbt_alice"`
/// in development, `"prod"."analytics"` in production — so the same unchanged model
/// compiles to different text in two environments. Fingerprints are taken over this
/// normalized text, so a comparison across environments sees code changes, not schema
/// names. Re-pointing a reference at a *different* node still changes the text: the
/// unique_id differs. String literals are skipped; nothing is matched by prefix.
fn normalize_relations(sql: &str, names: &HashMap<String, String>) -> String {
    if names.is_empty() {
        return sql.to_string();
    }
    let bytes = sql.as_bytes();
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80;
    let mut out = String::with_capacity(sql.len());
    let (mut i, mut emitted) = (0, 0);
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            // Skip a string literal ('' is an escaped quote inside one).
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if bytes.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i += 1;
            continue;
        }
        let starts_part = b == b'"' || b == b'`' || (is_word(b) && !b.is_ascii_digit());
        let continues_word = i > 0 && (is_word(bytes[i - 1]) || bytes[i - 1] == b'.');
        if !starts_part || continues_word {
            i += 1;
            continue;
        }
        // A dotted chain of identifier parts; remember where each part ends.
        let mut ends = Vec::new();
        let mut j = i;
        loop {
            let part_end = match bytes.get(j) {
                Some(&q @ (b'"' | b'`')) => {
                    let mut k = j + 1;
                    loop {
                        match bytes.get(k) {
                            Some(&c) if c == q && bytes.get(k + 1) == Some(&q) => k += 2,
                            Some(&c) if c == q => break Some(k + 1),
                            Some(_) => k += 1,
                            None => break None,
                        }
                    }
                }
                Some(&c) if is_word(c) => {
                    let mut k = j;
                    while k < bytes.len() && is_word(bytes[k]) {
                        k += 1;
                    }
                    Some(k)
                }
                _ => None,
            };
            let Some(end) = part_end else { break };
            ends.push(end);
            if bytes.get(end) == Some(&b'.') && end + 1 < bytes.len() {
                j = end + 1;
            } else {
                break;
            }
        }
        let Some(&chain_end) = ends.last() else {
            i += 1;
            continue;
        };
        let matched = ends.iter().rev().find_map(|&end| names.get(&sql[i..end]).map(|uid| (end, uid)));
        if let Some((end, uid)) = matched {
            out.push_str(&sql[emitted..i]);
            out.push_str(&quote(uid));
            emitted = end;
            i = end;
        } else {
            i = chain_end;
        }
    }
    out.push_str(&sql[emitted..]);
    out
}

/// A digest of a non-model input's definition: what makes a seed, snapshot, function
/// or source "changed" between two manifests.
fn input_digest(manifest: &Manifest, uid: &str) -> String {
    if let Some(node) = manifest.nodes.get(uid).or_else(|| manifest.functions.get(uid)) {
        return match &node.checksum {
            Some(c) if !c.checksum.is_empty() => format!("{}:{}", c.name, c.checksum),
            _ => format!("code:{}", node.raw_code.as_deref().unwrap_or_default()),
        };
    }
    if let Some(source) = manifest.sources.get(uid) {
        // The YAML's own values when the manifest records them: the rendered ones
        // differ between targets that read the same table.
        let database = source.unrendered_database.as_ref().or(source.database.as_ref());
        let schema = source.unrendered_schema.as_ref().or(source.schema.as_ref());
        let identifier = source.identifier.as_deref().unwrap_or(&source.name);
        return format!(
            "source:{}.{}.{}",
            database.map(String::as_str).unwrap_or_default(),
            schema.map(String::as_str).unwrap_or_default(),
            identifier
        );
    }
    "missing".to_string()
}

// ------------------------------------------------------------------- builder --

struct Builder<'m> {
    manifest: &'m Manifest,
    /// unique_id → planner name, for every model.
    names: BTreeMap<String, String>,
    /// Planner names of the ephemeral models.
    ephemeral: BTreeSet<String>,
    registry: Registry,
    known_keys: BTreeSet<String>,
    catalog: Catalog,
    /// planner name → its final column names (what a downstream can reference).
    columns_of: BTreeMap<String, Vec<String>>,
}

/// What reading one model's SQL produced.
struct Read {
    columns: Vec<Column>,
    lineage: Lineage,
    /// The output names downstream SQL can resolve, when they are known.
    catalog_names: Option<Vec<String>>,
}

impl Read {
    fn model_level(names: Vec<String>, reason: String, catalog_names: Option<Vec<String>>) -> Self {
        let names = if names.is_empty() { vec![UNKNOWN_COLUMNS.to_string()] } else { names };
        Read {
            columns: names.into_iter().map(|n| Column::new(n, Vec::new())).collect(),
            lineage: Lineage::ModelLevel { reason },
            catalog_names,
        }
    }
}

impl<'m> Builder<'m> {
    fn new(manifest: &'m Manifest) -> Self {
        let names = planner_names(manifest);
        let registry = Registry::new(manifest, &names);
        let known_keys = names.keys().filter_map(|uid| registry.key_of.get(uid).cloned()).collect();
        // Sources, seeds and snapshots seed the catalog with their documented columns.
        let mut catalog = Catalog::new();
        let documented_inputs = manifest
            .sources
            .iter()
            .map(|(uid, s)| (uid, &s.columns))
            .chain(
                manifest
                    .nodes
                    .iter()
                    .filter(|(_, n)| matches!(n.resource_type.as_str(), "seed" | "snapshot"))
                    .map(|(uid, n)| (uid, &n.columns)),
            );
        for (uid, columns) in documented_inputs {
            let cols = documented(columns);
            let Some(key) = registry.key_of.get(uid) else { continue };
            if !cols.is_empty() && !registry.is_ambiguous(key) {
                catalog.insert(key.clone(), cols);
            }
        }
        let ephemeral =
            names.iter().filter(|(uid, _)| is_ephemeral_node(&manifest.nodes[*uid])).map(|(_, n)| n.clone()).collect();
        Builder { manifest, names, ephemeral, registry, known_keys, catalog, columns_of: BTreeMap::new() }
    }

    fn build(mut self, basis_for: &dyn Fn(&Node) -> SqlBasis) -> DbtProject {
        let manifest = self.manifest;
        let model_deps: BTreeMap<String, Vec<String>> = self
            .names
            .keys()
            .map(|uid| {
                let mut deps: Vec<String> = manifest.nodes[uid]
                    .depends_on
                    .nodes
                    .iter()
                    .filter(|d| self.names.contains_key(*d) && *d != uid)
                    .cloned()
                    .collect();
                deps.sort();
                deps.dedup();
                (uid.clone(), deps)
            })
            .collect();

        let mut models = BTreeMap::new();
        let mut planner_models = Vec::new();
        for uid in topo_order(&model_deps) {
            let node = &manifest.nodes[&uid];
            let name = self.names[&uid].clone();
            let basis = basis_for(node);
            let deps: Vec<String> = model_deps[&uid].iter().map(|d| self.names[d].clone()).collect();
            let read = self.read(node, basis, &deps);

            let key = &self.registry.key_of[&uid];
            if let Some(cols) = &read.catalog_names {
                if !self.registry.is_ambiguous(key) {
                    self.catalog.insert(key.clone(), cols.clone());
                }
            }
            self.columns_of.insert(name.clone(), read.columns.iter().map(|c| c.name.clone()).collect());

            let materialized = node.config.materialized.clone().unwrap_or_else(|| "view".to_string());
            let inputs = node
                .depends_on
                .nodes
                .iter()
                .filter(|d| !self.names.contains_key(*d))
                .map(|d| (d.clone(), input_digest(manifest, d)))
                .collect();
            let relation = format!(
                "{}|{}|{}",
                key,
                node.config.schema.as_deref().unwrap_or_default(),
                node.config.database.as_deref().unwrap_or_default()
            );
            planner_models.push(Model {
                name: name.clone(),
                sql: self.canonical_sql(node, basis),
                materialization: materialization(&materialized),
                columns: read.columns,
                depends_on: deps.clone(),
            });
            models.insert(
                name.clone(),
                DbtModel {
                    unique_id: uid.clone(),
                    name,
                    materialized,
                    basis,
                    lineage: read.lineage,
                    has_compiled_sql: has_compiled_sql(node),
                    upstream_models: deps.len(),
                    relation,
                    source: freshet_fingerprint::normalize_sql_fallback(node.raw_code.as_deref().unwrap_or_default()),
                    macros: node.depends_on.macros.iter().cloned().collect(),
                    inputs,
                },
            );
        }
        let by_uid = self.names.iter().map(|(uid, name)| (uid.clone(), name.clone())).collect();
        DbtProject { project: Project::from_models(planner_models), models, by_uid }
    }

    /// The SQL fingerprints are taken over: environment-independent, and the same
    /// basis on both sides of a comparison.
    fn canonical_sql(&self, node: &Node, basis: SqlBasis) -> String {
        match basis {
            SqlBasis::Compiled => {
                normalize_relations(node.compiled_code.as_deref().unwrap_or_default(), &self.registry.by_relation_name)
            }
            SqlBasis::Raw => jinja::rewrite(node.raw_code.as_deref().unwrap_or_default(), |call| {
                resolve_call(self.manifest, node, call).map(|uid| quote(&uid))
            }),
        }
    }

    /// The SQL lineage is read from, with every relation named by its catalog key.
    fn analysis_sql(&self, node: &Node, basis: SqlBasis) -> Result<String, String> {
        match basis {
            SqlBasis::Compiled => Ok(node.compiled_code.clone().unwrap_or_default()),
            SqlBasis::Raw => {
                let raw = node.raw_code.as_deref().unwrap_or_default();
                let sql = jinja::rewrite(raw, |call| {
                    let uid = resolve_call(self.manifest, node, call)?;
                    self.registry.key_of.get(&uid).map(|key| quote(key))
                });
                if jinja::has_jinja(&sql) {
                    Err("its SQL has Jinja beyond ref()/source() and the manifest holds no compiled SQL \
                         for it (run `dbt compile`)"
                        .to_string())
                } else {
                    Ok(sql)
                }
            }
        }
    }

    /// Map a reference from relation key to planner name (a model) or unique_id (any
    /// other relation, which the planner never matches against a model). `None` if the
    /// key does not name exactly one relation.
    fn map_ref(&self, r: &ColumnRef) -> Option<ColumnRef> {
        let rel = self.registry.unique(&r.model)?;
        Some(ColumnRef::new(rel.model.clone().unwrap_or_else(|| rel.uid.clone()), r.column.clone()))
    }

    fn read(&mut self, node: &Node, basis: SqlBasis, model_deps: &[String]) -> Read {
        let fallback = |reason: String| Read::model_level(documented(&node.columns), reason, None);

        if let Some(lang) = node.language.as_deref().filter(|l| !l.eq_ignore_ascii_case("sql")) {
            return fallback(format!("it is a {lang} model; Freshet reads SQL only"));
        }
        let sql = match self.analysis_sql(node, basis) {
            Ok(sql) => sql,
            Err(reason) => return fallback(reason),
        };
        let lineage = match freshet_sql::analyze(&sql, &self.known_keys, &self.catalog) {
            Ok(l) => l,
            Err(e) => {
                let msg = e.to_string();
                let first = msg.lines().next().unwrap_or_default();
                return fallback(format!("Freshet could not analyze its SQL ({first})"));
            }
        };
        if lineage.columns.is_empty() {
            return fallback("its SQL has no output columns Freshet can name".to_string());
        }
        let names: Vec<String> = lineage.columns.iter().map(|c| c.name.clone()).collect();
        let dropped = |reason: String| Read::model_level(names.clone(), reason, Some(names.clone()));

        // A model that reads no other model has nothing lineage could prune.
        if model_deps.is_empty() {
            let columns = lineage
                .columns
                .into_iter()
                .map(|c| Column::new(c.name, c.upstream.iter().filter_map(|r| self.map_ref(r)).collect()))
                .collect();
            return Read { columns, lineage: Lineage::Columns, catalog_names: Some(names) };
        }
        // Only in a cyclic graph (which dbt refuses to run) is an upstream still unread.
        if let Some(dep) = model_deps.iter().find(|d| !self.columns_of.contains_key(*d)) {
            return dropped(format!("it is part of a dependency cycle through `{dep}`"));
        }

        if let Some(key) = node
            .depends_on
            .nodes
            .iter()
            .filter_map(|d| self.registry.key_of.get(d))
            .find(|key| self.registry.is_ambiguous(key))
        {
            let n = self.registry.by_key[key].len();
            return dropped(format!(
                "it reads `{key}`, a relation name {n} dbt relations share; Freshet will not guess \
                 which one a column comes from"
            ));
        }

        let mut columns: Vec<Column> = lineage
            .columns
            .into_iter()
            .map(|c| {
                // A reference that does not map to exactly one relation is uncertain,
                // which makes the whole column's lineage unknown.
                let mapped: Option<Vec<ColumnRef>> = c.upstream.iter().map(|r| self.map_ref(r)).collect();
                Column::new(c.name, mapped.unwrap_or_default())
            })
            .collect();

        if columns.iter().all(|c| c.upstream.is_empty()) {
            return dropped(self.why_unknown(node, model_deps));
        }

        // Row-deciding references: a filter, join key or grouping decides the rows of
        // every output column, so each one joins every column's upstream.
        let Some(query) = sqlscan::parse_query(&sql) else {
            return dropped("Freshet could not re-read its SQL".to_string());
        };
        let inlined = if basis == SqlBasis::Compiled { self.inlined_ctes(node) } else { BTreeSet::new() };
        let scan = sqlscan::scan_rows(&query, &inlined);
        if let Some(what) = scan.opaque {
            return dropped(format!("it uses {what}, and Freshet does not model how that decides rows"));
        }
        let mut row_refs: BTreeSet<ColumnRef> = BTreeSet::new();
        for r in &scan.refs {
            let last = r.path.last().expect("identifier paths are never empty");
            let name = &last.value;
            if scan.derived.contains(name) {
                return dropped(format!(
                    "a filter, join or grouping reads `{name}`, which the model computes in a CTE or \
                     subquery"
                ));
            }
            let hits = self.input_columns_named(node, name);
            if hits.is_empty() {
                if !r.required || (last.quote_style.is_none() && sqlscan::is_keyword_value(name)) {
                    continue;
                }
                return dropped(format!(
                    "a filter, join or grouping reads `{name}`, and Freshet cannot tell which relation \
                     it comes from"
                ));
            }
            row_refs.extend(hits);
        }
        for col in columns.iter_mut().filter(|c| !c.upstream.is_empty()) {
            col.upstream.extend(row_refs.iter().cloned());
            col.upstream.sort();
            col.upstream.dedup();
        }

        // A column whose lineage is unknown, next to columns whose lineage is known:
        // spell "may depend on anything" out as every column of every model it reads.
        // Left empty, the planner would rebuild the model but report only the known
        // columns as changed, and a downstream reading just the unknown one would be
        // skipped.
        let everything: Vec<ColumnRef> = model_deps
            .iter()
            .flat_map(|d| self.columns_of[d].iter().map(move |c| ColumnRef::new(d.clone(), c.clone())))
            .collect();
        for col in columns.iter_mut().filter(|c| c.upstream.is_empty()) {
            col.upstream = everything.clone();
        }

        // A model it reads that no column's lineage mentions reaches it some other way
        // (a semi-join, an aggregate over its rows) — count all of it against every
        // column. So does an ephemeral model under compiled SQL, however much the
        // lineage mentions it: dbt inlined its SQL as a CTE, so whatever was projected
        // from it traces past it, to what it reads, and a change to it alone would
        // otherwise match nothing.
        for dep in model_deps {
            let inlined = basis == SqlBasis::Compiled && self.ephemeral.contains(dep);
            if inlined || !columns.iter().any(|c| c.upstream.iter().any(|r| &r.model == dep)) {
                let all: Vec<ColumnRef> =
                    self.columns_of[dep].iter().map(|c| ColumnRef::new(dep.clone(), c.clone())).collect();
                for col in columns.iter_mut() {
                    col.upstream.extend(all.iter().cloned());
                    col.upstream.sort();
                    col.upstream.dedup();
                }
            }
        }

        Read { columns, lineage: Lineage::Columns, catalog_names: Some(names) }
    }

    /// Every input of `node` (by `depends_on`) whose known columns include `name`.
    ///
    /// An ephemeral model is inlined into its consumer's compiled SQL as a CTE, so the
    /// consumer's SQL also reads whatever the ephemeral model reads; those count too.
    /// (Such a reference points past the ephemeral model, at a relation the consumer
    /// does not list — which is why [`read`](Self::read) also counts an inlined
    /// ephemeral model in full against every column of its reader.)
    fn input_columns_named(&self, node: &Node, name: &str) -> Vec<ColumnRef> {
        let mut out = Vec::new();
        let mut pending: Vec<&str> = node.depends_on.nodes.iter().map(String::as_str).collect();
        let mut seen = BTreeSet::new();
        while let Some(uid) = pending.pop() {
            if !seen.insert(uid) {
                continue;
            }
            if let Some(key) = self.registry.key_of.get(uid) {
                if self.catalog.get(key).is_some_and(|cols| cols.iter().any(|c| c == name)) {
                    let owner = self.names.get(uid).cloned().unwrap_or_else(|| uid.to_string());
                    out.push(ColumnRef::new(owner, name));
                }
            }
            if let Some(inlined) = self.manifest.nodes.get(uid).filter(|n| is_ephemeral_node(n)) {
                pending.extend(inlined.depends_on.nodes.iter().map(String::as_str));
            }
        }
        out
    }

    /// The CTE names dbt inlines `node`'s ephemeral inputs under in its compiled SQL
    /// (`__dbt__cte__<alias>`), following ephemeral models that read ephemeral models.
    fn inlined_ctes(&self, node: &Node) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        let mut pending: Vec<&str> = node.depends_on.nodes.iter().map(String::as_str).collect();
        let mut seen = BTreeSet::new();
        while let Some(uid) = pending.pop() {
            if !seen.insert(uid) {
                continue;
            }
            let Some(inlined) = self.manifest.nodes.get(uid).filter(|n| is_ephemeral_node(n)) else { continue };
            if let Some(key) = self.registry.key_of.get(uid) {
                out.insert(format!("__dbt__cte__{key}"));
            }
            pending.extend(inlined.depends_on.nodes.iter().map(String::as_str));
        }
        out
    }

    /// Why no output column of `node` could be traced.
    fn why_unknown(&self, node: &Node, model_deps: &[String]) -> String {
        for uid in &node.depends_on.nodes {
            let Some(key) = self.registry.key_of.get(uid) else { continue };
            if self.catalog.contains_key(key) {
                continue;
            }
            return match self.names.get(uid) {
                Some(model) if model_deps.contains(model) => {
                    format!("it reads `{model}`, whose columns Freshet could not derive")
                }
                _ => format!("it reads `{uid}`, whose columns are not documented"),
            };
        }
        "no output column's source could be proven from its SQL".to_string()
    }
}

/// Planner names: the dbt `name`, `name.vN` for a versioned model (versions share a
/// name), and the unique_id for anything that still collides.
fn planner_names(manifest: &Manifest) -> BTreeMap<String, String> {
    let base: BTreeMap<String, String> = manifest
        .nodes
        .iter()
        .filter(|(_, n)| n.resource_type == "model")
        .map(|(uid, n)| {
            let name = match &n.version {
                Some(serde_json::Value::String(v)) if !v.is_empty() => {
                    format!("{}.{}", n.name, if v.starts_with('v') { v.clone() } else { format!("v{v}") })
                }
                Some(serde_json::Value::Number(v)) => format!("{}.v{v}", n.name),
                _ => n.name.clone(),
            };
            (uid.clone(), name)
        })
        .collect();
    let mut seen: BTreeMap<&String, usize> = BTreeMap::new();
    for name in base.values() {
        *seen.entry(name).or_default() += 1;
    }
    base.iter()
        .map(|(uid, name)| (uid.clone(), if seen[name] > 1 { uid.clone() } else { name.clone() }))
        .collect()
}

/// Kahn's algorithm over unique_ids, smallest-first so the order is deterministic.
/// Anything left in a cycle is appended in name order: the planner copes with cycles,
/// and lineage for those models just sees fewer upstream columns (more conservative).
fn topo_order(deps: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    let mut indegree: BTreeMap<&str, usize> = deps.keys().map(|k| (k.as_str(), 0)).collect();
    let mut downstream: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (uid, ups) in deps {
        for up in ups {
            *indegree.get_mut(uid.as_str()).expect("keyed above") += 1;
            downstream.entry(up.as_str()).or_default().push(uid.as_str());
        }
    }
    let mut ready: BTreeSet<&str> = indegree.iter().filter(|(_, d)| **d == 0).map(|(k, _)| *k).collect();
    let mut order = Vec::with_capacity(deps.len());
    while let Some(uid) = ready.pop_first() {
        order.push(uid.to_string());
        for d in downstream.get(uid).into_iter().flatten() {
            let e = indegree.get_mut(d).expect("keyed above");
            *e -= 1;
            if *e == 0 {
                ready.insert(d);
            }
        }
    }
    if order.len() < deps.len() {
        let placed: BTreeSet<String> = order.iter().cloned().collect();
        order.extend(deps.keys().filter(|k| !placed.contains(*k)).cloned());
    }
    order
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relation_names_are_replaced_whole_and_only_whole() {
        let names = HashMap::from([
            ("\"dev\".\"dbt\".\"orders\"".to_string(), "model.p.orders".to_string()),
            ("raw.shop.orders".to_string(), "source.p.shop.orders".to_string()),
        ]);
        let sql = "select * from \"dev\".\"dbt\".\"orders\" o join raw.shop.orders r using (id) \
                   join raw.shop.orders_v2 v using (id) where o.note = 'raw.shop.orders'";
        assert_eq!(
            normalize_relations(sql, &names),
            "select * from \"model.p.orders\" o join \"source.p.shop.orders\" r using (id) \
             join raw.shop.orders_v2 v using (id) where o.note = 'raw.shop.orders'"
        );
    }

    #[test]
    fn a_qualified_column_after_a_relation_name_keeps_its_column() {
        let names = HashMap::from([("\"db\".\"s\".\"t\"".to_string(), "model.p.t".to_string())]);
        assert_eq!(
            normalize_relations("select \"db\".\"s\".\"t\".x from \"db\".\"s\".\"t\"", &names),
            "select \"model.p.t\".x from \"model.p.t\""
        );
    }

    #[test]
    fn unknown_materializations_plan_as_tables() {
        assert_eq!(materialization("ephemeral"), Materialization::View);
        assert_eq!(materialization("incremental"), Materialization::Incremental);
        for other in ["snapshot", "seed", "materialized_view", "dynamic_table", "insert_by_period"] {
            assert_eq!(materialization(other), Materialization::Table, "{other}");
        }
    }

    #[test]
    fn a_cycle_does_not_lose_models() {
        let deps = BTreeMap::from([
            ("a".to_string(), vec!["b".to_string()]),
            ("b".to_string(), vec!["a".to_string()]),
            ("c".to_string(), vec![]),
        ]);
        let order = topo_order(&deps);
        assert_eq!(order.len(), 3);
        assert_eq!(order[0], "c");
    }
}
