//! Exporters: one [`GeneratedProject`] written three ways — a dbt project, a SQLMesh
//! project and a `freshet` project directory — so the out-of-band comparison in
//! `scripts/compare.sh` runs every tool over the **same** topology.
//!
//! ## Why the SQL is rewritten in place rather than re-rendered
//!
//! Each tool spells a table reference its own way: dbt wants `{{ ref() }}` /
//! `{{ source() }}`, SQLMesh a schema-qualified name, Freshet the bare name the
//! generator already wrote. Re-rendering every model from structure would work for
//! today's `select … from parent` generator, but it would be a second copy of the
//! generator, free to drift from the first the day someone adds a join or a filter —
//! and a comparison whose three inputs quietly differ measures nothing.
//!
//! So [`rewrite_relations`] replaces exactly the relation-name tokens and copies every
//! other byte through. That makes the "same topology" claim checkable, and the tests
//! check it: undo the rewrite and the generated SQL comes back byte for byte. Which
//! tokens are relations is decided by sqlparser's relation visitor — the parser the
//! planner itself uses — and the rewrite refuses to guess: if a column, alias or
//! function shares a relation's name, the number of rewritten tokens and the number
//! of relations the parser saw disagree, and the export fails rather than emitting a
//! subtly different model.
//!
//! Rendering happens in memory ([`Files`]) and only then is written, so a model that
//! fails to rewrite leaves nothing half-exported on disk.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, bail, ensure, Context, Result};
use freshet_core::Project;
use freshet_sql::Catalog;
use serde::{Deserialize, Serialize};
use sqlparser::ast::visit_relations;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, TokenWithLocation, Tokenizer};

use crate::{generate, EditSpec, GenSpec, GeneratedProject};

/// What [`export`] wrote. `freshet-bench-export` prints it as JSON; `compare.sh`
/// keeps it beside the timings and `freshet-bench-manifest` takes the spec and the
/// edit from it, so the manifest describes the project that was actually exported.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExportSummary {
    pub spec: GenSpec,
    pub models: usize,
    /// Model → model dependency edges (reads of a source are not edges).
    pub edges: usize,
    pub edit: Option<EditSpec>,
    /// Format name → the directory it was written to.
    pub dirs: BTreeMap<String, PathBuf>,
}

/// Generate `spec`, apply `edit` if given, and write each of `formats` to
/// `out/<format>`.
///
/// Everything is rendered before anything is written, so a failed export (an edit
/// that does not apply, an ambiguous rewrite) leaves no partial project behind.
pub fn export(spec: &GenSpec, formats: &[Format], edit: Option<&str>, out: &Path) -> Result<ExportSummary> {
    let base = generate(spec);
    let project =
        freshet_sql::build_project(&base.models, &base.sources).context("the generated project does not build")?;
    let edges = project.names().map(|n| project.get(n).map_or(0, |m| m.depends_on.len())).sum();

    let (g, edit) = match edit {
        Some(model) => {
            let closure = downstream_closure(&project, model).len();
            (edit_model(&base, model)?, Some(EditSpec { model: model.to_string(), closure }))
        }
        None => (base, None),
    };

    let rendered = formats.iter().map(|&f| Ok((f, f.render(&g)?))).collect::<Result<Vec<_>>>()?;
    let mut dirs = BTreeMap::new();
    for (format, files) in rendered {
        let dir = out.join(format.dir_name());
        write_files(&dir, &files)?;
        dirs.insert(format.dir_name().to_string(), dir);
    }
    Ok(ExportSummary { spec: *spec, models: g.models.len(), edges, edit, dirs })
}

/// The dbt source name, the dbt target schema and the SQLMesh schema.
///
/// One name for all three so every tool reads and writes the same relations:
/// dbt's `{{ source('bench', 'src') }}` and SQLMesh's `bench.src` are the same table,
/// and dbt's `m_0_0` and SQLMesh's `bench.m_0_0` land in the same schema.
pub const SCHEMA: &str = "bench";

/// The dbt project and profile name.
pub const DBT_PROJECT: &str = "freshet_bench";

/// The local DuckDB file both dbt and SQLMesh point at, relative to the project root.
///
/// Deliberately *not* `bench.duckdb`: DuckDB names the catalog after the file stem, and
/// a catalog and a schema both called `bench` make `bench.src` ambiguous the moment
/// anything executes. Planning never executes, but `compare.sh` has to apply one
/// SQLMesh plan (untimed) to give the warm operations a populated state to diff
/// against, and that apply runs the models.
pub const DUCKDB_FILE: &str = "warehouse.duckdb";

/// A rendered project: path relative to the project root → file contents.
pub type Files = BTreeMap<PathBuf, String>;

/// The three project layouts the comparison needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Format {
    Dbt,
    Sqlmesh,
    Freshet,
}

impl Format {
    pub const ALL: [Format; 3] = [Format::Dbt, Format::Sqlmesh, Format::Freshet];

    /// The subdirectory `freshet-bench-export` writes this format to.
    pub fn dir_name(self) -> &'static str {
        match self {
            Format::Dbt => "dbt",
            Format::Sqlmesh => "sqlmesh",
            Format::Freshet => "freshet",
        }
    }

    /// Render `g` in this format.
    pub fn render(self, g: &GeneratedProject) -> Result<Files> {
        match self {
            Format::Dbt => dbt_files(g),
            Format::Sqlmesh => sqlmesh_files(g),
            Format::Freshet => freshet_files(g),
        }
    }
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.dir_name())
    }
}

impl FromStr for Format {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Format::ALL
            .into_iter()
            .find(|f| f.dir_name() == s)
            .ok_or_else(|| anyhow!("unknown format `{s}` (expected dbt, sqlmesh or freshet)"))
    }
}

/// The `freshet` CLI layout: one `<name>.sql` per model plus `sources.json`, flat.
///
/// Model SQL is written verbatim — this is the generator's own dialect.
pub fn freshet_files(g: &GeneratedProject) -> Result<Files> {
    check_names(g)?;
    let mut files = Files::new();
    for (name, sql) in &g.models {
        files.insert(PathBuf::from(format!("{name}.sql")), format!("{sql}\n"));
    }
    let sources = serde_json::to_string_pretty(&g.sources).context("serializing sources.json")?;
    files.insert(PathBuf::from("sources.json"), format!("{sources}\n"));
    Ok(files)
}

/// A dbt-duckdb project: `dbt_project.yml`, `profiles.yml`, `models/sources.yml` and
/// one `models/<name>.sql` per model with `{{ ref() }}` / `{{ source() }}` in place of
/// bare names.
///
/// Materialization is set once in `dbt_project.yml` rather than by a `config()` call
/// per file, so the models carry no Jinja beyond the references — the shape dbt's
/// static parser is built for, and the cheapest one to hand it.
pub fn dbt_files(g: &GeneratedProject) -> Result<Files> {
    check_names(g)?;
    let models: BTreeSet<&str> = g.models.iter().map(|(n, _)| n.as_str()).collect();
    let rename = |name: &str| {
        if models.contains(name) {
            Some(format!("{{{{ ref('{name}') }}}}"))
        } else if g.sources.contains_key(name) {
            Some(format!("{{{{ source('{SCHEMA}', '{name}') }}}}"))
        } else {
            None
        }
    };

    let mut files = Files::new();
    files.insert(PathBuf::from("dbt_project.yml"), dbt_project_yml());
    files.insert(PathBuf::from("profiles.yml"), dbt_profiles_yml());
    files.insert(PathBuf::from("models/sources.yml"), dbt_sources_yml(&g.sources));
    for (name, sql) in &g.models {
        let body = rewrite_relations(sql, rename).with_context(|| format!("exporting `{name}` to dbt"))?;
        files.insert(PathBuf::from(format!("models/{name}.sql")), format!("{body}\n"));
    }
    Ok(files)
}

/// A SQLMesh project on a local DuckDB gateway: `config.yaml`, `external_models.yaml`
/// for the sources, and one `models/<name>.sql` per model with a
/// `MODEL (name bench.<name>, kind FULL);` header selecting from `bench.<parent>`.
///
/// `kind FULL` is SQLMesh's analogue of a dbt `table`: rebuilt whole, no intervals to
/// reason about, so neither tool gets a cheaper or dearer model kind than the other.
pub fn sqlmesh_files(g: &GeneratedProject) -> Result<Files> {
    check_names(g)?;
    let models: BTreeSet<&str> = g.models.iter().map(|(n, _)| n.as_str()).collect();
    let rename = |name: &str| {
        (models.contains(name) || g.sources.contains_key(name)).then(|| format!("{SCHEMA}.{name}"))
    };

    let mut files = Files::new();
    files.insert(PathBuf::from("config.yaml"), sqlmesh_config_yaml());
    files.insert(PathBuf::from("external_models.yaml"), sqlmesh_external_models_yaml(&g.sources));
    for (name, sql) in &g.models {
        let body = rewrite_relations(sql, rename).with_context(|| format!("exporting `{name}` to SQLMesh"))?;
        files.insert(
            PathBuf::from(format!("models/{name}.sql")),
            format!("MODEL (name {SCHEMA}.{name}, kind FULL);\n\n{body}\n"),
        );
    }
    Ok(files)
}

/// Write `files` under `dir`, which must be absent or empty.
///
/// Refusing a non-empty directory is the point, not a nicety: the exporter never
/// deletes, so exporting a 1k-model project over an earlier 10k-model one would leave
/// 9k stale model files that every tool would then parse as part of the project — and
/// the comparison would time a project nobody asked for.
pub fn write_files(dir: &Path, files: &Files) -> Result<()> {
    if dir.exists() {
        let mut entries = std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))?;
        ensure!(
            entries.next().is_none(),
            "refusing to export into non-empty directory {} (stale model files would join the project)",
            dir.display()
        );
    }
    for (rel, contents) in files {
        let path = dir.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&path, contents).with_context(|| format!("writing {}", path.display()))?;
    }
    Ok(())
}

/// Return a copy of `g` in which `model` carries one semantic edit: its query gains
/// `where <first output column> is not null`.
///
/// The edit is chosen so all three tools must agree on what it costs. It changes the
/// rows, so it is a real change to each of them — a new file checksum for dbt's
/// `state:modified`, a breaking change for SQLMesh's categorizer, a new AST
/// fingerprint for `freshet plan` (a cosmetic edit would be invisible to the last one,
/// by design). It changes neither the output columns nor the dependencies, so the set
/// that must rebuild is `model` plus its transitive dependents for every tool
/// ([`downstream_closure`]) and the plans can be checked against each other by size.
///
/// Applied to the [`GeneratedProject`] *before* export, so the three formats carry
/// byte-identical edits.
pub fn edit_model(g: &GeneratedProject, model: &str) -> Result<GeneratedProject> {
    let mut edited = g.clone();
    let (_, sql) = edited
        .models
        .iter_mut()
        .find(|(name, _)| name == model)
        .with_context(|| format!("no model named `{model}` to edit"))?;

    let before = freshet_sql::analyze(sql, &BTreeSet::new(), &Catalog::new())
        .with_context(|| format!("analyzing `{model}` before the edit"))?;
    let first = before.columns.first().with_context(|| format!("`{model}` has no output columns"))?;
    let candidate = format!("{sql} where {} is not null", first.name);

    // A query that already ends in WHERE / ORDER BY / LIMIT cannot take the suffix;
    // say so rather than export SQL that no tool can parse.
    let after = freshet_sql::analyze(&candidate, &BTreeSet::new(), &Catalog::new())
        .with_context(|| format!("`{model}` cannot take a trailing WHERE; the edit needs a plain select"))?;
    ensure!(
        before.columns.iter().map(|c| &c.name).eq(after.columns.iter().map(|c| &c.name)),
        "the edit changed `{model}`'s output columns, so tools could legitimately disagree on its blast radius"
    );
    ensure!(
        relations(sql)? == relations(&candidate)?,
        "the edit changed `{model}`'s dependencies"
    );
    *sql = candidate;
    Ok(edited)
}

/// `model` plus every model that transitively depends on it: what a correct planner
/// must rebuild after a change to `model`, at model granularity.
///
/// Column-level pruning cannot shrink this set on a generated project, because every
/// generated model selects all of its parent's columns; that is what makes it a fair
/// yardstick for dbt's model-level `state:modified+`, SQLMesh's indirect modifications
/// and Freshet's column-aware plan alike.
pub fn downstream_closure(project: &Project, model: &str) -> BTreeSet<String> {
    let mut seen = BTreeSet::from([model.to_string()]);
    let mut queue = VecDeque::from([model.to_string()]);
    while let Some(next) = queue.pop_front() {
        for child in project.downstream_of(&next) {
            if seen.insert(child.clone()) {
                queue.push_back(child.clone());
            }
        }
    }
    seen
}

/// Every single-part relation name `sql` references, in the order the parser visits
/// them (FROM / JOIN table factors; not CTE definitions, not column qualifiers).
pub fn relations(sql: &str) -> Result<Vec<String>> {
    let statements = parse(sql)?;
    let mut found = Vec::new();
    let _ = visit_relations(&statements, |name| {
        if let [ident] = name.0.as_slice() {
            if ident.quote_style.is_none() {
                found.push(ident.value.clone());
            }
        }
        ControlFlow::<()>::Continue(())
    });
    Ok(found)
}

/// Replace every relation reference in `sql` that `rename` maps, copying every other
/// byte — whitespace, comments, casing, literals — through untouched.
///
/// Only unquoted single-part names are candidates, and never a token that is one part
/// of a dotted name (`schema.table`, `t.col`). The parser decides which names are
/// relations; the tokenizer says where they are. The two must agree on the count, or
/// this fails: a disagreement means some non-relation token (a column, alias, CTE or
/// function) shares a relation's name, and rewriting it would change the query.
pub fn rewrite_relations(sql: &str, rename: impl Fn(&str) -> Option<String>) -> Result<String> {
    let expected = relations(sql)?.iter().filter(|name| rename(name).is_some()).count();

    let tokens = Tokenizer::new(&GenericDialect {}, sql)
        .tokenize_with_location()
        .map_err(|e| anyhow!("SQL tokenize error: {e}"))?;
    let starts = token_byte_offsets(sql, &tokens)?;

    let mut out = String::with_capacity(sql.len() + 24 * expected);
    let mut rewritten = 0usize;
    for (i, t) in tokens.iter().enumerate() {
        let end = starts.get(i + 1).copied().unwrap_or(sql.len());
        let replacement = match &t.token {
            Token::Word(w) if w.quote_style.is_none() && !is_dotted(&tokens, i) => rename(&w.value),
            _ => None,
        };
        match replacement {
            Some(r) => {
                out.push_str(&r);
                rewritten += 1;
            }
            None => out.push_str(&sql[starts[i]..end]),
        }
    }
    ensure!(
        rewritten == expected,
        "ambiguous relation names: the parser sees {expected} relation reference(s) to rewrite but \
         {rewritten} token(s) match — a column, alias, CTE or function shares a relation's name, \
         and rewriting it would change the query"
    );
    Ok(out)
}

fn parse(sql: &str) -> Result<Vec<sqlparser::ast::Statement>> {
    Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| anyhow!("SQL parse error: {e}"))
}

/// Byte offset at which each token starts.
///
/// sqlparser 0.52 reports a token's start as a 1-based (line, column) with columns
/// counted in chars, not bytes, and carries no spans. So the tokenizer's own
/// accounting is replayed: `\n` starts a new line, every other char — `\r` included —
/// advances the column by one.
fn token_byte_offsets(sql: &str, tokens: &[TokenWithLocation]) -> Result<Vec<usize>> {
    let mut starts = Vec::with_capacity(tokens.len());
    let mut chars = sql.char_indices().peekable();
    let (mut line, mut column) = (1u64, 1u64);
    for t in tokens {
        while (line, column) != (t.location.line, t.location.column) {
            let Some((_, ch)) = chars.next() else {
                bail!("token {:?} is located past the end of the SQL", t.token);
            };
            if ch == '\n' {
                line += 1;
                column = 1;
            } else {
                column += 1;
            }
        }
        starts.push(chars.peek().map_or(sql.len(), |&(at, _)| at));
    }
    Ok(starts)
}

/// Whether token `i` is one part of a dotted name, looking past whitespace and
/// comments the way the parser does.
fn is_dotted(tokens: &[TokenWithLocation], i: usize) -> bool {
    let significant = |t: &&TokenWithLocation| !matches!(t.token, Token::Whitespace(_));
    let before = tokens[..i].iter().rev().find(significant);
    let after = tokens[i + 1..].iter().find(significant);
    [before, after].into_iter().flatten().any(|t| t.token == Token::Period)
}

/// Names are spliced into YAML, Jinja and SQL unquoted, so every model, source and
/// column name must be a plain identifier, models must be unique, and no model may
/// share a source's name (the planner would read that reference as the model; dbt
/// would be told it is a source).
fn check_names(g: &GeneratedProject) -> Result<()> {
    fn plain(s: &str) -> bool {
        let mut chars = s.chars();
        matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }
    let mut seen = BTreeSet::new();
    for (name, _) in &g.models {
        ensure!(plain(name), "model name `{name}` is not a plain identifier");
        ensure!(seen.insert(name.as_str()), "model `{name}` is defined twice");
        ensure!(!g.sources.contains_key(name), "`{name}` is both a model and a source");
    }
    for (source, cols) in &g.sources {
        ensure!(plain(source), "source name `{source}` is not a plain identifier");
        for col in cols {
            ensure!(plain(col), "column `{source}.{col}` is not a plain identifier");
        }
    }
    Ok(())
}

const GENERATED: &str =
    "# Generated by freshet-bench-export (crates/freshet-bench); see scripts/compare.sh.\n";

fn dbt_project_yml() -> String {
    format!(
        "{GENERATED}\
name: {DBT_PROJECT}
version: \"1.0.0\"
config-version: 2
profile: {DBT_PROJECT}
model-paths: [\"models\"]
flags:
  # A usage-stats request at exit would put network latency inside a timed run.
  send_anonymous_usage_stats: false
models:
  {DBT_PROJECT}:
    +materialized: table
"
    )
}

/// No `threads`: dbt-core ignores it while parsing, and dbt v2 would take it as its
/// worker count. The profile says nothing a tool does not need, so no tool runs with a
/// setting it would not have by default.
fn dbt_profiles_yml() -> String {
    format!(
        "{GENERATED}\
{DBT_PROJECT}:
  target: bench
  outputs:
    bench:
      type: duckdb
      # Relative to the directory dbt runs in; compare.sh runs it from the project root.
      path: {DUCKDB_FILE}
      schema: {SCHEMA}
"
    )
}

fn dbt_sources_yml(sources: &Catalog) -> String {
    let mut out = format!("{GENERATED}version: 2\n\nsources:\n  - name: {SCHEMA}\n    tables:\n");
    for (table, cols) in sources {
        out.push_str(&format!("      - name: {table}\n        columns:\n"));
        for col in cols {
            out.push_str(&format!("          - name: {col}\n"));
        }
    }
    out
}

fn sqlmesh_config_yaml() -> String {
    format!(
        "{GENERATED}\
gateways:
  bench:
    connection:
      type: duckdb
      # Relative to the directory sqlmesh runs in; compare.sh runs it from the project root.
      database: {DUCKDB_FILE}
default_gateway: bench
model_defaults:
  dialect: duckdb
# SQLMesh flushes an analytics event at exit by default; that request would be timed.
disable_anonymized_analytics: true
"
    )
}

/// SQLMesh requires a type per external column; the generator has none, so every
/// column is declared `int`. Nothing is read from the table while planning, so the
/// type changes no timed code path — it only has to be *a* type.
fn sqlmesh_external_models_yaml(sources: &Catalog) -> String {
    let mut out = GENERATED.to_string();
    for (table, cols) in sources {
        out.push_str(&format!("- name: {SCHEMA}.{table}\n  columns:\n"));
        for col in cols {
            out.push_str(&format!("    {col}: int\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> GenSpec {
        GenSpec { layers: 6, width: 5, cols: 3, seed: 7 }
    }

    /// `(child, parent)` model edges per freshet-sql — an independent reading of the
    /// generated SQL, so the exporters are checked against the planner, not themselves.
    fn planner_edges(g: &GeneratedProject) -> BTreeSet<(String, String)> {
        let project = freshet_sql::build_project(&g.models, &g.sources).unwrap();
        g.models
            .iter()
            .flat_map(|(name, _)| {
                let deps = project.get(name).unwrap().depends_on.clone();
                deps.into_iter().map(move |d| (name.clone(), d))
            })
            .collect()
    }

    /// Extract `(kind, args)` for every `{{ kind(args) }}` call in a dbt model.
    fn jinja_calls(sql: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let mut rest = sql;
        while let Some(open) = rest.find("{{") {
            let close = rest[open..].find("}}").expect("unclosed jinja") + open;
            let call = rest[open + 2..close].trim();
            let paren = call.find('(').expect("call");
            out.push((call[..paren].to_string(), call[paren + 1..call.len() - 1].to_string()));
            rest = &rest[close + 2..];
        }
        out
    }

    fn model_files(files: &Files, dir: &str) -> BTreeMap<String, String> {
        files
            .iter()
            .filter(|(p, _)| p.extension().is_some_and(|e| e == "sql"))
            .filter(|(p, _)| p.parent().is_some_and(|d| d == Path::new(dir)))
            .map(|(p, s)| (p.file_stem().unwrap().to_str().unwrap().to_string(), s.clone()))
            .collect()
    }

    #[test]
    fn every_model_is_exported_exactly_once_in_every_format() {
        let g = generate(&spec());
        let expected: BTreeSet<String> = g.models.iter().map(|(n, _)| n.clone()).collect();
        assert_eq!(expected.len(), spec().model_count());
        for (format, dir) in [(Format::Dbt, "models"), (Format::Sqlmesh, "models"), (Format::Freshet, "")] {
            let files = format.render(&g).unwrap();
            let exported = model_files(&files, dir);
            // A map keyed by name cannot hold a duplicate, so also count the raw files.
            let sql_files = files.keys().filter(|p| p.extension().is_some_and(|e| e == "sql")).count();
            assert_eq!(sql_files, expected.len(), "{format}: one .sql per model, no extras");
            assert_eq!(exported.keys().cloned().collect::<BTreeSet<_>>(), expected, "{format}");
        }
    }

    #[test]
    fn dbt_refs_form_the_generated_edge_set() {
        let g = generate(&spec());
        let files = dbt_files(&g).unwrap();
        let mut refs = BTreeSet::new();
        let mut sourced = BTreeSet::new();
        for (name, sql) in model_files(&files, "models") {
            for (kind, args) in jinja_calls(&sql) {
                match kind.as_str() {
                    "ref" => {
                        refs.insert((name.clone(), args.trim_matches('\'').to_string()));
                    }
                    "source" => {
                        assert_eq!(args, format!("'{SCHEMA}', 'src'"));
                        sourced.insert(name.clone());
                    }
                    other => panic!("unexpected jinja call `{other}` in {name}"),
                }
            }
        }
        assert_eq!(refs, planner_edges(&g), "dbt refs must be exactly the planner's edge set");
        // Every model reads exactly one relation, so a model with no ref reads the source.
        let layer0: BTreeSet<String> = (0..spec().width).map(|i| format!("m_0_{i}")).collect();
        assert_eq!(sourced, layer0);
    }

    #[test]
    fn undoing_the_dbt_rewrite_gives_back_the_generated_sql_byte_for_byte() {
        let g = generate(&spec());
        let files = dbt_files(&g).unwrap();
        let exported = model_files(&files, "models");
        for (name, sql) in &g.models {
            let mut body = exported[name].clone();
            for (kind, args) in jinja_calls(&exported[name]) {
                let bare = args.rsplit('\'').nth(1).unwrap().to_string();
                body = body.replacen(&format!("{{{{ {kind}({args}) }}}}"), &bare, 1);
            }
            assert_eq!(body, format!("{sql}\n"), "{name}");
        }
    }

    #[test]
    fn sqlmesh_models_carry_their_header_and_bench_qualified_parents() {
        let g = generate(&spec());
        let files = sqlmesh_files(&g).unwrap();
        let exported = model_files(&files, "models");
        let mut edges = BTreeSet::new();
        for (name, sql) in &g.models {
            let file = &exported[name];
            let header = format!("MODEL (name {SCHEMA}.{name}, kind FULL);\n\n");
            let body = file.strip_prefix(&header).unwrap_or_else(|| panic!("{name}: missing header"));
            let unqualified_relations = relations(body).unwrap();
            assert!(unqualified_relations.is_empty(), "{name}: bare relations {unqualified_relations:?}");
            let unqualified = body.replace(&format!("{SCHEMA}."), "");
            assert_eq!(unqualified, format!("{sql}\n"), "{name}");
            for parent in relations(sql).unwrap().into_iter().filter(|r| r != "src") {
                edges.insert((name.clone(), parent));
            }
        }
        assert_eq!(edges, planner_edges(&g));
        assert!(files[Path::new("external_models.yaml")].contains(&format!("- name: {SCHEMA}.src\n")));
        assert!(files[Path::new("config.yaml")].contains("disable_anonymized_analytics: true"));
    }

    #[test]
    fn the_freshet_dir_is_the_generated_project_verbatim() {
        let g = generate(&spec());
        let files = freshet_files(&g).unwrap();
        let exported = model_files(&files, "");
        for (name, sql) in &g.models {
            assert_eq!(exported[name], format!("{sql}\n"));
        }
        let sources: Catalog = serde_json::from_str(&files[Path::new("sources.json")]).unwrap();
        assert_eq!(sources, g.sources);
        // Round trip through the planner: same models, same edges.
        let reloaded: Vec<(String, String)> = exported.into_iter().collect();
        let back = GeneratedProject { models: reloaded, sources };
        assert_eq!(planner_edges(&back), planner_edges(&g));
    }

    #[test]
    fn the_rewrite_touches_relations_only() {
        let rename = |n: &str| matches!(n, "src" | "m_0").then(|| format!("<{n}>"));
        let sql = "select a.c0, m_0.c1 -- from src\nfrom src a join m_0 on a.c0 = m_0.c0 where 'src' <> 'm_0'";
        assert_eq!(
            rewrite_relations(sql, rename).unwrap(),
            "select a.c0, m_0.c1 -- from src\nfrom <src> a join <m_0> on a.c0 = m_0.c0 where 'src' <> 'm_0'"
        );
        // Multi-byte text before a relation: offsets are bytes, locations are chars.
        let sql = "select 'né' as c0 from src";
        assert_eq!(rewrite_relations(sql, rename).unwrap(), "select 'né' as c0 from <src>");
        // A dotted name is someone else's relation; left alone.
        assert_eq!(rewrite_relations("select c0 from other.src", rename).unwrap(), "select c0 from other.src");
    }

    #[test]
    fn the_rewrite_refuses_a_name_shared_with_a_non_relation() {
        let rename = |n: &str| (n == "src").then(|| "<src>".to_string());
        for ambiguous in [
            "select src from src",                                       // column
            "select c0 from m_0 as src",                                 // alias
            "with src as (select 1 as c0) select c0 from src",           // CTE definition
            "select c0 from other where c0 in (select src(c0) from x)",  // function
        ] {
            let err = rewrite_relations(ambiguous, rename).unwrap_err().to_string();
            assert!(err.contains("ambiguous"), "{ambiguous}: {err}");
        }
    }

    #[test]
    fn the_writer_refuses_a_non_empty_directory() {
        let dir = std::env::temp_dir().join(format!("freshet-bench-export-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let g = generate(&spec());
        for format in Format::ALL {
            let target = dir.join(format.dir_name());
            write_files(&target, &format.render(&g).unwrap()).unwrap();
            let err = write_files(&target, &format.render(&g).unwrap()).unwrap_err().to_string();
            assert!(err.contains("non-empty"), "{err}");
        }
        for required in [
            "dbt/dbt_project.yml",
            "dbt/profiles.yml",
            "dbt/models/sources.yml",
            "dbt/models/m_5_4.sql",
            "sqlmesh/config.yaml",
            "sqlmesh/external_models.yaml",
            "sqlmesh/models/m_0_0.sql",
            "freshet/sources.json",
            "freshet/m_3_2.sql",
        ] {
            assert!(dir.join(required).is_file(), "{required} missing");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_edit_is_a_real_change_that_keeps_columns_and_edges() {
        let g = generate(&spec());
        let edited = edit_model(&g, "m_0_0").unwrap();
        let before = freshet_sql::build_project(&g.models, &g.sources).unwrap();
        let after = freshet_sql::build_project(&edited.models, &edited.sources).unwrap();

        let fp = freshet_fingerprint::AstFingerprinter::new();
        let (fp_before, fp_after) = (fp.fingerprint_project(&before), fp.fingerprint_project(&after));
        let changed: Vec<&String> = fp_before.keys().filter(|k| fp_before[*k] != fp_after[*k]).collect();
        assert_eq!(changed, ["m_0_0"], "the edit must be visible to freshet plan, and only on the edited model");

        assert_eq!(planner_edges(&g), planner_edges(&edited));
        assert_eq!(before.get("m_0_0").unwrap().output_columns(), after.get("m_0_0").unwrap().output_columns());

        // The edit survives export identically: the dbt and SQLMesh bodies differ from
        // the unedited export by exactly the same suffix.
        for format in [Format::Dbt, Format::Sqlmesh, Format::Freshet] {
            let dir = if format == Format::Freshet { "" } else { "models" };
            let a = &model_files(&format.render(&g).unwrap(), dir)["m_0_0"];
            let b = &model_files(&format.render(&edited).unwrap(), dir)["m_0_0"];
            assert_eq!(b, &format!("{} where c0 is not null\n", a.trim_end()), "{format}");
        }
        assert!(edit_model(&g, "nope").is_err());
    }

    #[test]
    fn the_downstream_closure_is_the_model_and_its_transitive_dependents() {
        let g = generate(&spec());
        let project = freshet_sql::build_project(&g.models, &g.sources).unwrap();
        for (name, _) in &g.models {
            let closure = downstream_closure(&project, name);
            assert!(closure.contains(name));
            for member in &closure {
                // Every member other than the root has a parent inside the closure.
                if member != name {
                    let deps = &project.get(member).unwrap().depends_on;
                    assert!(deps.iter().any(|d| closure.contains(d)), "{member} is not downstream of {name}");
                }
            }
            // And nothing outside it depends on anything inside it.
            for (other, _) in &g.models {
                if !closure.contains(other) {
                    let deps = &project.get(other).unwrap().depends_on;
                    assert!(deps.iter().all(|d| !closure.contains(d)), "{other} escaped the closure of {name}");
                }
            }
        }
    }

    #[test]
    fn export_writes_every_format_and_reports_the_edit() {
        let dir = std::env::temp_dir().join(format!("freshet-bench-export-run-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        // A failed edit writes nothing at all.
        assert!(export(&spec(), &Format::ALL, Some("m_9_9"), &dir).is_err());
        assert!(!dir.exists(), "a failed export must leave no partial project");

        let summary = export(&spec(), &Format::ALL, Some("m_0_0"), &dir).unwrap();
        assert_eq!(summary.models, spec().model_count());
        // Every model past layer 0 reads exactly one parent model.
        assert_eq!(summary.edges, spec().model_count() - spec().width);
        let edit = summary.edit.unwrap();
        assert_eq!(edit.model, "m_0_0");
        let g = generate(&spec());
        let project = freshet_sql::build_project(&g.models, &g.sources).unwrap();
        assert_eq!(edit.closure, downstream_closure(&project, "m_0_0").len());
        for format in Format::ALL {
            assert!(summary.dirs[format.dir_name()].is_dir(), "{format}");
        }
        let edited = std::fs::read_to_string(dir.join("freshet/m_0_0.sql")).unwrap();
        assert!(edited.ends_with(" where c0 is not null\n"), "{edited}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn both_tools_share_one_warehouse_file_whose_catalog_cannot_shadow_the_schema() {
        let g = generate(&spec());
        let profiles = &dbt_files(&g).unwrap()[Path::new("profiles.yml")];
        let config = &sqlmesh_files(&g).unwrap()[Path::new("config.yaml")];
        assert!(profiles.contains(&format!("path: {DUCKDB_FILE}\n")), "{profiles}");
        assert!(config.contains(&format!("database: {DUCKDB_FILE}\n")), "{config}");
        assert_ne!(Path::new(DUCKDB_FILE).file_stem().unwrap(), SCHEMA, "catalog `bench` + schema `bench` is ambiguous");
    }

    #[test]
    fn formats_parse_from_their_directory_names() {
        for f in Format::ALL {
            assert_eq!(f.dir_name().parse::<Format>().unwrap(), f);
        }
        assert!("all".parse::<Format>().is_err(), "`all` is the binary's concern, not a format");
    }
}
