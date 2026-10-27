//! The differential test that gates incremental reparse (ROADMAP §M4, and item 2 of the
//! testing strategy): random projects, many random edits each, and after every reload
//! the daemon's project must be exactly what a from-scratch `build_project_with_hints`
//! makes of the same input. "Exactly" is a canonical dump of every field of every
//! model, because `Model` has no `PartialEq`.
//!
//! The edits are the ones that move lineage — projections changed, columns renamed,
//! added and dropped, `select *`, a FROM re-pointed at another model, models added and
//! removed, sources changed — plus the ones that must not cost a reparse
//! (materializations, the incremental config, whitespace), the ones that must fail
//! (unparseable SQL, cycles), and CTEs named like the model they read (the dbt import
//! CTE, an ordinary edge) or like a model they do not read (a shadow, which the
//! engine cannot tell from a read and answers with a full rebuild).
//!
//! The run ends by asserting it exercised the incremental path, a real downstream
//! closure, and each kind of fallback. A differential test that quietly always fell
//! back to the full build would pass while proving nothing.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use freshet_core::{Model, Project};
use freshet_embed::build_project_with_hints;
use freshet_incremental::IncrementalConfig;
use freshet_sql::Catalog;
use serde_json::json;

use crate::{parse_materializations, ProjectCache, Request, Response};

/// Projects generated, each from its own seed; a failure names the seed and step.
const SEEDS: u64 = 64;
/// Reloads per project, the first of which is the initial load.
const STEPS: usize = 40;
/// Columns `c0`..`c4`. Sources hold some of them, so references resolve sometimes.
const COLUMNS: usize = 5;

#[test]
fn an_incremental_reload_is_byte_identical_to_a_full_rebuild_across_random_edits() {
    let mut tally = Tally::default();
    for seed in 0..SEEDS {
        let mut rng = Rng(seed);
        let mut world = World::new(&mut rng);
        let cache = ProjectCache::new();
        let mut accepted = Accepted::empty();
        let mut loaded = false;
        for step in 0..STEPS {
            let mut edits = Vec::new();
            if step > 0 {
                edits.push(world.edit(&mut rng));
                if rng.percent(30) {
                    edits.push(world.edit(&mut rng));
                }
            }
            let context = Context { seed, step, edits };
            loaded |= check(&cache, &world, &mut accepted, &context, &mut tally);
        }
        tally.projects_loaded += usize::from(loaded);
    }
    eprintln!("{tally:#?}");
    tally.assert_exercised();
}

/// One reload, checked against a from-scratch build of the same input. Returns whether
/// the reload succeeded.
fn check(
    cache: &ProjectCache,
    world: &World,
    accepted: &mut Accepted,
    ctx: &Context,
    tally: &mut Tally,
) -> bool {
    let models: Vec<(String, String)> = world.models.iter().map(|m| (m.name(), m.sql())).collect();
    let hints =
        parse_materializations(world.hints.clone()).expect("the generator writes valid materializations");
    let expected = build_project_with_hints(&models, &world.sources, &hints);
    let incremental: IncrementalConfig =
        serde_json::from_value(world.incremental.clone()).expect("the generator writes valid configs");
    let before = cache.revision();
    let response = cache.handle(Request::Reload {
        models: models.clone(),
        sources: world.sources.clone(),
        materializations: world.hints.clone(),
        incremental,
    });
    let context = || ctx.describe(world, &models);

    match (expected, response) {
        (
            Ok(full),
            Response::Reloaded { revision, models: count, reparsed, full_rebuild, full_rebuild_reason },
        ) => {
            let want = dump(&full);
            assert_eq!(dump(&project(cache)), want, "incremental project != full rebuild\n{}", context());
            assert_eq!(count, full.len(), "{}", context());

            // The revision moves exactly when what plans read moved.
            let changed = want != accepted.dump || world.incremental != accepted.incremental;
            assert_eq!(revision, before + u64::from(changed), "revision\n{}", context());

            let edited = models
                .iter()
                .filter(|(name, sql)| accepted.project.get(name).is_none_or(|m| m.sql != *sql))
                .count();
            if full_rebuild {
                assert_eq!(reparsed, count, "a full rebuild analyzes every model\n{}", context());
                let reason = full_rebuild_reason.unwrap_or_else(|| panic!("no reason\n{}", context()));
                *tally.full.entry(reason).or_default() += 1;
            } else {
                assert_eq!(full_rebuild_reason, None, "{}", context());
                // Every edited model is re-analyzed, and nothing is when nothing was.
                assert!(reparsed >= edited && reparsed <= count, "reparsed {reparsed}\n{}", context());
                assert_eq!(reparsed == 0, edited == 0, "reparsed {reparsed}\n{}", context());
                match reparsed {
                    0 => tally.untouched += 1,
                    n if n > edited => tally.through_closure += 1,
                    _ => tally.edited_only += 1,
                }
            }
            *accepted = Accepted { dump: want, incremental: world.incremental.clone(), project: full };
            true
        }
        (Err(e), Response::Error { message }) => {
            assert_eq!(message, format!("build_project: {e}"), "{}", context());
            assert_eq!(cache.revision(), before, "a failed reload keeps the revision\n{}", context());
            let kept = dump(&project(cache));
            assert_eq!(kept, accepted.dump, "a failed reload keeps the project\n{}", context());
            tally.rejected += 1;
            false
        }
        (expected, response) => panic!(
            "the full build said {:?}, the daemon said {response:?}\n{}",
            expected.map(|p| p.len()),
            context()
        ),
    }
}

/// Every field of every model, and the reverse edges, one line each so a failure diffs.
///
/// Destructures `Model` exhaustively: a field added upstream stops this compiling
/// rather than going unchecked.
fn dump(project: &Project) -> String {
    let mut out = String::new();
    for name in project.names() {
        let Model { name, sql, materialization, columns, depends_on } =
            project.get(name).expect("names() yields models");
        let downstream: Vec<&String> = project.downstream_of(name).collect();
        writeln!(out, "{name} {materialization:?} {sql:?}").unwrap();
        writeln!(out, "  depends_on {depends_on:?} downstream {downstream:?}").unwrap();
        for column in columns {
            let upstream: Vec<String> =
                column.upstream.iter().map(|r| format!("{}.{}", r.model, r.column)).collect();
            writeln!(out, "  {} <- {upstream:?}", column.name).unwrap();
        }
    }
    out
}

fn project(cache: &ProjectCache) -> std::sync::Arc<Project> {
    cache.inner.read().unwrap().project.clone()
}

/// The last reload the daemon accepted: what a revision bump is measured against, and
/// what "edited" means for the next one.
struct Accepted {
    dump: String,
    incremental: serde_json::Value,
    project: Project,
}

impl Accepted {
    /// A new cache: no models, the default config.
    fn empty() -> Self {
        Self { dump: String::new(), incremental: empty_config(), project: Project::default() }
    }
}

struct Context {
    seed: u64,
    step: usize,
    edits: Vec<&'static str>,
}

impl Context {
    fn describe(&self, world: &World, models: &[(String, String)]) -> String {
        let mut out = format!("seed {} step {} after {:?}\n", self.seed, self.step, self.edits);
        writeln!(out, "sources {:?}", world.sources).unwrap();
        writeln!(out, "hints {:?}", world.hints).unwrap();
        for (name, sql) in models {
            writeln!(out, "  {name}: {sql}").unwrap();
        }
        out
    }
}

/// What the run exercised.
#[derive(Debug, Default)]
struct Tally {
    /// Incremental reloads that analyzed the edited models and nothing else.
    edited_only: usize,
    /// Incremental reloads that also analyzed downstream models of an edit.
    through_closure: usize,
    /// Incremental reloads with no SQL edit, so nothing analyzed at all.
    untouched: usize,
    /// Full rebuilds, by reason.
    full: BTreeMap<String, usize>,
    /// Reloads the full build rejected, and the daemon with it.
    rejected: usize,
    /// Projects with at least one successful reload.
    projects_loaded: usize,
}

impl Tally {
    fn assert_exercised(&self) {
        let reloads = SEEDS as usize * STEPS;
        let incremental = self.edited_only + self.through_closure + self.untouched;
        assert!(incremental * 3 >= reloads, "the incremental path ran too rarely: {self:#?}");
        assert!(self.through_closure >= 100, "too few edits reached a downstream model: {self:#?}");
        assert!(self.untouched >= 50, "too few reloads without an SQL edit: {self:#?}");
        assert!(self.rejected >= 20, "too few rejected reloads: {self:#?}");
        let reason = |needle: &str| -> usize {
            self.full.iter().filter(|(r, _)| r.contains(needle)).map(|(_, n)| n).sum()
        };
        // Only the first *successful* reload has nothing to reparse against; a failed
        // one leaves no build behind, and every later one has one.
        assert_eq!(reason("first load"), self.projects_loaded, "one first load per project: {self:#?}");
        assert!(reason("model names") >= 20, "too few model-set fallbacks: {self:#?}");
        assert!(reason("sources") >= 20, "too few sources fallbacks: {self:#?}");
        assert!(reason("outside its depends_on") >= 10, "too few shadowing-CTE fallbacks: {self:#?}");
    }
}

// ---------------------------------------------------------------------------
// The generator
// ---------------------------------------------------------------------------

/// SplitMix64: a few lines, no dependency, and a failing seed reproduces by number.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn percent(&mut self, p: u64) -> bool {
        self.next() % 100 < p
    }
}

/// One projection item.
#[derive(Clone, Copy, Debug)]
enum Item {
    /// `*`
    Star,
    /// `t1.*`
    FromStar,
    /// `[t1.]c<col>[ + 1][ as a<alias>]`
    Column { qualified: bool, col: usize, plus_one: bool, alias: Option<usize> },
    /// `t1.c<col> + t2.c<col> as s<alias>`, or `t1.c<col> as s<alias>` with no join.
    Sum { col: usize, alias: usize },
    /// `1 as k<alias>`: an output column with no upstream at all.
    Constant { alias: usize },
}

impl Item {
    fn random(rng: &mut Rng) -> Self {
        match rng.below(12) {
            0 => Item::Star,
            1 => Item::FromStar,
            2 | 3 => Item::Sum { col: rng.below(COLUMNS), alias: rng.below(3) },
            4 => Item::Constant { alias: rng.below(2) },
            _ => Item::Column {
                qualified: rng.percent(50),
                col: rng.below(COLUMNS),
                plus_one: rng.percent(25),
                alias: rng.percent(25).then(|| rng.below(3)),
            },
        }
    }

    /// A small change: for a column, one of its expression (output name kept), its
    /// alias (output renamed), its qualification, or its source column.
    fn tweaked(self, rng: &mut Rng) -> Self {
        match self {
            Item::Column { qualified, col, plus_one, alias } => match rng.below(4) {
                0 => Item::Column { qualified, col, plus_one: !plus_one, alias },
                1 => {
                    let alias = match alias {
                        Some(_) => None,
                        None => Some(rng.below(3)),
                    };
                    Item::Column { qualified, col, plus_one, alias }
                }
                2 => Item::Column { qualified: !qualified, col, plus_one, alias },
                _ => Item::Column { qualified, col: rng.below(COLUMNS), plus_one, alias },
            },
            _ => Item::random(rng),
        }
    }
}

#[derive(Clone, Debug)]
struct GenModel {
    /// The model is `m<id>`. Ids only grow, so a model reading lower ids is acyclic.
    id: usize,
    /// Read as `t1`.
    from: String,
    /// Read as `t2`, joined on `c0`.
    join: Option<String>,
    items: Vec<Item>,
    /// `with <from> as (…) select … from <from>`: a CTE named like the relation the
    /// model reads. See [`Cte`].
    import_cte: Option<Cte>,
    /// `where t1.c1 > 0`: a row filter, which moves no lineage.
    filter: bool,
    /// Trailing spaces: an edit to the text and nothing else.
    pad: usize,
    /// Rendered as SQL that does not parse.
    broken: bool,
}

/// A top-level CTE that takes the name of the model's `from`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cte {
    /// `with <from> as (select * from <from>)`: the dbt import CTE — its body reads the
    /// real `<from>`, an ordinary edge.
    Star,
    /// `with <from> as (select c0, c1 from <from>)`: the same, naming columns.
    Cols,
    /// `with <from> as (select c0, c1 from raw_a)`: hides `<from>` without reading it.
    /// The engine's text scan still sees `<from>` and cannot tell the two apart, so a
    /// reload that could reach it falls back to the full build.
    Shadow,
}

impl Cte {
    fn random(rng: &mut Rng) -> Cte {
        match rng.below(5) {
            0 => Cte::Star,
            1 | 2 => Cte::Cols,
            _ => Cte::Shadow,
        }
    }
}

impl GenModel {
    fn name(&self) -> String {
        format!("m{}", self.id)
    }

    fn sql(&self) -> String {
        let mut sql = String::new();
        match self.import_cte {
            Some(Cte::Star) => write!(sql, "with {from} as (select * from {from}) ", from = self.from),
            Some(Cte::Cols) => write!(sql, "with {from} as (select c0, c1 from {from}) ", from = self.from),
            Some(Cte::Shadow) => write!(sql, "with {from} as (select c0, c1 from raw_a) ", from = self.from),
            None => Ok(()),
        }
        .unwrap();
        sql.push_str(if self.broken { "selec " } else { "select " });
        let items: Vec<String> = self.items.iter().map(|item| self.render(*item)).collect();
        sql.push_str(&items.join(", "));
        write!(sql, " from {} as t1", self.from).unwrap();
        if let Some(join) = &self.join {
            write!(sql, " join {join} as t2 on t1.c0 = t2.c0").unwrap();
        }
        if self.filter {
            sql.push_str(" where t1.c1 > 0");
        }
        sql.push_str(&" ".repeat(self.pad));
        sql
    }

    fn render(&self, item: Item) -> String {
        match item {
            Item::Star => "*".to_string(),
            Item::FromStar => "t1.*".to_string(),
            Item::Column { qualified, col, plus_one, alias } => {
                let mut expr = format!("{}c{col}", if qualified { "t1." } else { "" });
                if plus_one {
                    expr.push_str(" + 1");
                }
                if let Some(alias) = alias {
                    write!(expr, " as a{alias}").unwrap();
                }
                expr
            }
            Item::Sum { col, alias } if self.join.is_some() => format!("t1.c{col} + t2.c{col} as s{alias}"),
            Item::Sum { col, alias } => format!("t1.c{col} as s{alias}"),
            Item::Constant { alias } => format!("1 as k{alias}"),
        }
    }
}

/// A project as the generator edits it; rendered to a reload each step.
struct World {
    models: Vec<GenModel>,
    sources: Catalog,
    hints: BTreeMap<String, String>,
    incremental: serde_json::Value,
    next_id: usize,
}

fn columns(indices: &[usize]) -> Vec<String> {
    indices.iter().map(|c| format!("c{c}")).collect()
}

fn empty_config() -> serde_json::Value {
    json!({ "models": {} })
}

impl World {
    fn new(rng: &mut Rng) -> Self {
        let mut sources = Catalog::new();
        sources.insert("raw_a".to_string(), columns(&[0, 1, 2]));
        sources.insert("raw_b".to_string(), columns(&[0, 2, 3, 4]));
        if rng.percent(30) {
            // A source named like a model: `build_project` shows it to the models it
            // analyzes before `m1`, and `m1`'s own columns to the rest.
            sources.insert("m1".to_string(), columns(&[0, 4]));
        }
        let mut world = World {
            models: Vec::new(),
            sources,
            hints: BTreeMap::new(),
            incremental: empty_config(),
            next_id: 0,
        };
        for _ in 0..3 + rng.below(8) {
            world.add_model(rng);
        }
        for _ in 0..rng.below(3) {
            world.edit_hint(rng);
        }
        world
    }

    /// A table to read: a source, a model (only below `below` when given, which keeps
    /// the graph acyclic), or a table that is neither.
    fn relation(&self, rng: &mut Rng, below: Option<usize>) -> String {
        let models: Vec<String> =
            self.models.iter().filter(|m| below.is_none_or(|b| m.id < b)).map(GenModel::name).collect();
        let roll = rng.below(20);
        if roll < 5 || models.is_empty() {
            // Not a source that is currently also a model name: that is a model reference.
            let sources: Vec<&String> =
                self.sources.keys().filter(|s| !self.models.iter().any(|m| m.name() == **s)).collect();
            sources[rng.below(sources.len())].clone()
        } else if roll == 5 {
            GHOST.to_string()
        } else {
            models[rng.below(models.len())].clone()
        }
    }

    fn add_model(&mut self, rng: &mut Rng) {
        let id = self.next_id;
        self.next_id += 1;
        let from = self.relation(rng, Some(id));
        let join = rng.percent(25).then(|| self.relation(rng, Some(id)));
        let items = (0..1 + rng.below(3)).map(|_| Item::random(rng)).collect();
        let import_cte = rng.percent(15).then(|| Cte::random(rng));
        let filter = rng.percent(20);
        self.models.push(GenModel { id, from, join, items, import_cte, filter, pad: 0, broken: false });
    }

    /// Apply one random edit and name it.
    fn edit(&mut self, rng: &mut Rng) -> &'static str {
        if self.models.is_empty() {
            self.add_model(rng);
            return "add a model";
        }
        let i = rng.below(self.models.len());
        match rng.below(100) {
            0..=17 => {
                let model = &mut self.models[i];
                let k = rng.below(model.items.len());
                model.items[k] = model.items[k].tweaked(rng);
                "change a projection"
            }
            18..=25 => {
                self.models[i].items.push(Item::random(rng));
                "add a column"
            }
            26..=31 => {
                let model = &mut self.models[i];
                if model.items.len() > 1 {
                    model.items.remove(rng.below(model.items.len()));
                }
                "drop a column"
            }
            32..=36 => {
                let model = &mut self.models[i];
                if rng.percent(50) {
                    model.items = vec![Item::Star];
                } else {
                    model.items.push(Item::FromStar);
                }
                "select *"
            }
            37..=45 => {
                // Any model at all, so this can close a cycle.
                self.models[i].from = self.relation(rng, None);
                "re-point a from"
            }
            46..=50 => {
                let join = match self.models[i].join {
                    Some(_) => None,
                    None => Some(self.relation(rng, None)),
                };
                self.models[i].join = join;
                "toggle a join"
            }
            51..=54 => {
                let model = &mut self.models[i];
                model.import_cte = match model.import_cte {
                    Some(_) => None,
                    None => Some(Cte::random(rng)),
                };
                "toggle an import cte"
            }
            55..=58 => {
                let model = &mut self.models[i];
                if rng.percent(50) {
                    model.filter = !model.filter;
                } else {
                    model.pad += 1;
                }
                "edit that moves no lineage"
            }
            59..=62 => {
                self.add_model(rng);
                "add a model"
            }
            63..=65 => {
                self.models.remove(i);
                "remove a model"
            }
            66..=71 => {
                self.edit_sources(rng);
                "change the sources"
            }
            72..=76 => {
                self.edit_hint(rng);
                "change a materialization"
            }
            77..=80 => {
                self.edit_incremental(rng);
                "change the incremental config"
            }
            81..=82 => {
                self.models[i].broken = true;
                "break a model"
            }
            83..=92 => {
                self.repair();
                "repair"
            }
            _ => "nothing",
        }
    }

    fn edit_sources(&mut self, rng: &mut Rng) {
        match rng.below(3) {
            0 => {
                // Toggle a column of raw_b, never c0 (every join is on it).
                let col = format!("c{}", 1 + rng.below(COLUMNS - 1));
                let cols = self.sources.get_mut("raw_b").expect("raw_b is never removed");
                match cols.iter().position(|c| *c == col) {
                    Some(p) => {
                        cols.remove(p);
                    }
                    None => cols.push(col),
                }
            }
            1 => {
                if self.sources.remove("m1").is_none() {
                    self.sources.insert("m1".to_string(), columns(&[0, 4]));
                }
            }
            _ => {
                // A source nothing may read: the full rebuild runs, but a project that
                // comes out identical keeps its revision.
                if self.sources.remove("extra").is_none() {
                    self.sources.insert("extra".to_string(), columns(&[0]));
                }
            }
        }
    }

    fn edit_hint(&mut self, rng: &mut Rng) {
        // Sometimes a model that does not exist, which the build ignores.
        let name = if self.models.is_empty() || rng.percent(15) {
            "m99".to_string()
        } else {
            self.models[rng.below(self.models.len())].name()
        };
        match rng.below(5) {
            0 => {
                self.hints.remove(&name);
            }
            k => {
                self.hints.insert(name, ["table", "view", "incremental", "VIEW"][k - 1].to_string());
            }
        }
    }

    fn edit_incremental(&mut self, rng: &mut Rng) {
        self.incremental = if self.incremental == empty_config() && !self.models.is_empty() {
            let model = self.models[rng.below(self.models.len())].name();
            json!({ "models": { model: { "edges": { "raw_a": { "window": 1 + rng.below(3) } } } } })
        } else {
            empty_config()
        };
    }

    /// Undo every edit that can fail a build — unparseable SQL, an edge to a model at or
    /// above one's own id (the only way to a cycle), a read of a table nobody declares,
    /// and an import CTE's `select *` — so a run that wandered into errors gets back to
    /// reloads that succeed.
    fn repair(&mut self) {
        let ids: BTreeMap<String, usize> = self.models.iter().map(|m| (m.name(), m.id)).collect();
        for model in &mut self.models {
            let own = model.id;
            let risky = |relation: &str| {
                relation == GHOST || ids.get(relation).is_some_and(|&id| id >= own)
            };
            model.broken = false;
            if risky(model.from.as_str()) {
                model.from = "raw_a".to_string();
            }
            if model.join.as_deref().is_some_and(risky) {
                model.join = None;
            }
            if model.import_cte == Some(Cte::Star) {
                model.import_cte = Some(Cte::Cols);
            }
        }
    }
}

/// A table that is neither a model nor a source: its columns are unknown, so a `*` over
/// it fails the build and a column read from it has no provable lineage.
const GHOST: &str = "ghost";
