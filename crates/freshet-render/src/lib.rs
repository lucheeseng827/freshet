//! freshet-render — turn a plan's write intent into the SQL a warehouse runs.
//!
//! ## The gap this closes
//!
//! Until this crate, Freshet had never produced a line of executable SQL. A plan
//! said *which* models and partitions rebuild, and
//! [`freshet_incremental::replace`] said *what the rebuild does to the relation*
//! (`insert_overwrite` two partitions, `merge` on a key, …) — as semantics, with
//! "the adapter that renders one into a statement" left as roadmap. This is that
//! adapter: a resolved [`ReplaceOp`] (or, for an undeclared model, its
//! [`Materialization`]) goes in, dialect-specific SQL text comes out.
//!
//! ## Text out, nothing executed
//!
//! The planner never runs SQL, and neither does this crate: it has no driver, no
//! connection and no credentials. It emits statements; an executor runs them —
//! dagron's generic `dagron-step-sql`, Airflow's `SQLExecuteQueryOperator`, `psql`.
//! That split is what lets one renderer serve every orchestrator, and it is why the
//! output is a list of statements with an [`RenderedSql::atomic`] flag rather than
//! a function that "does the write".
//!
//! ## One renderer, parameterised by dialect
//!
//! Not one connector per warehouse. The strategies are the same everywhere; what
//! varies is how each [`Dialect`] spells an atomic replace — a data-modifying CTE
//! on Postgres, a transaction on Snowflake and Redshift, `MERGE … ON FALSE` on
//! BigQuery, `REPLACE WHERE` on Databricks. Adding a dialect is adding match arms, which is the
//! same bet dagron's `dagron-step-sql` made (and Airflow made, when it deprecated its
//! per-warehouse operators for one generic SQL operator).
//!
//! ## The rules
//!
//! 1. **Never narrower than the op.** A partition-scoped write filters *both* sides
//!    by exactly the op's keys: the rows it deletes from the target and the rows it
//!    inserts from the model. Filtering only the delete duplicates rows outside the
//!    partitions; filtering only the insert leaves stale ones inside them.
//! 2. **Named, not positional, inserts** wherever the statement form allows a column
//!    list. `INSERT INTO t SELECT …` matches columns by position, so a model whose
//!    columns were reordered writes values into the wrong columns without an error
//!    whenever the types line up. Where a form has no column list (Databricks'
//!    `REPLACE WHERE`), the crate says so — see [`Dialect::Databricks`].
//! 3. **Refuse rather than guess.** A partition-scoped rebuild of an incremental model
//!    with no declared strategy is an error, not a silent full rewrite and not a
//!    guessed `merge`; so is a partition key this crate cannot spell safely, or an
//!    unnamed output column where a column list is needed.
//! 4. **Atomic where the dialect allows it**, and flagged where it does not.
//! 5. **Recreate exactly when the definition changed.** A model whose own SQL changed
//!    (or that the warehouse has never received) is created from its SQL, because its
//!    columns may have changed and only recreating keeps the relation's shape equal to
//!    the model's — a row-preserving write after a column was *removed* would leave
//!    that column behind, silently NULL. Every other rebuild (an upstream changed, or
//!    a restatement) keeps the relation and replaces rows, so its partitioning,
//!    clustering, grants and dependent views survive. The caller says which models
//!    were redefined ([`RenderOptions::recreate`]); a fingerprint diff knows exactly.
//! 6. **Postgres (and Redshift) bind views to tables.** Recreating a relation there drops and
//!    recreates the views that depend on it *and that the plan also rebuilds* — never
//!    `CASCADE`, so a dependent the plan does not rebuild (a hand-made BI view) makes
//!    the rebuild fail loudly instead of vanishing. [`render_plan`] computes them;
//!    [`render_model`] has no plan to look at, and renders none.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use freshet_core::{BackfillPlan, BackfillUnit, Materialization, Model, Project};
use freshet_incremental::replace::resolve_plan;
use freshet_incremental::{
    IncrementalConfig, PartitionType, ReplaceOp, ReplaceStrategy, ReplaceTarget,
};

/// The alias every partition-scoped statement gives the model's query.
const MODEL_ALIAS: &str = "freshet_model";
/// The alias a `MERGE` gives its source.
const SOURCE_ALIAS: &str = "freshet_src";
/// The alias a `MERGE` gives its target.
const TARGET_ALIAS: &str = "freshet_tgt";

// ---------------------------------------------------------------------------
// Dialects
// ---------------------------------------------------------------------------

/// A SQL dialect this crate renders for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Dialect {
    /// PostgreSQL 15+ (`MERGE` arrived in 15). Every row-level write is **one**
    /// statement — a data-modifying CTE deletes and inserts atomically — which
    /// matters because single-statement executors (dagron's `dagron-step-sql` on the
    /// Postgres wire) can run it. Recreating a relation is the exception: it needs
    /// `DROP` + `CREATE` inside `BEGIN`/`COMMIT` (Postgres has no
    /// `CREATE OR REPLACE TABLE`), and — because Postgres binds a view to its
    /// table — drops the dependent views the plan rebuilds first (rule 6).
    Postgres,
    /// Snowflake. Partition replaces are a `BEGIN TRANSACTION` … `COMMIT` pair
    /// (Snowflake has no single-statement partition replace); whole-table writes
    /// are single statements (`INSERT OVERWRITE`, `CREATE OR REPLACE … COPY GRANTS`).
    Snowflake,
    /// BigQuery. Partition replaces use `MERGE … ON FALSE` with a
    /// `NOT MATCHED BY SOURCE` delete — atomic, one statement, and pruned by the
    /// partition predicate. `delete_insert` is honoured literally as a
    /// multi-statement transaction.
    BigQuery,
    /// Databricks (Delta). Partition replaces use `INSERT INTO … REPLACE WHERE`,
    /// which is atomic but takes no column list, so it inserts **by position**. That
    /// is sound here because every whole-relation write on Databricks is a
    /// `CREATE OR REPLACE TABLE … AS` from the model's SQL — the only whole-table
    /// replace whose meaning does not depend on the session's
    /// `partitionOverwriteMode` — and a model is recreated whenever its definition
    /// changes (rule 5), so the table's column order is always the model's.
    /// `delete_insert` is two statements and **not atomic** (Delta has no
    /// multi-statement transactions); the output says so.
    Databricks,
    /// Amazon Redshift. Postgres on the wire, not in the grammar: no data-modifying
    /// CTEs, and its `MERGE` takes only a table as its source. So every row-level
    /// write is `BEGIN` / `DELETE` / `INSERT` (named columns) / `COMMIT` — atomic,
    /// because Redshift runs DDL and DML transactionally — and a `merge` is the keyed
    /// delete-then-insert AWS documents as Redshift's upsert. Never `TRUNCATE`, which
    /// commits implicitly there and would break the transaction. Views bind to their
    /// tables as on Postgres (unless created `WITH NO SCHEMA BINDING`), so recreating
    /// drops the dependent views the plan rebuilds (rule 6). A `merge` source with
    /// duplicate keys inserts duplicates where `MERGE` would have failed.
    Redshift,
}

impl Dialect {
    /// Every dialect, in a stable order — for tests and `--help`.
    pub const ALL: [Dialect; 5] = [
        Dialect::Postgres,
        Dialect::Snowflake,
        Dialect::BigQuery,
        Dialect::Databricks,
        Dialect::Redshift,
    ];

    /// The spelling used on the wire and on the command line.
    pub fn as_str(self) -> &'static str {
        match self {
            Dialect::Postgres => "postgres",
            Dialect::Snowflake => "snowflake",
            Dialect::BigQuery => "bigquery",
            Dialect::Databricks => "databricks",
            Dialect::Redshift => "redshift",
        }
    }

    /// Whether the dialect binds a view to the relation it reads, so recreating the
    /// relation must drop the view first (rule 6).
    fn binds_views(self) -> bool {
        matches!(self, Dialect::Postgres | Dialect::Redshift)
    }

    /// The character this dialect quotes identifiers with.
    fn quote(self) -> char {
        match self {
            Dialect::Postgres | Dialect::Snowflake | Dialect::Redshift => '"',
            Dialect::BigQuery | Dialect::Databricks => '`',
        }
    }
}

impl fmt::Display for Dialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A dialect name this crate does not render for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownDialect(pub String);

impl fmt::Display for UnknownDialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let known: Vec<&str> = Dialect::ALL.iter().map(|d| d.as_str()).collect();
        write!(
            f,
            "unknown SQL dialect `{}` (expected one of: {})",
            self.0,
            known.join(", ")
        )
    }
}

impl std::error::Error for UnknownDialect {}

impl FromStr for Dialect {
    type Err = UnknownDialect;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "postgres" | "postgresql" => Ok(Dialect::Postgres),
            "snowflake" => Ok(Dialect::Snowflake),
            "bigquery" => Ok(Dialect::BigQuery),
            "databricks" => Ok(Dialect::Databricks),
            "redshift" => Ok(Dialect::Redshift),
            _ => Err(UnknownDialect(s.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// Inputs and outputs
// ---------------------------------------------------------------------------

/// How to render.
#[derive(Clone, Debug)]
pub struct RenderOptions {
    pub dialect: Dialect,
    /// Qualifies every target relation: `analytics` renders `events` as
    /// `analytics.events`; `proj.analytics` as `proj.analytics.events`. `None`
    /// leaves names bare, resolved by the session's search path / default dataset —
    /// the same way the models' own SQL resolves the upstreams it reads.
    pub schema: Option<String>,
    /// Models whose relation is (re)created from their SQL instead of written into:
    /// models the warehouse has never received from this planner, and models whose
    /// own definition changed. See rule 5 in the crate docs.
    ///
    /// A code-change plan knows this set exactly — it is the fingerprint diff, the
    /// models the plan marks directly changed — so a caller derives it from state
    /// rather than asking the warehouse. A **restatement** passes an empty set: its
    /// seed model is "directly changed" in the plan's vocabulary, but its definition
    /// did not move, and recreating it would throw away every partition it was not
    /// asked to touch.
    ///
    /// Listing a model that did not change is safe (it is recreated, which is correct
    /// if slower), and is the escape hatch for a relation someone altered by hand.
    pub recreate: BTreeSet<String>,
}

impl RenderOptions {
    pub fn new(dialect: Dialect) -> Self {
        Self {
            dialect,
            schema: None,
            recreate: BTreeSet::new(),
        }
    }
}

/// The SQL for one model's rebuild.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedSql {
    pub dialect: Dialect,
    /// Run **in order, on one session**. None carries a trailing `;` — separators
    /// are the executor's business; [`Self::script`] adds them for a script runner.
    pub statements: Vec<String>,
    /// Whether the statements are all-or-nothing when run as above: a single
    /// statement, or a sequence wrapped in its own `BEGIN`/`COMMIT`. `false` means a
    /// failure part-way can leave the target half-written, and a caller showing a
    /// human anything should show that.
    pub atomic: bool,
}

impl RenderedSql {
    /// The statements as one script, each terminated with `;`.
    ///
    /// Safe to terminate naively: a statement whose last line holds a `--` comment
    /// already ends in a newline (see [`model_body`]), so the `;` can never be
    /// swallowed by the comment.
    pub fn script(&self) -> String {
        self.statements
            .iter()
            .map(|s| format!("{s};"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Why a model could not be rendered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RenderError {
    /// A partition-scoped rebuild of an incremental model that declares no replace
    /// strategy. Warehouses disagree on the default, so none is guessed; and
    /// rewriting the whole table instead of the partitions asked for is not a
    /// fallback anyone should discover from their bill.
    UndeclaredPartitionWrite { model: String, partitions: usize },
    /// The statement needs a column list and this output column has no real name —
    /// an unaliased expression, which lineage names `_colN` and the warehouse names
    /// something else entirely.
    UnnamedColumn { model: String, column: String },
    /// The statement needs a column list and lineage found no output columns.
    NoColumns { model: String },
    /// A `partition_column` / `unique_key` that is not one of the model's output
    /// columns — almost always a typo in the config, and cheaper to catch here than
    /// as a warehouse error at 3am.
    UnknownColumn {
        model: String,
        column: String,
        role: &'static str,
    },
    /// A partition key with characters this crate will not put inside a string
    /// literal. Warehouses disagree about escapes (`''` is an escaped quote in
    /// Postgres and two adjacent literals in Spark), so rather than escape per
    /// dialect and be wrong in one, keys are restricted to characters no dialect
    /// treats specially.
    UnsafePartitionKey { model: String, key: String },
    /// A key that does not parse as its declared [`PartitionType`].
    BadPartitionKey {
        model: String,
        key: String,
        expected: PartitionType,
    },
    /// An identifier holding a quote character, a backslash or a control character.
    UnsafeIdentifier { model: String, identifier: String },
    /// The model's SQL is empty once trailing semicolons and whitespace are removed.
    EmptySql { model: String },
    /// A replace op this crate cannot have been handed by
    /// [`freshet_incremental::replace::resolve`] — e.g. a partition strategy over the
    /// whole relation. Refused rather than reinterpreted.
    InconsistentOp { model: String, detail: String },
    /// The plan names a model the project does not have.
    UnknownModel { model: String },
    /// A move of some partitions of a model that declares no `partition_column`:
    /// there is nothing to scope the copy by, and copying the whole relation instead
    /// is not what was asked.
    UndeclaredPartitionColumn { model: String, partitions: usize },
}

impl fmt::Display for RenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UndeclaredPartitionWrite { model, partitions } => write!(
                f,
                "`{model}`: the plan rebuilds {partitions} partition(s) of this incremental model, but it \
                 declares no replace strategy — add one to incremental.json (e.g. \
                 {{\"replace\": {{\"strategy\": \"insert_overwrite\", \"partition_column\": \"dt\"}}}}); \
                 Freshet will not guess one or silently rewrite the whole table"
            ),
            Self::UnnamedColumn { model, column } => write!(
                f,
                "`{model}`: output column `{column}` is an unaliased expression; alias it \
                 (`… AS name`) so the write can name its columns"
            ),
            Self::NoColumns { model } => {
                write!(f, "`{model}`: no output columns were derived, so the write cannot name its columns")
            }
            Self::UnknownColumn { model, column, role } => write!(
                f,
                "`{model}`: {role} `{column}` is not one of the model's output columns"
            ),
            Self::UnsafePartitionKey { model, key } => write!(
                f,
                "`{model}`: partition key {key:?} contains characters Freshet will not put in a SQL \
                 literal (allowed: letters, digits, space and - _ : . / +)"
            ),
            Self::BadPartitionKey { model, key, expected } => {
                write!(f, "`{model}`: partition key {key:?} is not a valid {expected:?}")
            }
            Self::UnsafeIdentifier { model, identifier } => write!(
                f,
                "`{model}`: identifier {identifier:?} contains a quote, backslash or control \
                 character and cannot be quoted safely in every dialect"
            ),
            Self::EmptySql { model } => write!(f, "`{model}`: the model's SQL is empty"),
            Self::InconsistentOp { model, detail } => write!(f, "`{model}`: {detail}"),
            Self::UnknownModel { model } => write!(f, "`{model}`: the plan names a model the project does not have"),
            Self::UndeclaredPartitionColumn { model, partitions } => write!(
                f,
                "`{model}`: the plan moves {partitions} partition(s) of this model, but it declares no \
                 partition_column to scope the copy by — add one to incremental.json"
            ),
        }
    }
}

impl std::error::Error for RenderError {}

/// Every model that failed, so one run reports them all rather than the first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderErrors(pub Vec<RenderError>);

impl fmt::Display for RenderErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} model(s) could not be rendered to SQL:", self.0.len())?;
        for e in &self.0 {
            write!(f, "\n  - {e}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RenderErrors {}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

/// Render every model in `plan`, in plan order, resolving each model's replace op
/// against `cfg` exactly as the wire contract does.
///
/// All-or-nothing: executing part of a plan runs downstream models over upstreams
/// that were never rebuilt, so a caller gets either SQL for every model or every
/// reason it could not have it.
///
/// Prefer this over [`render_model`]: only a plan knows which Postgres views to drop
/// around a recreated relation (rule 6), and only the config knows a model's declared
/// partitioning once resolution has widened its op to a full refresh.
pub fn render_plan(
    project: &Project,
    plan: &BackfillPlan,
    cfg: &IncrementalConfig,
    opts: &RenderOptions,
) -> Result<BTreeMap<String, RenderedSql>, RenderErrors> {
    let ops = resolve_plan(project, cfg, plan);
    let mut out = BTreeMap::new();
    let mut errors = Vec::new();
    for name in &plan.models {
        let Some(model) = project.get(name) else {
            errors.push(RenderError::UnknownModel {
                model: name.clone(),
            });
            continue;
        };
        // An absent unit reads as "everything", never as "no partitions" — the
        // same rule `resolve_plan` applies, for the same reason.
        let unit = plan
            .unit
            .get(name)
            .cloned()
            .unwrap_or(BackfillUnit::FullModel);
        let ctx = Context {
            declared: cfg
                .models
                .get(name)
                .and_then(|m| m.replace.as_ref())
                .and_then(|spec| {
                    spec.partition_column
                        .clone()
                        .map(|c| (c, spec.partition_type.unwrap_or_default()))
                }),
            dependent_views: if opts.dialect.binds_views() {
                dependent_views_in_plan(project, plan, name)
            } else {
                Vec::new()
            },
        };
        match render_with(model, &unit, ops.get(name), &ctx, opts) {
            Ok(sql) => {
                out.insert(name.clone(), sql);
            }
            Err(e) => errors.push(e),
        }
    }
    if errors.is_empty() {
        Ok(out)
    } else {
        Err(RenderErrors(errors))
    }
}

/// Render one model's rebuild, with no plan around it.
///
/// `op` is the model's resolved replace op, if it declared one; `unit` is what the
/// plan asked for. For a declared model the op is authoritative (it was resolved
/// *against* `unit`); for an undeclared one the materialization decides.
///
/// Without a plan there are no dependent views to manage (rule 6) and no declared
/// partitioning to carry into a recreate beyond what `op` still holds — see
/// [`render_plan`].
pub fn render_model(
    model: &Model,
    unit: &BackfillUnit,
    op: Option<&ReplaceOp>,
    opts: &RenderOptions,
) -> Result<RenderedSql, RenderError> {
    let ctx = Context {
        declared: op.and_then(|o| {
            o.partition_column
                .clone()
                .map(|c| (c, o.partition_type.unwrap_or_default()))
        }),
        dependent_views: Vec::new(),
    };
    render_with(model, unit, op, &ctx, opts)
}

/// What rendering a model needs to know beyond the model itself.
#[derive(Default)]
struct Context {
    /// The declared partition column and its type, used to keep a recreated relation
    /// partitioned where the dialect can say so in `CREATE … AS`. Taken from the
    /// declaration, not the resolved op, because a full refresh widens the op and
    /// widening clears the column — but the table is still partitioned by it.
    declared: Option<(String, PartitionType)>,
    /// Postgres only: views the plan also rebuilds that depend on this model, to be
    /// dropped before it is recreated. Unrendered model names, dependents first.
    dependent_views: Vec<String>,
}

fn render_with(
    model: &Model,
    unit: &BackfillUnit,
    op: Option<&ReplaceOp>,
    ctx: &Context,
    opts: &RenderOptions,
) -> Result<RenderedSql, RenderError> {
    let write = decide(model, unit, op, opts)?;
    let body = model_body(model)?;
    let dependent_views = ctx
        .dependent_views
        .iter()
        .map(|v| {
            qualified(opts, v).ok_or_else(|| RenderError::UnsafeIdentifier {
                model: model.name.clone(),
                identifier: v.clone(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let r = Renderer {
        d: opts.dialect,
        model,
        body,
        target: relation(model, opts)?,
        ctx,
        dependent_views,
    };
    let (statements, atomic) = match write {
        Write::CreateTable => r.create_table()?,
        Write::CreateView { recreate } => r.create_view(recreate),
        Write::ReplaceAll => r.replace_all()?,
        Write::ReplacePartitions {
            strategy,
            column,
            keys,
            ptype,
        } => r.replace_partitions(strategy, column, keys, ptype)?,
        Write::Merge { unique_key, scope } => r.merge(unique_key, scope)?,
    };
    Ok(RenderedSql {
        dialect: opts.dialect,
        statements,
        atomic,
    })
}

/// Views in `plan` that depend on `model` directly or through other views, in an
/// order safe to drop in one statement (dependents before what they depend on).
///
/// Only views, because only a view binds to its upstream in Postgres — a table made
/// by `CREATE TABLE … AS` holds copied rows and no reference. Only views the plan
/// rebuilds, because those come back later in the same run; anything else that
/// depends on the relation is left in place so the `DROP` fails and says so.
fn dependent_views_in_plan(project: &Project, plan: &BackfillPlan, model: &str) -> Vec<String> {
    let in_plan: BTreeSet<&str> = plan.models.iter().map(String::as_str).collect();
    let mut found: BTreeSet<String> = BTreeSet::new();
    let mut frontier = vec![model.to_string()];
    while let Some(current) = frontier.pop() {
        for down in project.downstream_of(&current) {
            let is_view = project
                .get(down)
                .is_some_and(|m| m.materialization == Materialization::View);
            if is_view && in_plan.contains(down.as_str()) && found.insert(down.clone()) {
                frontier.push(down.clone());
            }
        }
    }
    // Plan order is topological; reversed, every view precedes what it selects from.
    plan.models
        .iter()
        .rev()
        .filter(|m| found.contains(*m))
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// What to write
// ---------------------------------------------------------------------------

/// The write a model's rebuild performs, independent of dialect.
#[derive(Debug)]
enum Write<'a> {
    /// (Re)create the table from the model's SQL.
    CreateTable,
    /// (Re)create the view; `recreate` when its definition changed.
    CreateView { recreate: bool },
    /// Replace every row and keep the relation.
    ReplaceAll,
    /// Replace the rows of these partitions.
    ReplacePartitions {
        strategy: ReplaceStrategy,
        column: &'a str,
        keys: &'a [String],
        ptype: PartitionType,
    },
    /// Upsert on `unique_key`, sourcing only the scoped partitions when a partition
    /// column is declared and the whole model output otherwise.
    Merge {
        unique_key: &'a [String],
        scope: Option<(&'a str, &'a [String], PartitionType)>,
    },
}

fn decide<'a>(
    model: &'a Model,
    unit: &'a BackfillUnit,
    op: Option<&'a ReplaceOp>,
    opts: &RenderOptions,
) -> Result<Write<'a>, RenderError> {
    let recreate = opts.recreate.contains(&model.name);
    match model.materialization {
        Materialization::View => return Ok(Write::CreateView { recreate }),
        // Rule 5. A table's own definition is unchanged unless it is in `recreate`,
        // so its columns are too, and its rows can be replaced in place. Resolution
        // widens any partition-wise declaration on a table to a full refresh, so the
        // op has nothing to add either way.
        Materialization::Table => {
            // A table whose columns cannot all be named cannot take a named insert;
            // recreating is its natural form anyway, so that is the fallback here —
            // unlike an incremental relation, which is never recreated behind the
            // operator's back.
            let nameable = !model.columns.is_empty()
                && !model.columns.iter().any(|c| is_synthetic_column(&c.name));
            return Ok(if recreate || !nameable {
                Write::CreateTable
            } else {
                Write::ReplaceAll
            });
        }
        Materialization::Incremental => {}
    }
    if recreate {
        // New, or redefined: nothing to preserve that still has the right shape, and
        // a row-level write into a table that does not exist fails.
        return Ok(Write::CreateTable);
    }

    let inconsistent = |detail: String| RenderError::InconsistentOp {
        model: model.name.clone(),
        detail,
    };

    let Some(op) = op else {
        return match unit {
            // A whole-model rebuild needs no strategy: replacing every row is the
            // one answer every warehouse agrees on.
            BackfillUnit::FullModel => Ok(Write::ReplaceAll),
            // Empty is contradictory ("rebuild this model, no partitions of it");
            // widen like `resolve` does rather than render a write of nothing.
            BackfillUnit::Partitions(ps) if ps.is_empty() => Ok(Write::ReplaceAll),
            BackfillUnit::Partitions(ps) => Err(RenderError::UndeclaredPartitionWrite {
                model: model.name.clone(),
                partitions: ps.len(),
            }),
        };
    };

    let ptype = op.partition_type.unwrap_or_default();
    match (op.strategy, &op.target) {
        (ReplaceStrategy::FullRefresh, _) => Ok(Write::ReplaceAll),
        (ReplaceStrategy::Merge, ReplaceTarget::Partitions(keys)) => {
            if op.unique_key.is_empty() {
                return Err(inconsistent(
                    "a merge with no unique_key (resolution widens these)".into(),
                ));
            }
            Ok(Write::Merge {
                unique_key: &op.unique_key,
                scope: op
                    .partition_column
                    .as_deref()
                    .map(|c| (c, keys.as_slice(), ptype)),
            })
        }
        (
            s @ (ReplaceStrategy::InsertOverwrite
            | ReplaceStrategy::DeleteInsert
            | ReplaceStrategy::ReplaceWhere),
            ReplaceTarget::Partitions(keys),
        ) => {
            let Some(column) = op.partition_column.as_deref() else {
                return Err(inconsistent(format!(
                    "{s:?} with no partition_column (resolution widens these)"
                )));
            };
            if keys.is_empty() {
                return Err(inconsistent(format!(
                    "{s:?} over zero partitions (resolution widens these)"
                )));
            }
            Ok(Write::ReplacePartitions {
                strategy: s,
                column,
                keys,
                ptype,
            })
        }
        (s, ReplaceTarget::Whole) => Err(inconsistent(format!(
            "{s:?} over the whole relation (resolution widens these to a full refresh)"
        ))),
    }
}

// ---------------------------------------------------------------------------
// How to spell it
// ---------------------------------------------------------------------------

struct Renderer<'a> {
    d: Dialect,
    model: &'a Model,
    /// The model's SQL, trimmed and safe to embed (see [`model_body`]).
    body: String,
    /// The rendered, qualified target relation.
    target: String,
    ctx: &'a Context,
    /// `ctx.dependent_views`, rendered and qualified.
    dependent_views: Vec<String>,
}

impl Renderer<'_> {
    fn create_table(&self) -> Result<(Vec<String>, bool), RenderError> {
        let (t, body) = (&self.target, &self.body);
        let partitioning = self.partitioning_ddl()?;
        Ok(match self.d {
            // No CREATE OR REPLACE TABLE in Postgres, and DDL is transactional, so the
            // swap is atomic. Views bind to the table's OID here, so the ones the plan
            // rebuilds are dropped first and come back in their own step (rule 6).
            // Never CASCADE: a dependent the plan does not rebuild must stop this
            // rather than disappear with it.
            Dialect::Postgres | Dialect::Redshift => {
                let mut stmts = vec!["BEGIN".to_string()];
                if !self.dependent_views.is_empty() {
                    stmts.push(format!(
                        "DROP VIEW IF EXISTS {}",
                        self.dependent_views.join(", ")
                    ));
                }
                stmts.push(format!("DROP TABLE IF EXISTS {t}"));
                stmts.push(format!("CREATE TABLE {t} AS\n{body}"));
                stmts.push("COMMIT".into());
                (stmts, true)
            }
            // COPY GRANTS: a rebuilt table should not silently lose who could read it.
            Dialect::Snowflake => (
                vec![format!(
                    "CREATE OR REPLACE TABLE {t} COPY GRANTS AS\n{body}"
                )],
                true,
            ),
            Dialect::BigQuery | Dialect::Databricks => (
                vec![format!(
                    "CREATE OR REPLACE TABLE {t}{partitioning} AS\n{body}"
                )],
                true,
            ),
        })
    }

    /// Keep a recreated relation partitioned, where `CREATE … AS` can say so.
    ///
    /// A partition-scoped write into a table that lost its partitioning is still
    /// correct — it just scans everything, forever, which on BigQuery is a bill and
    /// not an error. So when the model declares a partition column, recreating
    /// carries it: `PARTITION BY` on BigQuery (whose partition columns must be dates,
    /// so only for a `date`-typed one), `PARTITIONED BY` on Databricks. Postgres
    /// cannot partition from `CREATE TABLE … AS`, and Snowflake has no partitions to
    /// declare.
    fn partitioning_ddl(&self) -> Result<String, RenderError> {
        let Some((column, ptype)) = &self.ctx.declared else {
            return Ok(String::new());
        };
        if self.model.materialization != Materialization::Incremental {
            return Ok(String::new());
        }
        self.require_output_column(column, "partition_column")?;
        let col = self.ident(column)?;
        Ok(match (self.d, ptype) {
            (Dialect::BigQuery, PartitionType::Date) => format!("\nPARTITION BY {col}"),
            (Dialect::Databricks, _) => format!("\nPARTITIONED BY ({col})"),
            _ => String::new(),
        })
    }

    fn create_view(&self, recreate: bool) -> (Vec<String>, bool) {
        let (t, body) = (&self.target, &self.body);
        let stmts = match self.d {
            // `CREATE OR REPLACE VIEW` cannot drop or rename a column in Postgres, and a
            // redefined view may have done either — so it is dropped and created, its
            // dependents in the plan first (rule 6).
            Dialect::Postgres | Dialect::Redshift if recreate => {
                let mut drop = self.dependent_views.clone();
                drop.push(t.clone());
                vec![
                    "BEGIN".into(),
                    format!("DROP VIEW IF EXISTS {}", drop.join(", ")),
                    format!("CREATE VIEW {t} AS\n{body}"),
                    "COMMIT".into(),
                ]
            }
            Dialect::Snowflake => {
                vec![format!("CREATE OR REPLACE VIEW {t} COPY GRANTS AS\n{body}")]
            }
            _ => vec![format!("CREATE OR REPLACE VIEW {t} AS\n{body}")],
        };
        (stmts, true)
    }

    /// Replace every row of a relation, keeping the relation.
    fn replace_all(&self) -> Result<(Vec<String>, bool), RenderError> {
        let t = &self.target;
        let stmts = match self.d {
            Dialect::Postgres => {
                let cols = self.column_list()?;
                vec![format!(
                    "WITH freshet_deleted AS (\n  DELETE FROM {t}\n)\nINSERT INTO {t} ({cols})\n{}",
                    self.select(&cols, None)
                )]
            }
            Dialect::Snowflake => {
                let cols = self.column_list()?;
                vec![format!(
                    "INSERT OVERWRITE INTO {t} ({cols})\n{}",
                    self.select(&cols, None)
                )]
            }
            Dialect::BigQuery => {
                let names = self.columns()?;
                vec![format!(
                    "MERGE INTO {t} AS {TARGET_ALIAS}\nUSING (\n{}\n) AS {SOURCE_ALIAS}\nON FALSE\n\
                     WHEN NOT MATCHED BY SOURCE THEN DELETE\n{}",
                    self.body,
                    self.merge_insert(&names)
                )]
            }
            // See `Dialect::Databricks`: the one whole-table replace whose meaning does
            // not depend on `partitionOverwriteMode`, and the reason the positional
            // `REPLACE WHERE` below can trust the table's column order.
            Dialect::Databricks => return self.create_table(),
            // DELETE, not TRUNCATE: TRUNCATE commits implicitly on Redshift.
            Dialect::Redshift => {
                let cols = self.column_list()?;
                vec![
                    "BEGIN".into(),
                    format!("DELETE FROM {t}"),
                    format!("INSERT INTO {t} ({cols})\n{}", self.select(&cols, None)),
                    "COMMIT".into(),
                ]
            }
        };
        Ok((stmts, true))
    }

    fn replace_partitions(
        &self,
        strategy: ReplaceStrategy,
        column: &str,
        keys: &[String],
        ptype: PartitionType,
    ) -> Result<(Vec<String>, bool), RenderError> {
        self.require_output_column(column, "partition_column")?;
        let t = &self.target;
        let col = self.ident(column)?;
        let values = self.literals(keys, ptype)?;
        // The same keys on both sides, always — rule 1 of this crate.
        let target_pred = format!("{col} IN ({values})");
        let source_pred = format!("{MODEL_ALIAS}.{col} IN ({values})");
        let merge_target_pred = format!("{TARGET_ALIAS}.{col} IN ({values})");

        let out = match (self.d, strategy) {
            // One statement: the CTE's DELETE and the INSERT share a snapshot, so the
            // delete sees only the old rows and the pair commits or fails together.
            // Single-statement executors (dagron-step-sql on the Postgres wire) can
            // run it as-is. All three strategies mean "replace the rows of these
            // partitions", and this is Postgres's one atomic way to say it.
            (Dialect::Postgres, _) => {
                let cols = self.column_list()?;
                (
                    vec![format!(
                        "WITH freshet_deleted AS (\n  DELETE FROM {t} WHERE {target_pred}\n)\n\
                         INSERT INTO {t} ({cols})\n{}",
                        self.select(&cols, Some(&source_pred))
                    )],
                    true,
                )
            }
            // No data-modifying CTE on Redshift: the same delete + insert, in a
            // transaction, for every strategy.
            (Dialect::Redshift, _) => {
                let cols = self.column_list()?;
                (
                    vec![
                        "BEGIN".into(),
                        format!("DELETE FROM {t} WHERE {target_pred}"),
                        format!(
                            "INSERT INTO {t} ({cols})\n{}",
                            self.select(&cols, Some(&source_pred))
                        ),
                        "COMMIT".into(),
                    ],
                    true,
                )
            }
            // No single-statement partition replace, so every strategy is a
            // delete + insert inside an explicit transaction. `BEGIN TRANSACTION`
            // rather than `BEGIN`, which Snowflake Scripting reads as a block.
            (Dialect::Snowflake, _) => {
                let cols = self.column_list()?;
                (
                    vec![
                        "BEGIN TRANSACTION".into(),
                        format!("DELETE FROM {t} WHERE {target_pred}"),
                        format!(
                            "INSERT INTO {t} ({cols})\n{}",
                            self.select(&cols, Some(&source_pred))
                        ),
                        "COMMIT".into(),
                    ],
                    true,
                )
            }
            // `delete_insert` is honoured literally, as a multi-statement transaction.
            // BigQuery allows one only inside a single script (or a session), and an
            // executor that sends each entry as its own query job — Airflow's BigQuery
            // hook does — would run `BEGIN TRANSACTION` alone and fail, or worse,
            // commit the DELETE without the INSERT. So the transaction is ONE entry:
            // a script. The other two strategies get the atomic single-statement form.
            (Dialect::BigQuery, ReplaceStrategy::DeleteInsert) => {
                let cols = self.column_list()?;
                (
                    vec![format!(
                        "BEGIN TRANSACTION;\nDELETE FROM {t} WHERE {target_pred};\n\
                         INSERT INTO {t} ({cols})\n{};\nCOMMIT TRANSACTION",
                        self.select(&cols, Some(&source_pred))
                    )],
                    true,
                )
            }
            (Dialect::BigQuery, _) => {
                let names = self.columns()?;
                (
                    vec![format!(
                        "MERGE INTO {t} AS {TARGET_ALIAS}\nUSING (\n{}\n) AS {SOURCE_ALIAS}\nON FALSE\n\
                         WHEN NOT MATCHED BY SOURCE AND {merge_target_pred} THEN DELETE\n{}",
                        self.scoped_source(&source_pred),
                        self.merge_insert(&names)
                    )],
                    true,
                )
            }
            // Delta has no multi-statement transactions: honoured literally, and
            // flagged non-atomic so nobody mistakes it for `replace_where`.
            (Dialect::Databricks, ReplaceStrategy::DeleteInsert) => {
                let cols = self.column_list()?;
                (
                    vec![
                        format!("DELETE FROM {t} WHERE {target_pred}"),
                        format!(
                            "INSERT INTO {t} ({cols})\n{}",
                            self.select(&cols, Some(&source_pred))
                        ),
                    ],
                    false,
                )
            }
            // Atomic, positional: see `Dialect::Databricks` for why position is sound.
            (Dialect::Databricks, _) => (
                vec![format!(
                    "INSERT INTO {t} REPLACE WHERE {target_pred}\n{}",
                    self.scoped_source(&source_pred)
                )],
                true,
            ),
        };
        Ok(out)
    }

    fn merge(
        &self,
        unique_key: &[String],
        scope: Option<(&str, &[String], PartitionType)>,
    ) -> Result<(Vec<String>, bool), RenderError> {
        let names = self.columns()?;
        for k in unique_key {
            self.require_output_column(k, "unique_key column")?;
        }
        let source = match scope {
            Some((column, keys, ptype)) => {
                self.require_output_column(column, "partition_column")?;
                let pred = format!(
                    "{MODEL_ALIAS}.{} IN ({})",
                    self.ident(column)?,
                    self.literals(keys, ptype)?
                );
                self.scoped_source(&pred)
            }
            // No partition column: the partitions cannot bound the source, so the
            // whole model output is merged. A superset of the scoped rows, and merge
            // is keyed, so it is more work and never a different answer.
            None => self.body.clone(),
        };

        let key_idents: Vec<String> = unique_key
            .iter()
            .map(|k| self.ident(k))
            .collect::<Result<_, _>>()?;
        let on = key_idents
            .iter()
            .map(|k| format!("{TARGET_ALIAS}.{k} = {SOURCE_ALIAS}.{k}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let updates: Vec<String> = names
            .iter()
            .filter(|c| !key_idents.contains(c))
            .map(|c| format!("{c} = {SOURCE_ALIAS}.{c}"))
            .collect();

        // Redshift's MERGE takes only a table as its source, so its upsert is the keyed
        // delete-then-insert AWS documents, in one transaction: every target row whose
        // key the source has is replaced by the source's row, and new keys are added.
        if self.d == Dialect::Redshift {
            let t = &self.target;
            let matches = key_idents
                .iter()
                .map(|k| format!("{t}.{k} = {SOURCE_ALIAS}.{k}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            let cols = names.join(", ");
            return Ok((
                vec![
                    "BEGIN".into(),
                    format!(
                        "DELETE FROM {t}\nWHERE EXISTS (\nSELECT 1 FROM (\n{source}\n) AS {SOURCE_ALIAS}\nWHERE {matches}\n)"
                    ),
                    format!(
                        "INSERT INTO {t} ({cols})\nSELECT {cols} FROM (\n{source}\n) AS {SOURCE_ALIAS}"
                    ),
                    "COMMIT".into(),
                ],
                true,
            ));
        }

        let mut stmt = format!(
            "MERGE INTO {} AS {TARGET_ALIAS}\nUSING (\n{source}\n) AS {SOURCE_ALIAS}\nON {on}\n",
            self.target
        );
        // A model made only of key columns has nothing to update; an empty SET list
        // is a syntax error everywhere, so the clause is dropped rather than faked.
        if !updates.is_empty() {
            stmt.push_str(&format!(
                "WHEN MATCHED THEN UPDATE SET {}\n",
                updates.join(", ")
            ));
        }
        stmt.push_str(&self.merge_insert(&names));
        Ok((vec![stmt], true))
    }

    // --- building blocks ------------------------------------------------------

    /// `SELECT <cols> FROM (<model>) AS freshet_model [WHERE <pred>]`.
    fn select(&self, cols: &str, pred: Option<&str>) -> String {
        let mut s = format!("SELECT {cols} FROM (\n{}\n) AS {MODEL_ALIAS}", self.body);
        if let Some(p) = pred {
            s.push_str(&format!("\nWHERE {p}"));
        }
        s
    }

    /// The model's rows for the scoped partitions, every column, in model order.
    fn scoped_source(&self, pred: &str) -> String {
        self.select("*", Some(pred))
    }

    fn merge_insert(&self, names: &[String]) -> String {
        let values: Vec<String> = names
            .iter()
            .map(|c| format!("{SOURCE_ALIAS}.{c}"))
            .collect();
        format!(
            "WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
            names.join(", "),
            values.join(", ")
        )
    }

    /// The model's output columns, rendered, for a statement that must name them.
    fn columns(&self) -> Result<Vec<String>, RenderError> {
        if self.model.columns.is_empty() {
            return Err(RenderError::NoColumns {
                model: self.model.name.clone(),
            });
        }
        self.model
            .columns
            .iter()
            .map(|c| {
                if is_synthetic_column(&c.name) {
                    return Err(RenderError::UnnamedColumn {
                        model: self.model.name.clone(),
                        column: c.name.clone(),
                    });
                }
                self.ident(&c.name)
            })
            .collect()
    }

    fn column_list(&self) -> Result<String, RenderError> {
        Ok(self.columns()?.join(", "))
    }

    fn require_output_column(&self, column: &str, role: &'static str) -> Result<(), RenderError> {
        if self.model.columns.iter().any(|c| c.name == column) {
            Ok(())
        } else {
            Err(RenderError::UnknownColumn {
                model: self.model.name.clone(),
                column: column.to_string(),
                role,
            })
        }
    }

    fn ident(&self, name: &str) -> Result<String, RenderError> {
        ident(self.d, name).ok_or_else(|| RenderError::UnsafeIdentifier {
            model: self.model.name.clone(),
            identifier: name.to_string(),
        })
    }

    fn literals(&self, keys: &[String], ptype: PartitionType) -> Result<String, RenderError> {
        let lits: Vec<String> = keys
            .iter()
            .map(|k| literal(&self.model.name, k, ptype))
            .collect::<Result<_, _>>()?;
        Ok(lits.join(", "))
    }
}

// ---------------------------------------------------------------------------
// Lexical helpers
// ---------------------------------------------------------------------------

/// The model's SQL, ready to embed: trimmed, trailing semicolons removed, and — if
/// its last line holds a `--` comment — ending in a newline, so nothing appended
/// after it (a `)` or a `;`) can be commented out.
fn model_body(model: &Model) -> Result<String, RenderError> {
    let mut s = model.sql.trim();
    while let Some(rest) = s.strip_suffix(';') {
        s = rest.trim_end();
    }
    if s.is_empty() {
        return Err(RenderError::EmptySql {
            model: model.name.clone(),
        });
    }
    let mut body = s.to_string();
    if body.lines().last().is_some_and(|l| l.contains("--")) {
        body.push('\n');
    }
    Ok(body)
}

/// `name` rendered as a relation in `opts.schema`, or `None` if any part of it cannot
/// be quoted safely.
fn qualified(opts: &RenderOptions, name: &str) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(schema) = &opts.schema {
        for part in schema.split('.') {
            parts.push(ident(opts.dialect, part)?);
        }
    }
    parts.push(ident(opts.dialect, name)?);
    Some(parts.join("."))
}

/// The rendered target relation for `model`, qualified by `opts.schema`.
///
/// A model name is one identifier, not a path: lineage matches relations by their
/// last identifier, so a dotted model name is a name with a dot in it, and is quoted.
fn relation(model: &Model, opts: &RenderOptions) -> Result<String, RenderError> {
    qualified(opts, &model.name).ok_or_else(|| RenderError::UnsafeIdentifier {
        model: model.name.clone(),
        identifier: match &opts.schema {
            Some(schema) => format!("{schema}.{}", model.name),
            None => model.name.clone(),
        },
    })
}

/// Render an identifier: as written when it is a plain identifier, quoted otherwise.
///
/// Leaving plain identifiers unquoted is deliberate, not laziness. Quoting changes
/// meaning: `"events"` is case-sensitive lowercase in Snowflake while the model's own
/// unquoted `events` resolves to `EVENTS`. Writing names the way the user's SQL does
/// makes the target resolve the way their SQL resolves it. Only names that cannot be
/// written bare (spaces, dashes, reserved words) are quoted, and one this crate
/// cannot quote safely in every dialect is refused (`None`).
pub fn ident(d: Dialect, name: &str) -> Option<String> {
    if name.is_empty()
        || name
            .chars()
            .any(|c| c == '"' || c == '`' || c == '\\' || c.is_control())
    {
        return None;
    }
    let plain = {
        let mut chars = name.chars();
        chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    };
    if plain && !is_reserved(name) {
        Some(name.to_string())
    } else {
        let q = d.quote();
        Some(format!("{q}{name}{q}"))
    }
}

/// Render a partition key as a literal of its declared type.
fn literal(model: &str, key: &str, ptype: PartitionType) -> Result<String, RenderError> {
    let bad = || RenderError::BadPartitionKey {
        model: model.to_string(),
        key: key.to_string(),
        expected: ptype,
    };
    match ptype {
        PartitionType::String => {
            let safe = !key.is_empty()
                && key.chars().all(|c| {
                    c.is_ascii_alphanumeric()
                        || matches!(c, ' ' | '-' | '_' | ':' | '.' | '/' | '+')
                });
            if safe {
                Ok(format!("'{key}'"))
            } else {
                Err(RenderError::UnsafePartitionKey {
                    model: model.to_string(),
                    key: key.to_string(),
                })
            }
        }
        PartitionType::Date => {
            if is_calendar_date(key) {
                Ok(format!("DATE '{key}'"))
            } else {
                Err(bad())
            }
        }
        PartitionType::Int => {
            // Parse, then print the parsed value: `007` and `+7` become `7`, and
            // nothing but digits and a sign can reach the statement.
            key.trim()
                .parse::<i64>()
                .map(|v| v.to_string())
                .map_err(|_| bad())
        }
    }
}

/// `YYYY-MM-DD` naming a real day (so `2026-02-30` is refused, not rolled over).
fn is_calendar_date(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return false;
    }
    let digits = |r: std::ops::Range<usize>| b[r].iter().all(u8::is_ascii_digit);
    if !(digits(0..4) && digits(5..7) && digits(8..10)) {
        return false;
    }
    let (y, m, d): (u32, u32, u32) = (
        s[0..4].parse().unwrap(),
        s[5..7].parse().unwrap(),
        s[8..10].parse().unwrap(),
    );
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let days = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    (1..=days).contains(&d)
}

/// The name lineage gives an unaliased expression (`_col0`, `_col1`, …).
fn is_synthetic_column(name: &str) -> bool {
    name.strip_prefix("_col")
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

/// Words reserved in at least one supported dialect, which therefore cannot be
/// written bare as an identifier. Deliberately a superset: quoting a word that did
/// not need it only costs a pair of quotes.
fn is_reserved(name: &str) -> bool {
    const RESERVED: &[&str] = &[
        "ALL",
        "ALTER",
        "AND",
        "ANY",
        "ARRAY",
        "AS",
        "ASC",
        "BETWEEN",
        "BY",
        "CASE",
        "CAST",
        "CHECK",
        "COLUMN",
        "CONSTRAINT",
        "CREATE",
        "CROSS",
        "CURRENT",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "DEFAULT",
        "DELETE",
        "DESC",
        "DISTINCT",
        "DROP",
        "ELSE",
        "END",
        "EXCEPT",
        "EXISTS",
        "FALSE",
        "FETCH",
        "FOR",
        "FOREIGN",
        "FROM",
        "FULL",
        "GRANT",
        "GROUP",
        "HAVING",
        "IN",
        "INNER",
        "INSERT",
        "INTERSECT",
        "INTERVAL",
        "INTO",
        "IS",
        "JOIN",
        "LATERAL",
        "LEFT",
        "LIKE",
        "LIMIT",
        "MERGE",
        "NATURAL",
        "NOT",
        "NULL",
        "OF",
        "OFFSET",
        "ON",
        "OR",
        "ORDER",
        "OUTER",
        "OVER",
        "PARTITION",
        "PRIMARY",
        "QUALIFY",
        "RANGE",
        "REFERENCES",
        "RIGHT",
        "ROW",
        "ROWS",
        "SELECT",
        "SET",
        "SOME",
        "TABLE",
        "THEN",
        "TO",
        "TRUE",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "USER",
        "USING",
        "VALUES",
        "WHEN",
        "WHERE",
        "WINDOW",
        "WITH",
    ];
    RESERVED.iter().any(|r| r.eq_ignore_ascii_case(name))
}

pub mod migrate;

#[cfg(test)]
mod tests;
