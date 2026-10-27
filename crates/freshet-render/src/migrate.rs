//! Moving a warehouse, partition by partition: the SQL for each side of a copy.
//!
//! A move copies the relations a plan names from one warehouse to another, through
//! Parquet in object storage: each partition is **unloaded** from the source into a
//! directory of its own under a *stage*, then **loaded** into the target, replacing
//! that partition there. Each unit also carries what a **parity check** needs to show
//! the two sides equal afterwards. dagron runs all three (`dagron-step-sql` for the
//! unload and the load, `dagron-step-sql parity` for the check).
//!
//! This module writes the text, and nothing else, like the rest of the crate: no
//! connection, no credential, no storage access. The SQL names no host and holds no
//! secret. The stage is reached through each warehouse's own grant (a Snowflake
//! storage integration, a Redshift IAM role, the identity BigQuery and Databricks run
//! as) or, for DuckDB, its credential chain.
//!
//! ## One unit, one partition
//!
//! A model the plan rebuilds by partition is moved one partition per unit, so a
//! failure retries one partition and the parity check answers per partition, which
//! is what a cut-over is decided on. A model the plan rebuilds whole (a table, an
//! incremental model the plan widened) is one unit for the whole relation. A view
//! holds no rows, so it is not moved: the target gets it by being planned for, like
//! any other view.
//!
//! ## Postgres is moved through DuckDB
//!
//! Postgres reads and writes no Parquet. On a Postgres side the unload and the load
//! are DuckDB scripts that attach the database with DuckDB's `postgres` extension
//! (`ATTACH '' AS freshet_pg (TYPE postgres)`), through libpq's own environment
//! (`PGHOST`, `PGDATABASE`, …). That is why a unit's [`StepSql::engine`] can be
//! `duckdb` while its parity side's is `postgres`: the check runs on Postgres itself.
//!
//! ## What the load assumes
//!
//! The target relation exists, with the model's columns. A cold plan for the target
//! creates it (`freshet plan --sql <target>` recreates every model it has never
//! seen). The load names its columns where the statement allows it (Postgres,
//! Snowflake by name, BigQuery); Redshift's `COPY` and Databricks' `REPLACE WHERE` take
//! the Parquet's columns by position, which is the model's order, because the unload
//! wrote them in it.
//!
//! ## Rules this module keeps
//!
//! 1. **The same rows on both sides.** A unit's unload, load and both parity sides
//!    are scoped by the same keys, rendered by the same literal rules as a rebuild.
//! 2. **A rerun is a rerun.** Every unload overwrites its directory (or its one
//!    file), and every load replaces its partition inside one transaction, so a
//!    retried unit leaves what one run leaves. Snowflake's `COPY INTO` is `FORCE`d,
//!    because it otherwise skips a file it remembers loading, which after the
//!    `DELETE` would leave the partition empty.
//! 3. **Refuse what a side cannot reach.** A stage the source cannot write or the
//!    target cannot read is an error before anything is rendered, not a statement
//!    that fails half-way through a move.

use std::fmt;
use std::str::FromStr;

use freshet_core::{BackfillPlan, BackfillUnit, Materialization, Model, Project};
use freshet_incremental::{IncrementalConfig, PartitionType};

use crate::{ident, is_synthetic_column, literal, Dialect, RenderError, RenderErrors};

/// The engine that attaches a Postgres side, in `SQL_ENGINE`'s spelling.
const DUCKDB: &str = "duckdb";
/// The name DuckDB gives the attached Postgres database.
const ATTACHED: &str = "freshet_pg";

// ---------------------------------------------------------------------------
// The stage
// ---------------------------------------------------------------------------

/// Where the Parquet goes between the two sides.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scheme {
    /// `s3://bucket/prefix/`
    S3,
    /// `gs://bucket/prefix/` (Snowflake spells it `gcs://`).
    Gcs,
    /// `file:///path/`: a directory both DuckDB sides can see. Postgres to Postgres
    /// only, since no cloud warehouse reads a local path.
    File,
}

impl fmt::Display for Scheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Scheme::S3 => "s3://",
            Scheme::Gcs => "gs://",
            Scheme::File => "file://",
        })
    }
}

/// The stage's root: the move's own prefix. Every unit is one directory directly
/// under it, so the root is the one thing to create, delete or grant access to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stage {
    pub scheme: Scheme,
    /// The root URI, always ending in `/`.
    pub root: String,
}

impl FromStr for Stage {
    type Err = String;

    fn from_str(raw: &str) -> Result<Stage, String> {
        let raw = raw.trim();
        let scheme = if raw.starts_with("s3://") {
            Scheme::S3
        } else if raw.starts_with("gs://") {
            Scheme::Gcs
        } else if raw.starts_with("file:///") {
            Scheme::File
        } else {
            return Err(format!(
                "stage {raw:?} must be s3://bucket/prefix/, gs://bucket/prefix/ or file:///absolute/path/"
            ));
        };
        // It goes inside a string literal on every side, so nothing that could end
        // one or escape in one, and nothing a URL would need to encode.
        if let Some(c) = raw.chars().find(|c| !(c.is_ascii_alphanumeric() || "/:._-=~+@".contains(*c))) {
            return Err(format!("stage {raw:?} holds {c:?}; a stage URI here is letters, digits and / : . _ - = ~ + @"));
        }
        let after = raw.split_once("://").map(|(_, r)| r).unwrap_or("");
        if scheme != Scheme::File && after.split('/').next().is_none_or(str::is_empty) {
            return Err(format!("stage {raw:?} names no bucket"));
        }
        let mut root = raw.to_string();
        if !root.ends_with('/') {
            root.push('/');
        }
        Ok(Stage { scheme, root })
    }
}

impl Stage {
    /// The stage as `dialect` writes it.
    fn uri(&self, d: Dialect, path: &str) -> String {
        let full = format!("{}{path}", self.root);
        match (d, self.scheme) {
            (Dialect::Snowflake, Scheme::Gcs) => full.replacen("gs://", "gcs://", 1),
            (_, Scheme::File) => full.replacen("file://", "", 1),
            _ => full,
        }
    }
}

/// The schemes a side can write (as the source) and read (as the target).
fn reaches(d: Dialect) -> &'static [Scheme] {
    match d {
        // DuckDB, through httpfs and its credential chain, or a local path.
        Dialect::Postgres => &[Scheme::S3, Scheme::File],
        Dialect::Redshift => &[Scheme::S3],
        Dialect::Snowflake | Dialect::Databricks => &[Scheme::S3, Scheme::Gcs],
        Dialect::BigQuery => &[Scheme::Gcs],
    }
}

/// A directory name for one part of a unit's path: letters, digits, `-` and `_`
/// as they are, anything else as `~HH` per byte. A key may hold `/`, `:` or a space,
/// and none of them belongs in a single directory name on every store.
fn segment(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' {
            out.push(b as char);
        } else {
            out.push_str(&format!("~{b:02X}"));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Inputs and outputs
// ---------------------------------------------------------------------------

/// How to render a move.
#[derive(Clone, Debug)]
pub struct MoveOptions {
    pub from: Dialect,
    pub to: Dialect,
    /// Qualifies every relation on the source, as [`crate::RenderOptions::schema`].
    pub from_schema: Option<String>,
    /// Qualifies every relation on the target.
    pub to_schema: Option<String>,
    pub stage: Stage,
    /// Snowflake, on either side: the storage integration that reaches the stage.
    pub storage_integration: Option<String>,
    /// Redshift, on either side: the IAM role that reaches the stage, an ARN, or
    /// `default` for the cluster's default role.
    pub iam_role: Option<String>,
}

/// Statements one task runs, in order, on one session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepSql {
    /// What runs them, as `SQL_ENGINE` spells it: the dialect's own name, or
    /// `duckdb` for a Postgres side.
    pub engine: &'static str,
    /// None carries a trailing `;`, as in [`crate::RenderedSql`].
    pub statements: Vec<String>,
    /// Whether the relation it writes is all-or-nothing. An unload writes no
    /// relation, so it is `true`.
    pub atomic: bool,
}

impl StepSql {
    /// The statements as one script, each terminated with `;`.
    pub fn script(&self) -> String {
        self.statements.iter().map(|s| format!("{s};")).collect::<Vec<_>>().join("\n")
    }
}

/// One side of a unit's parity check: what `dagron-step-sql parity` compares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParitySide {
    /// The store itself (`postgres` on a Postgres side, not `duckdb`).
    pub engine: &'static str,
    /// As the move was given it, for the store's `information_schema`.
    pub schema: Option<String>,
    pub table: String,
    /// The relation as this side's SQL writes it.
    pub relation: String,
    /// The unit's rows, in this side's dialect. `None` for a whole relation.
    pub predicate: Option<String>,
}

/// A unit's partition: the declared column and one key of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionRef {
    pub column: String,
    pub key: String,
    pub partition_type: PartitionType,
}

/// One copy: unload, load, check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MoveUnit {
    pub model: String,
    /// `None` moves the whole relation.
    pub partition: Option<PartitionRef>,
    /// The unit's directory under the stage, ending in `/`.
    pub location: String,
    pub unload: StepSql,
    pub load: StepSql,
    pub source: ParitySide,
    pub target: ParitySide,
}

/// A model the plan names that is not moved, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotMoved {
    pub model: String,
    pub reason: String,
}

/// Every unit of a move, in plan order, and the models it leaves out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderedMove {
    pub units: Vec<MoveUnit>,
    pub not_moved: Vec<NotMoved>,
}

/// Why a move could not be rendered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MoveError {
    /// The move itself: its stage, a side that cannot reach it, a grant it needs.
    Setup(String),
    /// These models, every one of them.
    Models(RenderErrors),
}

impl fmt::Display for MoveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MoveError::Setup(m) => f.write_str(m),
            MoveError::Models(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for MoveError {}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Render a move of every model `plan` names, in plan order.
///
/// All or nothing, like [`crate::render_plan`]: a move that copied some of a plan's
/// models and not others would be cut over on a partial answer.
pub fn render_move(
    project: &Project,
    plan: &BackfillPlan,
    cfg: &IncrementalConfig,
    opts: &MoveOptions,
) -> Result<RenderedMove, MoveError> {
    check_setup(opts)?;
    let mut units = Vec::new();
    let mut not_moved = Vec::new();
    let mut errors = Vec::new();
    for name in &plan.models {
        let Some(model) = project.get(name) else {
            errors.push(RenderError::UnknownModel { model: name.clone() });
            continue;
        };
        if model.materialization == Materialization::View {
            not_moved.push(NotMoved {
                model: name.clone(),
                reason: "a view holds no rows: the target gets it by being planned for".into(),
            });
            continue;
        }
        match model_units(model, plan, cfg, opts) {
            Ok(mut us) => units.append(&mut us),
            Err(e) => errors.push(e),
        }
    }
    if errors.is_empty() {
        Ok(RenderedMove { units, not_moved })
    } else {
        Err(MoveError::Models(RenderErrors(errors)))
    }
}

/// What has to hold before any model is looked at.
fn check_setup(opts: &MoveOptions) -> Result<(), MoveError> {
    let setup = |m: String| Err(MoveError::Setup(m));
    for (role, d) in [("source", opts.from), ("target", opts.to)] {
        if !reaches(d).contains(&opts.stage.scheme) {
            let can: Vec<String> = reaches(d).iter().map(ToString::to_string).collect();
            return setup(format!(
                "the {role}, {d}, cannot reach a {} stage; it reaches {}. Pick a stage both sides can \
                 reach, or move in two hops.",
                opts.stage.scheme,
                can.join(" and ")
            ));
        }
    }
    for (role, d, schema) in [("source", opts.from, &opts.from_schema), ("target", opts.to, &opts.to_schema)] {
        if d == Dialect::Postgres && schema.as_deref().is_some_and(|s| s.contains('.')) {
            return setup(format!(
                "the {role} is Postgres, whose relations are schema.table: its schema is one name, not \
                 {:?}",
                schema.as_deref().unwrap_or_default()
            ));
        }
    }
    let uses = |d: Dialect| opts.from == d || opts.to == d;
    if uses(Dialect::Snowflake) {
        match &opts.storage_integration {
            None => {
                return setup(
                    "a Snowflake side reaches the stage through a storage integration: name it \
                     (--storage-integration NAME). Freshet writes no credential into SQL."
                        .into(),
                )
            }
            Some(i) if ident(Dialect::Snowflake, i).is_none() => {
                return setup(format!("storage integration {i:?} cannot be quoted safely"))
            }
            Some(_) => {}
        }
    }
    if uses(Dialect::Redshift) {
        match &opts.iam_role {
            None => {
                return setup(
                    "a Redshift side reaches the stage through an IAM role: name it (--iam-role \
                     arn:aws:iam::…:role/…, or `default`). Freshet writes no credential into SQL."
                        .into(),
                )
            }
            Some(r) if r != "default" && !r.chars().all(|c| c.is_ascii_alphanumeric() || ":/_+=,.@-".contains(c)) => {
                return setup(format!("IAM role {r:?} is not an ARN"))
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// The units for one model: one per partition the plan names, or one for the whole
/// relation.
fn model_units(
    model: &Model,
    plan: &BackfillPlan,
    cfg: &IncrementalConfig,
    opts: &MoveOptions,
) -> Result<Vec<MoveUnit>, RenderError> {
    let unit = plan.unit.get(&model.name).cloned().unwrap_or(BackfillUnit::FullModel);
    let declared = cfg
        .models
        .get(&model.name)
        .and_then(|m| m.replace.as_ref())
        .and_then(|s| s.partition_column.clone().map(|c| (c, s.partition_type.unwrap_or_default())));
    // A table has no partitions to move one at a time, whatever the plan's unit.
    let keys = match (&unit, model.materialization) {
        (BackfillUnit::Partitions(keys), Materialization::Incremental) if !keys.is_empty() => keys.clone(),
        _ => Vec::new(),
    };
    if keys.is_empty() {
        return Ok(vec![unit_for(model, None, opts)?]);
    }
    let Some((column, partition_type)) = declared else {
        return Err(RenderError::UndeclaredPartitionColumn { model: model.name.clone(), partitions: keys.len() });
    };
    if !model.columns.iter().any(|c| c.name == column) {
        return Err(RenderError::UnknownColumn { model: model.name.clone(), column, role: "partition_column" });
    }
    keys.into_iter()
        .map(|key| unit_for(model, Some(PartitionRef { column: column.clone(), key, partition_type }), opts))
        .collect()
}

/// How one side refers to the model's relation and columns.
struct SideNames {
    d: Dialect,
    /// The relation in this side's SQL. On a Postgres side, through DuckDB's
    /// attached catalog, for the unload or the load; the parity side uses
    /// `relation_native`.
    relation: String,
    relation_native: String,
    cols: Vec<String>,
    predicate: Option<String>,
}

fn side_names(
    model: &Model,
    d: Dialect,
    schema: &Option<String>,
    partition: &Option<PartitionRef>,
) -> Result<SideNames, RenderError> {
    let unsafe_ident = |identifier: &str| RenderError::UnsafeIdentifier {
        model: model.name.clone(),
        identifier: identifier.to_string(),
    };
    let mut parts = Vec::new();
    if let Some(s) = schema {
        for p in s.split('.') {
            parts.push(ident(d, p).ok_or_else(|| unsafe_ident(s))?);
        }
    }
    parts.push(ident(d, &model.name).ok_or_else(|| unsafe_ident(&model.name))?);
    let relation_native = parts.join(".");
    let relation = if d == Dialect::Postgres {
        format!("{ATTACHED}.{relation_native}")
    } else {
        relation_native.clone()
    };
    if model.columns.is_empty() {
        return Err(RenderError::NoColumns { model: model.name.clone() });
    }
    let cols = model
        .columns
        .iter()
        .map(|c| {
            if is_synthetic_column(&c.name) {
                return Err(RenderError::UnnamedColumn { model: model.name.clone(), column: c.name.clone() });
            }
            ident(d, &c.name).ok_or_else(|| unsafe_ident(&c.name))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let predicate = match partition {
        None => None,
        Some(p) => Some(format!(
            "{} IN ({})",
            ident(d, &p.column).ok_or_else(|| unsafe_ident(&p.column))?,
            literal(&model.name, &p.key, p.partition_type)?
        )),
    };
    Ok(SideNames { d, relation, relation_native, cols, predicate })
}

fn unit_for(model: &Model, partition: Option<PartitionRef>, opts: &MoveOptions) -> Result<MoveUnit, RenderError> {
    let src = side_names(model, opts.from, &opts.from_schema, &partition)?;
    let dst = side_names(model, opts.to, &opts.to_schema, &partition)?;
    let dir = match &partition {
        None => segment(&model.name),
        Some(p) => format!("{}.{}={}", segment(&model.name), segment(&p.column), segment(&p.key)),
    };
    let location = format!("{}{dir}/", opts.stage.root);
    let unload = unload(&src, opts, &dir);
    let load = load(&dst, opts, &dir);
    let parity = |s: &SideNames, schema: &Option<String>| ParitySide {
        engine: s.d.as_str(),
        schema: schema.clone(),
        table: model.name.clone(),
        relation: s.relation_native.clone(),
        predicate: s.predicate.clone(),
    };
    Ok(MoveUnit {
        model: model.name.clone(),
        source: parity(&src, &opts.from_schema),
        target: parity(&dst, &opts.to_schema),
        partition,
        location,
        unload,
        load,
    })
}

/// `SELECT <cols> FROM <relation> [WHERE <predicate>]`: the unit's rows, in the
/// model's column order.
fn select(s: &SideNames) -> String {
    let mut q = format!("SELECT {} FROM {}", s.cols.join(", "), s.relation);
    if let Some(p) = &s.predicate {
        q.push_str(&format!(" WHERE {p}"));
    }
    q
}

/// DuckDB's opening statements on a Postgres side: the extensions, the stage's
/// credentials, and the attached database. `read_only` for an unload.
fn duckdb_preamble(stage: &Stage, read_only: bool) -> Vec<String> {
    let mut s = vec!["INSTALL postgres".to_string(), "LOAD postgres".to_string()];
    if stage.scheme == Scheme::S3 {
        s.extend([
            "INSTALL httpfs".to_string(),
            "LOAD httpfs".to_string(),
            "INSTALL aws".to_string(),
            "LOAD aws".to_string(),
            // The task's AWS environment or instance role: nothing in the SQL.
            "CREATE OR REPLACE SECRET freshet_stage (TYPE s3, PROVIDER credential_chain)".to_string(),
        ]);
    }
    // '' is libpq's environment: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.
    let mode = if read_only { ", READ_ONLY" } else { "" };
    s.push(format!("ATTACH '' AS {ATTACHED} (TYPE postgres{mode})"));
    s
}

fn snowflake_access(opts: &MoveOptions) -> String {
    let name = opts.storage_integration.as_deref().expect("checked in check_setup");
    format!("STORAGE_INTEGRATION = {}", ident(Dialect::Snowflake, name).expect("checked in check_setup"))
}

fn redshift_access(opts: &MoveOptions) -> String {
    match opts.iam_role.as_deref().expect("checked in check_setup") {
        "default" => "IAM_ROLE default".into(),
        arn => format!("IAM_ROLE '{arn}'"),
    }
}

fn unload(s: &SideNames, opts: &MoveOptions, dir: &str) -> StepSql {
    let loc = opts.stage.uri(s.d, &format!("{dir}/"));
    let (engine, statements) = match s.d {
        Dialect::Postgres => {
            let mut stmts = duckdb_preamble(&opts.stage, true);
            stmts.push(match opts.stage.scheme {
                // One file at a fixed name: object storage needs no directory, and a
                // rerun overwrites exactly what the first run wrote.
                Scheme::S3 => format!("COPY ({}) TO '{loc}data_0.parquet' (FORMAT parquet)", select(s)),
                // DuckDB makes the directory and, with OVERWRITE, empties it first.
                _ => format!(
                    "COPY ({}) TO '{}' (FORMAT parquet, PER_THREAD_OUTPUT true, OVERWRITE true)",
                    select(s),
                    loc.trim_end_matches('/')
                ),
            });
            (DUCKDB, stmts)
        }
        // CLEANPATH empties the prefix first, so a rerun with fewer slices leaves no
        // file from the last one. The query is a string literal, so its quotes double.
        Dialect::Redshift => (
            "redshift",
            vec![format!(
                "UNLOAD ('{}')\nTO '{loc}'\n{}\nFORMAT AS PARQUET\nCLEANPATH",
                select(s).replace('\'', "''"),
                redshift_access(opts)
            )],
        ),
        // SINGLE: one file at a fixed name, which OVERWRITE replaces exactly (5 GB
        // at most, the most S3 takes in one PUT). HEADER keeps the column names in
        // the file, which the target's load matches by.
        Dialect::Snowflake => (
            "snowflake",
            vec![format!(
                "COPY INTO '{loc}data_0.parquet'\nFROM ({})\n{}\nFILE_FORMAT = (TYPE = PARQUET)\nHEADER = TRUE\n\
                 OVERWRITE = TRUE\nSINGLE = TRUE\nMAX_FILE_SIZE = 5368709120",
                select(s),
                snowflake_access(opts)
            )],
        ),
        Dialect::BigQuery => (
            "bigquery",
            vec![format!(
                "EXPORT DATA OPTIONS (uri = '{loc}*.parquet', format = 'PARQUET', overwrite = true) AS\n{}",
                select(s)
            )],
        ),
        // OVERWRITE DIRECTORY replaces the directory's contents.
        Dialect::Databricks => (
            "databricks",
            vec![format!("INSERT OVERWRITE DIRECTORY '{loc}' USING PARQUET\n{}", select(s))],
        ),
    };
    StepSql { engine, statements, atomic: true }
}

fn load(s: &SideNames, opts: &MoveOptions, dir: &str) -> StepSql {
    let loc = opts.stage.uri(s.d, &format!("{dir}/"));
    let cols = s.cols.join(", ");
    let t = &s.relation;
    let delete = match &s.predicate {
        Some(p) => format!("DELETE FROM {t} WHERE {p}"),
        // BigQuery's DELETE requires a WHERE.
        None if s.d == Dialect::BigQuery => format!("DELETE FROM {t} WHERE TRUE"),
        None => format!("DELETE FROM {t}"),
    };
    let (engine, statements) = match s.d {
        Dialect::Postgres => {
            let mut stmts = duckdb_preamble(&opts.stage, false);
            stmts.extend([
                "BEGIN".to_string(),
                delete,
                format!("INSERT INTO {t} ({cols})\nSELECT {cols} FROM read_parquet('{loc}*.parquet')"),
                "COMMIT".to_string(),
            ]);
            (DUCKDB, stmts)
        }
        // By position: Redshift's COPY from Parquet maps the file's columns in
        // order, and the unload wrote them in the model's.
        Dialect::Redshift => (
            "redshift",
            vec![
                "BEGIN".into(),
                delete,
                format!("COPY {t}\nFROM '{loc}'\n{}\nFORMAT AS PARQUET", redshift_access(opts)),
                "COMMIT".into(),
            ],
        ),
        // FORCE: COPY INTO remembers the files it loaded and skips them for 64 days.
        // A rerun of this unit deletes the partition first, so a skipped file would
        // leave it empty.
        Dialect::Snowflake => (
            "snowflake",
            vec![
                "BEGIN TRANSACTION".into(),
                delete,
                format!(
                    "COPY INTO {t}\nFROM '{loc}'\n{}\nFILE_FORMAT = (TYPE = PARQUET)\n\
                     MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE\nPATTERN = '.*[.]parquet'\nFORCE = TRUE",
                    snowflake_access(opts)
                ),
                "COMMIT".into(),
            ],
        ),
        // One script, like a rebuild's delete_insert: BigQuery runs a multi-statement
        // transaction only inside one. The files land in a temporary table first, so
        // the transaction holds only the swap.
        Dialect::BigQuery => (
            "bigquery",
            vec![format!(
                "LOAD DATA INTO TEMP TABLE freshet_load\nFROM FILES (format = 'PARQUET', uris = ['{loc}*.parquet']);\n\
                 BEGIN TRANSACTION;\n{delete};\nINSERT INTO {t} ({cols})\nSELECT {cols} FROM freshet_load;\n\
                 COMMIT TRANSACTION"
            )],
        ),
        // One statement, atomic on Delta. Positional, in the model's order, which is
        // the order of the files and of a table Freshet created.
        Dialect::Databricks => (
            "databricks",
            vec![match &s.predicate {
                Some(p) => format!("INSERT INTO {t} REPLACE WHERE {p}\nSELECT {cols} FROM parquet.`{loc}`"),
                None => format!("INSERT OVERWRITE {t}\nSELECT {cols} FROM parquet.`{loc}`"),
            }],
        ),
    };
    StepSql { engine, statements, atomic: true }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freshet_core::Column;
    use freshet_incremental::{ModelPartitionConfig, ReplaceSpec, ReplaceStrategy};

    fn model(name: &str, materialization: Materialization, cols: &[&str]) -> Model {
        Model {
            name: name.into(),
            sql: format!("select {} from raw", cols.join(", ")),
            materialization,
            columns: cols.iter().map(|c| Column::new(*c, Vec::new())).collect(),
            depends_on: Vec::new(),
        }
    }

    fn project() -> Project {
        Project::from_models([
            model("events", Materialization::Incremental, &["dt", "user_id", "amount"]),
            model("user_dim", Materialization::Table, &["user_id", "name"]),
            model("events_v", Materialization::View, &["dt", "user_id"]),
        ])
    }

    fn cfg() -> IncrementalConfig {
        let mut cfg = IncrementalConfig::default();
        cfg.models.insert(
            "events".into(),
            ModelPartitionConfig {
                replace: Some(ReplaceSpec {
                    strategy: Some(ReplaceStrategy::InsertOverwrite),
                    partition_column: Some("dt".into()),
                    unique_key: vec![],
                    partition_type: Some(PartitionType::Date),
                }),
                ..Default::default()
            },
        );
        cfg
    }

    fn plan(models: &[(&str, Option<&[&str]>)]) -> BackfillPlan {
        let mut p = BackfillPlan::default();
        for (m, keys) in models {
            p.models.push(m.to_string());
            if let Some(keys) = keys {
                p.unit.insert(m.to_string(), BackfillUnit::Partitions(keys.iter().map(|k| k.to_string()).collect()));
            }
        }
        p
    }

    fn opts(from: Dialect, to: Dialect, stage: &str) -> MoveOptions {
        MoveOptions {
            from,
            to,
            from_schema: Some("public".into()),
            to_schema: Some(if to == Dialect::Postgres { "marts" } else { "ANALYTICS.MARTS" }.into()),
            stage: stage.parse().unwrap(),
            storage_integration: Some("FRESHET_STAGE".into()),
            iam_role: Some("arn:aws:iam::123456789012:role/freshet-move".into()),
        }
    }

    fn render(from: Dialect, to: Dialect, stage: &str, p: &BackfillPlan) -> RenderedMove {
        render_move(&project(), p, &cfg(), &opts(from, to, stage)).unwrap()
    }

    #[test]
    fn a_stage_is_a_bucket_prefix_or_a_local_directory_and_nothing_that_escapes_a_literal() {
        let s: Stage = "s3://acme-moves/pg-to-sf".parse().unwrap();
        assert_eq!(s.root, "s3://acme-moves/pg-to-sf/", "always a prefix");
        assert_eq!("gs://b/p/".parse::<Stage>().unwrap().scheme, Scheme::Gcs);
        assert_eq!("file:///srv/moves/".parse::<Stage>().unwrap().scheme, Scheme::File);
        assert!("https://b/p".parse::<Stage>().unwrap_err().contains("s3://"));
        assert!("s3:///p".parse::<Stage>().unwrap_err().contains("no bucket"));
        assert!("s3://b/it's".parse::<Stage>().unwrap_err().contains("'\\''"));
        assert!("s3://b/a b".parse::<Stage>().is_err(), "a space would need encoding somewhere");
    }

    /// The exit gate's shape: Postgres on-prem to a cloud warehouse, by partition.
    #[test]
    fn postgres_to_snowflake_moves_each_partition_through_duckdb_and_copy_into() {
        let m = render(Dialect::Postgres, Dialect::Snowflake, "s3://acme-moves/run-1/", &plan(&[("events", Some(&["2026-06-20", "2026-06-21"]))]));
        assert_eq!(m.units.len(), 2, "one unit per partition");
        let u = &m.units[0];
        assert_eq!(u.location, "s3://acme-moves/run-1/events.dt=2026-06-20/");
        assert_eq!(u.partition.as_ref().unwrap().key, "2026-06-20");

        assert_eq!(u.unload.engine, "duckdb");
        assert_eq!(
            u.unload.statements,
            [
                "INSTALL postgres",
                "LOAD postgres",
                "INSTALL httpfs",
                "LOAD httpfs",
                "INSTALL aws",
                "LOAD aws",
                "CREATE OR REPLACE SECRET freshet_stage (TYPE s3, PROVIDER credential_chain)",
                "ATTACH '' AS freshet_pg (TYPE postgres, READ_ONLY)",
                "COPY (SELECT dt, user_id, amount FROM freshet_pg.public.events WHERE dt IN (DATE '2026-06-20')) \
                 TO 's3://acme-moves/run-1/events.dt=2026-06-20/data_0.parquet' (FORMAT parquet)",
            ]
        );

        assert_eq!(u.load.engine, "snowflake");
        assert_eq!(
            u.load.statements,
            [
                "BEGIN TRANSACTION",
                "DELETE FROM ANALYTICS.MARTS.events WHERE dt IN (DATE '2026-06-20')",
                "COPY INTO ANALYTICS.MARTS.events\nFROM 's3://acme-moves/run-1/events.dt=2026-06-20/'\n\
                 STORAGE_INTEGRATION = FRESHET_STAGE\nFILE_FORMAT = (TYPE = PARQUET)\n\
                 MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE\nPATTERN = '.*[.]parquet'\nFORCE = TRUE",
                "COMMIT",
            ]
        );
        assert!(u.load.atomic);

        // The check runs on Postgres itself, not through DuckDB.
        assert_eq!(u.source, ParitySide {
            engine: "postgres",
            schema: Some("public".into()),
            table: "events".into(),
            relation: "public.events".into(),
            predicate: Some("dt IN (DATE '2026-06-20')".into()),
        });
        assert_eq!(u.target.engine, "snowflake");
        assert_eq!(u.target.relation, "ANALYTICS.MARTS.events");
        assert_eq!(u.target.predicate, u.source.predicate, "the same rows on both sides");
    }

    #[test]
    fn postgres_to_postgres_on_a_local_stage_is_duckdb_on_both_sides() {
        let m = render(Dialect::Postgres, Dialect::Postgres, "file:///srv/moves/", &plan(&[("events", Some(&["2026-06-20"]))]));
        let u = &m.units[0];
        assert_eq!(
            u.unload.statements.last().unwrap(),
            "COPY (SELECT dt, user_id, amount FROM freshet_pg.public.events WHERE dt IN (DATE '2026-06-20')) \
             TO '/srv/moves/events.dt=2026-06-20' (FORMAT parquet, PER_THREAD_OUTPUT true, OVERWRITE true)",
        );
        assert!(!u.unload.statements.iter().any(|s| s.contains("SECRET")), "a local stage needs no credentials");
        assert_eq!(u.load.engine, "duckdb");
        assert_eq!(
            u.load.statements[2..],
            [
                "ATTACH '' AS freshet_pg (TYPE postgres)",
                "BEGIN",
                "DELETE FROM freshet_pg.marts.events WHERE dt IN (DATE '2026-06-20')",
                "INSERT INTO freshet_pg.marts.events (dt, user_id, amount)\n\
                 SELECT dt, user_id, amount FROM read_parquet('/srv/moves/events.dt=2026-06-20/*.parquet')",
                "COMMIT",
            ]
        );
    }

    #[test]
    fn a_table_moves_whole_and_a_view_not_at_all() {
        let p = plan(&[("user_dim", Some(&["2026-06-20"])), ("events_v", None)]);
        let m = render(Dialect::Snowflake, Dialect::BigQuery, "gs://acme/moves/", &p);
        assert_eq!(m.units.len(), 1);
        let u = &m.units[0];
        assert_eq!(u.partition, None, "a table has no partitions to move one at a time");
        assert_eq!(u.location, "gs://acme/moves/user_dim/");
        assert_eq!(
            u.unload.statements,
            ["COPY INTO 'gcs://acme/moves/user_dim/data_0.parquet'\nFROM (SELECT user_id, name FROM public.user_dim)\n\
              STORAGE_INTEGRATION = FRESHET_STAGE\nFILE_FORMAT = (TYPE = PARQUET)\nHEADER = TRUE\nOVERWRITE = TRUE\n\
              SINGLE = TRUE\nMAX_FILE_SIZE = 5368709120"],
            "Snowflake spells the GCS scheme gcs://"
        );
        assert_eq!(
            u.load.statements,
            ["LOAD DATA INTO TEMP TABLE freshet_load\nFROM FILES (format = 'PARQUET', uris = ['gs://acme/moves/user_dim/*.parquet']);\n\
              BEGIN TRANSACTION;\nDELETE FROM ANALYTICS.MARTS.user_dim WHERE TRUE;\nINSERT INTO ANALYTICS.MARTS.user_dim (user_id, name)\n\
              SELECT user_id, name FROM freshet_load;\nCOMMIT TRANSACTION"],
            "BigQuery's DELETE needs a WHERE, and its transaction one script"
        );
        assert_eq!(u.target.predicate, None);
        assert_eq!(m.not_moved, [NotMoved { model: "events_v".into(), reason: "a view holds no rows: the target gets it by being planned for".into() }]);
    }

    #[test]
    fn redshift_and_databricks_spell_their_own_copies() {
        let p = plan(&[("events", Some(&["2026-06-20"]))]);
        let m = render(Dialect::Redshift, Dialect::Databricks, "s3://acme/m/", &p);
        let u = &m.units[0];
        assert_eq!(
            u.unload.statements,
            ["UNLOAD ('SELECT dt, user_id, amount FROM public.events WHERE dt IN (DATE ''2026-06-20'')')\n\
              TO 's3://acme/m/events.dt=2026-06-20/'\nIAM_ROLE 'arn:aws:iam::123456789012:role/freshet-move'\n\
              FORMAT AS PARQUET\nCLEANPATH"],
            "the query is a literal, so its quotes double"
        );
        assert_eq!(
            u.load.statements,
            ["INSERT INTO ANALYTICS.MARTS.events REPLACE WHERE dt IN (DATE '2026-06-20')\n\
              SELECT dt, user_id, amount FROM parquet.`s3://acme/m/events.dt=2026-06-20/`"]
        );
        let back = render(Dialect::Databricks, Dialect::Redshift, "s3://acme/m/", &p);
        let u = &back.units[0];
        assert_eq!(u.unload.statements, ["INSERT OVERWRITE DIRECTORY 's3://acme/m/events.dt=2026-06-20/' USING PARQUET\n\
            SELECT dt, user_id, amount FROM public.events WHERE dt IN (DATE '2026-06-20')"]);
        assert_eq!(u.load.statements[0], "BEGIN");
        assert_eq!(u.load.statements[2], "COPY ANALYTICS.MARTS.events\nFROM 's3://acme/m/events.dt=2026-06-20/'\n\
            IAM_ROLE 'arn:aws:iam::123456789012:role/freshet-move'\nFORMAT AS PARQUET");
    }

    #[test]
    fn a_stage_a_side_cannot_reach_is_refused_before_anything_is_rendered() {
        let p = plan(&[("events", Some(&["2026-06-20"]))]);
        let e = render_move(&project(), &p, &cfg(), &opts(Dialect::Postgres, Dialect::BigQuery, "s3://b/m/")).unwrap_err().to_string();
        assert!(e.contains("the target, bigquery, cannot reach a s3:// stage; it reaches gs://"), "{e}");
        let e = render_move(&project(), &p, &cfg(), &opts(Dialect::Redshift, Dialect::Postgres, "file:///srv/m/")).unwrap_err().to_string();
        assert!(e.contains("the source, redshift"), "{e}");
        let mut o = opts(Dialect::Postgres, Dialect::Snowflake, "s3://b/m/");
        o.from_schema = Some("db.public".into());
        assert!(render_move(&project(), &p, &cfg(), &o).unwrap_err().to_string().contains("its schema is one name"));

        let mut o = opts(Dialect::Postgres, Dialect::Snowflake, "s3://b/m/");
        o.storage_integration = None;
        assert!(render_move(&project(), &p, &cfg(), &o).unwrap_err().to_string().contains("--storage-integration"));
        let mut o = opts(Dialect::Redshift, Dialect::Postgres, "s3://b/m/");
        o.iam_role = None;
        assert!(render_move(&project(), &p, &cfg(), &o).unwrap_err().to_string().contains("--iam-role"));
        o.iam_role = Some("arn' OR 1=1".into());
        assert!(render_move(&project(), &p, &cfg(), &o).unwrap_err().to_string().contains("not an ARN"));
        o.iam_role = Some("default".into());
        let m = render_move(&project(), &p, &cfg(), &o).unwrap();
        assert!(m.units[0].unload.statements[0].contains("\nIAM_ROLE default\n"));
    }

    #[test]
    fn a_partitioned_move_of_a_model_with_no_partition_column_is_refused() {
        let p = plan(&[("events", Some(&["2026-06-20"]))]);
        let e = render_move(&project(), &p, &IncrementalConfig::default(), &opts(Dialect::Postgres, Dialect::Snowflake, "s3://b/m/"))
            .unwrap_err()
            .to_string();
        assert!(e.contains("declares no partition_column"), "{e}");
    }

    #[test]
    fn a_key_becomes_one_safe_directory_name() {
        let mut c = cfg();
        c.models.get_mut("events").unwrap().replace.as_mut().unwrap().partition_type = Some(PartitionType::String);
        let p = plan(&[("events", Some(&["eu-west/2026 06:20"]))]);
        let m = render_move(&project(), &p, &c, &opts(Dialect::Postgres, Dialect::Snowflake, "s3://b/m/")).unwrap();
        assert_eq!(m.units[0].location, "s3://b/m/events.dt=eu-west~2F2026~2006~3A20/");
        assert_eq!(m.units[0].source.predicate.as_deref(), Some("dt IN ('eu-west/2026 06:20')"), "the key itself is unchanged in SQL");
    }
}
