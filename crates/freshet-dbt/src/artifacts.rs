//! The slice of dbt's artifacts Freshet reads, and nothing more.
//!
//! A real `manifest.json` runs to tens of megabytes, and its schema is versioned
//! (`https://schemas.getdbt.com/dbt/manifest/v12.json` for dbt 1.8 onwards). Every
//! struct here names only the fields planning needs, defaults every one of them, ignores
//! everything else, and reads `null` wherever dbt is allowed to write one. That is what
//! lets a manifest from an older or newer dbt load: a field this crate has never heard
//! of cannot fail the parse, and a field dbt stopped writing reads as its default.
//!
//! What the defaults must never do is invent a *smaller* graph. The one field whose
//! absence could hide an edge — `depends_on` — defaults to empty exactly where dbt's own
//! schema says it does, and a value of the wrong *type* is still a hard error rather
//! than a silent default: a manifest this crate cannot read correctly is reported, not
//! planned around.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Deserializer};

/// `null` reads as the type's default, exactly like a missing field.
///
/// dbt writes `null` for plenty of optional values (`compiled_code`, `database`,
/// `relation_name`, …); serde alone would reject a `null` where a map or string is
/// declared.
fn nullable<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}

/// `target/manifest.json`, reduced to what the planner reads.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Manifest {
    #[serde(deserialize_with = "nullable")]
    pub metadata: ManifestMetadata,
    /// Models, seeds, snapshots, tests, analyses — keyed by `unique_id`.
    #[serde(deserialize_with = "nullable")]
    pub nodes: BTreeMap<String, Node>,
    #[serde(deserialize_with = "nullable")]
    pub sources: BTreeMap<String, Source>,
    /// Read so a change to a macro a model calls counts as a change to the model.
    #[serde(deserialize_with = "nullable")]
    pub macros: BTreeMap<String, Macro>,
    /// User-defined functions (manifest v12 as written by newer dbt). Read for the same
    /// reason as `macros`: a model calling a changed function has changed.
    #[serde(deserialize_with = "nullable")]
    pub functions: BTreeMap<String, Node>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ManifestMetadata {
    #[serde(deserialize_with = "nullable")]
    pub dbt_schema_version: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub dbt_version: Option<String>,
}

/// One entry of `nodes` (or `functions`).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Node {
    #[serde(deserialize_with = "nullable")]
    pub unique_id: String,
    /// `model`, `seed`, `snapshot`, `test`, `analysis`, …
    #[serde(deserialize_with = "nullable")]
    pub resource_type: String,
    #[serde(deserialize_with = "nullable")]
    pub name: String,
    #[serde(deserialize_with = "nullable")]
    pub package_name: String,
    /// The relation's identifier. dbt defaults it to `name`; compiled SQL refers to the
    /// relation by this, not by the model name.
    #[serde(deserialize_with = "nullable")]
    pub alias: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub database: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub schema: Option<String>,
    /// How dbt renders the relation into SQL (`"db"."schema"."alias"`). Environment-
    /// specific, which is why fingerprints replace it with the node's `unique_id`.
    #[serde(deserialize_with = "nullable")]
    pub relation_name: Option<String>,
    /// The model file as written, Jinja and all.
    #[serde(deserialize_with = "nullable")]
    pub raw_code: Option<String>,
    /// The rendered SQL. Present only for nodes a `dbt compile` / `run` / `build`
    /// actually compiled.
    #[serde(deserialize_with = "nullable")]
    pub compiled_code: Option<String>,
    /// `sql` or `python`.
    #[serde(deserialize_with = "nullable")]
    pub language: Option<String>,
    /// A model version (`2`, `"2"`, `"v2"`); versions of one model share a `name`.
    #[serde(deserialize_with = "nullable")]
    pub version: Option<serde_json::Value>,
    #[serde(deserialize_with = "nullable")]
    pub checksum: Option<FileHash>,
    #[serde(deserialize_with = "nullable")]
    pub depends_on: DependsOn,
    #[serde(deserialize_with = "nullable")]
    pub config: NodeConfig,
    /// Documented columns. Only as complete as the project's YAML.
    #[serde(deserialize_with = "nullable")]
    pub columns: BTreeMap<String, ColumnInfo>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct DependsOn {
    #[serde(deserialize_with = "nullable")]
    pub macros: Vec<String>,
    /// Every node this one reads: models, sources, seeds, snapshots. The graph dbt runs
    /// in, and the edges Freshet plans over.
    #[serde(deserialize_with = "nullable")]
    pub nodes: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct NodeConfig {
    #[serde(deserialize_with = "nullable")]
    pub materialized: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub alias: Option<String>,
    /// The *configured* schema (`+schema: marketing`), not the environment-specific one
    /// dbt generates from it — which is what makes it comparable across environments.
    #[serde(deserialize_with = "nullable")]
    pub schema: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub database: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ColumnInfo {
    #[serde(deserialize_with = "nullable")]
    pub name: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct FileHash {
    #[serde(deserialize_with = "nullable")]
    pub name: String,
    #[serde(deserialize_with = "nullable")]
    pub checksum: String,
}

/// One entry of `sources`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Source {
    #[serde(deserialize_with = "nullable")]
    pub unique_id: String,
    /// The table's name inside its source (`orders` in `source('shop', 'orders')`).
    #[serde(deserialize_with = "nullable")]
    pub name: String,
    /// The source's name (`shop` in `source('shop', 'orders')`).
    #[serde(deserialize_with = "nullable")]
    pub source_name: String,
    #[serde(deserialize_with = "nullable")]
    pub package_name: String,
    /// The physical table name, which compiled SQL uses. Defaults to `name`.
    #[serde(deserialize_with = "nullable")]
    pub identifier: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub database: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub schema: Option<String>,
    /// The database/schema as written in YAML, before target-specific rendering
    /// (newer manifests only). Preferred when comparing two environments.
    #[serde(deserialize_with = "nullable")]
    pub unrendered_database: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub unrendered_schema: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub relation_name: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub columns: BTreeMap<String, ColumnInfo>,
}

/// One entry of `macros`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Macro {
    #[serde(deserialize_with = "nullable")]
    pub macro_sql: String,
    #[serde(deserialize_with = "nullable")]
    pub depends_on: MacroDependsOn,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct MacroDependsOn {
    #[serde(deserialize_with = "nullable")]
    pub macros: Vec<String>,
}

impl Manifest {
    /// Read and parse a `manifest.json`.
    pub fn from_path(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        Self::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// `target/run_results.json`: what the last dbt invocation actually did.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct RunResults {
    #[serde(deserialize_with = "nullable")]
    pub results: Vec<RunResult>,
    /// The invocation's arguments. `which` names the command (`run`, `build`,
    /// `compile`, …) — every dbt command overwrites this file, so it is how a run is
    /// told apart from a compile.
    #[serde(deserialize_with = "nullable")]
    pub args: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct RunResult {
    #[serde(deserialize_with = "nullable")]
    pub unique_id: String,
    /// `success`, `error`, `skipped`, `partial success`, `no-op` for executed nodes;
    /// `pass`/`fail`/`warn` for tests.
    #[serde(deserialize_with = "nullable")]
    pub status: String,
}

impl RunResults {
    pub fn from_path(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        Self::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// The dbt command that wrote the file, when the file says.
    pub fn command(&self) -> Option<&str> {
        self.args.get("which").and_then(|v| v.as_str())
    }
}
