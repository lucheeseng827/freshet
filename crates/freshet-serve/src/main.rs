//! `freshet-serve` — the planner daemon.
//!
//! The point of this binary is that a user never writes Rust. Point it at a
//! directory of `.sql` files and it holds the parsed project warm and answers plan,
//! restatement, graph and commit requests over HTTP, using the same funnel every
//! other binding calls.
//!
//! ```sh
//! freshet-serve --project ./models --port 8788
//! curl -s localhost:8788/rpc -d '{"op":"plan","include_cost":true,"include_queue":true}'
//! ```
//!
//! The models are loaded once at startup. Reloading is a `{"op":"reload"}` request —
//! the daemon does not watch the filesystem (that is the off-by-default `watch`
//! feature), because a planner that reparses under a client mid-plan would invalidate
//! that client's commit token without it ever having asked for a reload.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use freshet_serve::{http, ProjectCache, Request, Response};

#[derive(Debug, Parser)]
#[command(
    name = "freshet-serve",
    version,
    about = "Serve the backfill planner over HTTP: what a change rebuilds, what it will cost, and the queue to run it."
)]
struct Cli {
    /// A directory of `.sql` models. Each file's stem is the model name.
    #[arg(long, value_name = "DIR")]
    project: Option<PathBuf>,

    /// A JSON file of source-table schemas: `{"raw_orders": ["id", "amount"]}`.
    ///
    /// Without it, columns of tables the project selects from but does not define
    /// cannot be resolved, and the planner falls back to its conservative behaviour
    /// (an unresolved column means "assume it changed").
    #[arg(long, value_name = "FILE")]
    sources: Option<PathBuf>,

    /// A JSON file of materializations: `{"events": "incremental"}`.
    ///
    /// Anything unnamed is a `table`. This is what makes partition scoping reachable:
    /// only `incremental` models are partition-scopable, so a project with no hints
    /// plans every rebuild as a whole model.
    #[arg(long, value_name = "FILE")]
    materializations: Option<PathBuf>,

    /// A JSON file of per-model incremental declarations, in the shape
    /// `freshet_incremental::IncrementalConfig` deserialises.
    ///
    /// Only the `replace` half is read here — the daemon plans through
    /// `plan_backfill`, not `plan_incremental` — and omitting it resolves every model
    /// to no replace op, which is the pre-v3 behaviour exactly. It is a startup flag
    /// as well as a `reload` field because a daemon that can only be given this by a
    /// client cannot be *started* on a v3 project.
    #[arg(long, value_name = "FILE")]
    incremental: Option<PathBuf>,

    #[arg(long, default_value_t = 8788)]
    port: u16,

    /// The address to bind. Defaults to loopback: there is no authentication in this
    /// version, and binding anything else prints a warning saying what that means.
    #[arg(long, default_value = "127.0.0.1")]
    host: IpAddr,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let cache = Arc::new(ProjectCache::new());

    if let Some(dir) = &cli.project {
        let models = read_models(dir)?;
        let count = models.len();
        let sources = match &cli.sources {
            Some(p) => serde_json::from_str(&std::fs::read_to_string(p)?)?,
            None => Default::default(),
        };
        let materializations: BTreeMap<String, String> = match &cli.materializations {
            Some(p) => serde_json::from_str(&std::fs::read_to_string(p)?)?,
            None => BTreeMap::new(),
        };
        let incremental = match &cli.incremental {
            Some(p) => serde_json::from_str(&std::fs::read_to_string(p)?)?,
            None => Default::default(),
        };
        // Fail at startup rather than on the first request: a daemon that accepts a
        // connection is claiming it has a project, and a bad `--project` is an
        // operator error worth exiting for.
        match cache.handle(Request::Reload {
            models,
            sources,
            materializations,
            incremental,
        }) {
            Response::Reloaded { models, .. } => {
                eprintln!("loaded {models} model(s) from {}", dir.display());
            }
            Response::Error { message } => anyhow::bail!("loading {}: {message}", dir.display()),
            other => anyhow::bail!("unexpected reload response: {other:?}"),
        }
        debug_assert_eq!(count, cache_len(&cache));
    } else {
        // These three are read only inside the branch above. Without `--project` they are
        // silently inert, and a silently ignored `--incremental` is the worst of the three:
        // the daemon then plans with no replace ops at all, which looks like a correct plan
        // for a project that declared none.
        if cli.incremental.is_some() || cli.sources.is_some() || cli.materializations.is_some() {
            eprintln!(
                "warning: --incremental, --sources and --materializations are read only \
                 alongside --project, so they are being ignored. Send them on the `reload` \
                 request instead."
            );
        }
        eprintln!("no --project given; send a `{{\"op\":\"reload\"}}` request to load one");
    }

    let addr = SocketAddr::new(cli.host, cli.port);
    http::warn_if_exposed(addr);
    eprintln!("freshet-serve listening on http://{addr}");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(http::serve(cache, addr))?;
    Ok(())
}

/// How many models the cache holds, via the public surface (the graph order).
fn cache_len(cache: &ProjectCache) -> usize {
    match cache.handle(Request::Graph) {
        Response::Graph { order, .. } => order.len(),
        _ => 0,
    }
}

/// Read `*.sql` from `dir`, non-recursively, as `(model name, SQL)` pairs.
///
/// Non-recursive on purpose: nested directories in dbt-shaped projects carry meaning
/// (staging/marts/…) that this daemon has no model for, and flattening them would
/// silently collide two models named `orders.sql` in different folders. A project
/// laid out that way should be described by a `reload` request, which names each
/// model explicitly.
fn read_models(dir: &Path) -> anyhow::Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    let entries = std::fs::read_dir(dir)
        .map_err(|e| anyhow::anyhow!("reading project directory {}: {e}", dir.display()))?;
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|s| s.to_str()) else {
            anyhow::bail!("model file name is not valid UTF-8: {}", path.display());
        };
        out.push((name.to_string(), std::fs::read_to_string(&path)?));
    }
    if out.is_empty() {
        anyhow::bail!("no .sql files in {}", dir.display());
    }
    // Deterministic order: `read_dir` yields in filesystem order, which differs
    // between machines, and the project's model order reaches the plan's output.
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}
