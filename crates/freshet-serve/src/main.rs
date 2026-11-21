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
//! The models are loaded once at startup. Reloading is a `{"op":"reload"}` request, or,
//! with `--watch` (the `watch` feature), the files changing on disk: opted into,
//! because a watched edit is a reload, and a plan staged before it can no longer commit.
//!
//! Set `FRESHET_SERVE_TOKEN` and `/rpc` requires `Authorization: Bearer <token>`.

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use freshet_serve::{http, ProjectCache, ProjectFiles, Response};

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

    /// Reload from disk whenever `--project`'s `.sql` files (or the `--sources`,
    /// `--materializations`, `--incremental` files) change, once they are quiet. A plan
    /// staged before an edit a plan would see can then no longer commit: the stale
    /// guard rejects it, as it would after any reload.
    #[cfg(feature = "watch")]
    #[arg(long, requires = "project")]
    watch: bool,

    #[arg(long, default_value_t = 8788)]
    port: u16,

    /// The address to bind. Defaults to loopback. Anywhere else, set
    /// `FRESHET_SERVE_TOKEN` so `/rpc` requires `Authorization: Bearer <token>`;
    /// without it the daemon warns that anyone who can reach it can use it.
    #[arg(long, default_value = "127.0.0.1")]
    host: IpAddr,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let token = match std::env::var(http::TOKEN_ENV) {
        Ok(t) if t.trim().is_empty() => anyhow::bail!("{} is set but empty", http::TOKEN_ENV),
        Ok(t) => Some(t),
        Err(std::env::VarError::NotPresent) => None,
        Err(e) => anyhow::bail!("{}: {e}", http::TOKEN_ENV),
    };
    let cache = Arc::new(ProjectCache::new());

    let files = cli.project.as_ref().map(|dir| ProjectFiles {
        dir: dir.clone(),
        sources: cli.sources.clone(),
        materializations: cli.materializations.clone(),
        incremental: cli.incremental.clone(),
    });
    if let Some(files) = &files {
        // Fail at startup rather than on the first request: a daemon that accepts a
        // connection is claiming it has a project, and a bad `--project` is an
        // operator error worth exiting for.
        let dir = files.dir.display();
        match cache.handle(files.reload_request().map_err(|e| anyhow::anyhow!("loading {dir}: {e:#}"))?) {
            Response::Reloaded { models, .. } => eprintln!("loaded {models} model(s) from {dir}"),
            Response::Error { message } => anyhow::bail!("loading {dir}: {message}"),
            other => anyhow::bail!("unexpected reload response: {other:?}"),
        }
    } else {
        // These three are read only alongside `--project`. Without it they are
        // silently inert, and a silently ignored `--incremental` is the worst of the
        // three: the daemon then plans with no replace ops at all, which looks like a
        // correct plan for a project that declared none.
        if cli.incremental.is_some() || cli.sources.is_some() || cli.materializations.is_some() {
            eprintln!(
                "warning: --incremental, --sources and --materializations are read only \
                 alongside --project, so they are being ignored. Send them on the `reload` \
                 request instead."
            );
        }
        eprintln!("no --project given; send a `{{\"op\":\"reload\"}}` request to load one");
    }

    // Held until the server returns: dropping it stops the watch.
    #[cfg(feature = "watch")]
    let _watching = match (&files, cli.watch) {
        (Some(files), true) => {
            let dir = files.dir.display().to_string();
            eprintln!("watching {dir} for changes");
            Some(freshet_serve::watch::start(
                cache.clone(),
                files.clone(),
                freshet_serve::watch::QUIET,
                move |response| match response {
                    Response::Reloaded { revision, models, reparsed, .. } => {
                        eprintln!("{dir} changed: reloaded {models} model(s), reparsed {reparsed}, revision {revision}")
                    }
                    Response::Error { message } => {
                        eprintln!("{dir} changed, and the reload failed (still serving the previous project): {message}")
                    }
                    other => eprintln!("{dir} changed: unexpected reload response {other:?}"),
                },
            )?)
        }
        (None, true) => anyhow::bail!("--watch needs --project: there is nothing on disk to watch"),
        (_, false) => None,
    };

    let addr = SocketAddr::new(cli.host, cli.port);
    http::warn_if_exposed(addr, token.is_some());
    let auth = if token.is_some() { " (/rpc requires the bearer token)" } else { "" };
    eprintln!("freshet-serve listening on http://{addr}{auth}");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(http::serve(cache, addr, token))?;
    Ok(())
}
