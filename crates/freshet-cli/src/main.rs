//! planner — one-shot CLI over the backfill planner.
//!
//! Loads a project from a directory of `*.sql` files (model name = file stem), an
//! optional `sources.json` (`{ "raw": ["a","b"] }`) for source-table schemas, and an
//! optional `materializations.json` (`{ "events": "incremental" }`) for the
//! materialization hints that make partition scoping reachable.
//!
//! ```text
//! freshet plan        --project ./models --state .planner/state.json [--commit] [--json] [--sql postgres]
//! freshet plan        --project ./models --state https://edge.example/state/v1 [--commit [--expect-version N]]
//! freshet restate     --project ./models --model events --partitions 2026-06-20,2026-06-21 [--json] [--sql bigquery]
//! freshet fingerprint --project ./models      # AST-canonical fingerprints + text fallbacks
//! freshet graph       --project ./models      # topological model order
//! freshet submit      --project ./models --to <url> --command 'dbt run --select {{ model }}'
//! freshet move        --project ./models --from postgres --to snowflake --stage s3://… [--partitions …] [--json]
//! ```
//!
//! Two things open a socket, both behind default-on features and both to a URL the
//! caller supplies in full. `submit` posts the same JSON `plan --json` prints — see
//! [`submit`] for what it refuses to know about the thing on the other end. And
//! `--state` may name **hosted state** (`http(s)://…`, token in `FRESHET_STATE_TOKEN`)
//! instead of a file: the same snapshot, versioned, committed by compare-and-swap —
//! see [`state`] and [`hosted`].
//!
//! `--sql DIALECT` renders each planned model's write as SQL (`freshet-render`): the
//! statements are printed, carried on `--json` as contract v4, and posted by `submit`.
//! Freshet still executes nothing — the SQL is for whoever runs the plan.
//!
//! `plan` wires Freshet's **AST-canonical fingerprint** ([`freshet_fingerprint`])
//! into the planner core as a drop-in for `freshet_state::fingerprint_project`, so
//! cosmetic SQL edits don't trigger rebuilds. State persists via freshet-state's
//! `FileStateBackend`.

#[cfg(feature = "submit")]
mod submit;
mod explain;
mod migrate;
#[cfg(feature = "hosted")]
mod hosted;
mod state;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use freshet_core::{CostModel, Materialization, Project};
use freshet_embed::{build_project_with_hints, to_wire, to_wire_with_estimate_and_budget};
use freshet_fingerprint::AstFingerprinter;
use freshet_incremental::IncrementalConfig;
use freshet_sql::Catalog;
use freshet_state::StateSnapshot;
use state::{ExpectedVersion, StateLocation};

#[derive(Parser)]
#[command(name = "freshet", version, about = "Freshet — what a SQL change must rebuild: models, columns and partitions")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

/// `--sql` and its companions, shared by every subcommand that emits a plan.
#[derive(clap::Args, Clone, Debug, Default)]
struct RenderArgs {
    /// Render each planned model's write as SQL in this dialect:
    /// postgres | snowflake | bigquery | databricks | redshift.
    ///
    /// All or nothing: if any model cannot be rendered (an incremental model with no
    /// declared replace strategy, say) nothing is printed but the reasons.
    #[arg(long = "sql", value_name = "DIALECT")]
    dialect: Option<String>,
    /// Qualify every target relation with this schema (`analytics`, `proj.analytics`).
    #[arg(long, value_name = "SCHEMA", requires = "dialect")]
    schema: Option<String>,
    /// Recreate these models from their SQL even though their definition did not
    /// change — for a relation someone altered by hand. Models whose definition did
    /// change are recreated without being named.
    #[arg(long, value_name = "MODEL,...", value_delimiter = ',', requires = "dialect")]
    recreate: Vec<String>,
}

impl RenderArgs {
    /// Render options for a plan whose redefined models are `redefined` (empty for a
    /// restatement), or `None` if `--sql` was not given.
    fn options(&self, redefined: BTreeSet<String>) -> Result<Option<freshet_render::RenderOptions>> {
        let Some(dialect) = &self.dialect else { return Ok(None) };
        let mut opts = freshet_render::RenderOptions::new(dialect.parse()?);
        opts.schema = self.schema.clone();
        opts.recreate = redefined;
        opts.recreate.extend(self.recreate.iter().cloned());
        Ok(Some(opts))
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Compute the minimal backfill for code changes vs. the last committed state.
    Plan {
        #[arg(long, default_value = ".")]
        project: PathBuf,
        /// Where committed state lives: a state file, or a hosted state API
        /// (`https://<edge>/state/v1`, token in FRESHET_STATE_TOKEN) whose commits are
        /// compare-and-swap.
        #[arg(long, default_value = ".planner/state.json", value_name = "FILE|URL")]
        state: String,
        /// Persist the new snapshot (do this only after the plan actually ran).
        #[arg(long)]
        commit: bool,
        /// Hosted state: commit only if the state is still at this version — the one
        /// the plan you ran was computed against, which `plan` printed — or `none`.
        /// Otherwise exit 3 without writing: another commit landed; re-plan.
        #[arg(long, value_name = "VERSION|none", requires = "commit", value_parser = state::parse_expected_version)]
        expect_version: Option<ExpectedVersion>,
        #[arg(long)]
        json: bool,
        /// Cap this plan at N compute units, and report the headroom left.
        ///
        /// The self-hosted processing budget. Accepts fractional CU (`--budget 2.5`);
        /// the wire and the arithmetic are in milliCU, so this is the one place the
        /// conversion happens. Omit for an uncapped plan, which is the default and
        /// what every deployment that has not opted into a cap gets.
        ///
        /// A plan over the cap still prints what it would have rebuilt — you cannot
        /// scope down what you cannot see — but exits non-zero and **never commits**,
        /// even with `--commit`.
        #[arg(long, value_name = "CU")]
        budget: Option<f64>,
        #[command(flatten)]
        render: RenderArgs,
    },
    /// Plan a data restatement of specific partitions of a model.
    ///
    /// With `--state`, the restated partitions are recorded in the state's ledger as
    /// owed, so a run that dies part-way is re-planned by the next `freshet plan`;
    /// run again with `--commit` once the restatement has succeeded to settle them.
    Restate {
        #[arg(long, default_value = ".")]
        project: PathBuf,
        #[arg(long)]
        model: String,
        #[arg(long, value_delimiter = ',')]
        partitions: Vec<String>,
        #[arg(long)]
        json: bool,
        /// State whose ledger records this restatement — a file, or a hosted state
        /// URL. Omit to plan only.
        #[arg(long, value_name = "FILE|URL")]
        state: Option<String>,
        /// The restatement ran successfully: settle its partitions in the ledger.
        #[arg(long, requires = "state")]
        commit: bool,
        /// Hosted state: write only if the state is still at this version (or `none`);
        /// otherwise exit 3 without writing.
        #[arg(long, value_name = "VERSION|none", requires = "state", value_parser = state::parse_expected_version)]
        expect_version: Option<ExpectedVersion>,
        #[command(flatten)]
        render: RenderArgs,
    },
    /// Show each model's AST-canonical fingerprint and any text-scheme fallbacks.
    Fingerprint {
        #[arg(long, default_value = ".")]
        project: PathBuf,
    },
    /// Print the project's models in topological order.
    Graph {
        #[arg(long, default_value = ".")]
        project: PathBuf,
    },
    /// Show what Freshet would have pruned from an existing dbt project's last run.
    Explain(explain::ExplainArgs),
    /// Move the project to another warehouse, partition by partition: render each
    /// unit's unload, load and parity check for dagron to run. Executes nothing.
    Move(migrate::MoveArgs),
    /// Post the plan to a consumer of the wire contract, which compiles and runs it.
    ///
    /// Never commits state: the run it starts has not finished. Commit with
    /// `plan --commit` once it has.
    #[cfg(feature = "submit")]
    Submit {
        #[arg(long, default_value = ".")]
        project: PathBuf,
        /// As `plan`: a state file or a hosted state URL. Read only — submit never
        /// commits.
        #[arg(long, default_value = ".planner/state.json", value_name = "FILE|URL")]
        state: String,
        /// The consumer's submit endpoint, in full. No default: freshet does not
        /// know who consumes its plans or where they mounted the route.
        #[arg(long, value_name = "URL", required_unless_present = "dry_run")]
        to: Option<String>,
        /// Shorthand for `options.command_template = ["sh", "-c", <SHELL>]`.
        /// Placeholders in SHELL are the consumer's; freshet passes them through.
        #[arg(long, value_name = "SHELL")]
        command: Option<String>,
        /// A JSON object merged into `options`. Forwarded, never interpreted.
        #[arg(long, value_name = "FILE")]
        options_file: Option<PathBuf>,
        /// Leave the project graph out of the request. Smaller body; the consumer
        /// then cannot report what the plan skipped.
        #[arg(long)]
        no_graph: bool,
        /// Print the request body and exit. Posts nothing.
        #[arg(long)]
        dry_run: bool,
        /// Extra request header, repeatable: `--header 'Authorization: Bearer …'`.
        #[arg(long = "header", value_name = "NAME: VALUE")]
        headers: Vec<String>,
        /// Carry each model's SQL in the plan (contract v4), so the consumer can run
        /// it — e.g. through `{{ sql }}` in dagron-state.
        #[command(flatten)]
        render: RenderArgs,
    },
}

fn main() -> Result<()> {
    let result = match Cli::parse().cmd {
        Cmd::Plan { project, state, commit, expect_version, json, budget, render } => {
            cmd_plan(&project, &state, commit, expect_version, json, budget, &render)
        }
        Cmd::Restate { project, model, partitions, json, state, commit, expect_version, render } => {
            cmd_restate(&project, &model, partitions, json, state.as_deref(), commit, expect_version, &render)
        }
        Cmd::Fingerprint { project } => cmd_fingerprint(&project),
        Cmd::Explain(args) => explain::run(&args),
        Cmd::Move(args) => migrate::run(&args),
        Cmd::Graph { project } => cmd_graph(&project),
        #[cfg(feature = "submit")]
        Cmd::Submit { project, state, to, command, options_file, no_graph, dry_run, headers, render } => {
            cmd_submit(SubmitArgs {
                project_dir: &project,
                state: &state,
                to: to.as_deref(),
                command: command.as_deref(),
                options_file: options_file.as_deref(),
                no_graph,
                dry_run,
                headers: &headers,
                render: &render,
            })
        }
    };
    // Hosted state moved under this plan. Not a failure of the program — the
    // compare-and-swap doing its job — so it gets its own exit status (a script
    // re-plans on 3 and gives up on 1) and a plain `error:` line rather than
    // anyhow's report, for the same reason the budget refusal does.
    if let Err(e) = &result {
        if let Some(conflict) = e.downcast_ref::<state::StateConflict>() {
            eprintln!("error: {conflict}");
            std::process::exit(state::EXIT_STATE_CONFLICT);
        }
    }
    result
}

/// Plan the project against committed state.
///
/// Shared by `plan` and `submit` so the two can never disagree about what the state
/// diff produced — a submit that planned differently from the `plan` the operator
/// just read would be the worst kind of surprise.
///
/// v2 drop-in: AST-canonical fingerprints instead of freshet-state's text-normalized
/// `fingerprint_project` — same `BTreeMap<String, u64>` — plus whatever the state's
/// ledger says a crashed restatement still owes, and the declared grains and windows.
/// `previous` is the same snapshot whether it came from a file or hosted state.
fn diff_against_state(project: &Project, incremental: &IncrementalConfig, previous: &StateSnapshot) -> freshet_embed::CodeChangePlan {
    // A scheme upgrade re-keys unchanged models instead of rebuilding them.
    let r = freshet_fingerprint::reconcile(previous, project, &AstFingerprinter::new());
    if !r.rekeyed.is_empty() {
        eprintln!(
            "note: {} model(s) carry fingerprints from an older scheme but are unchanged — \
             re-keyed on the next commit, not rebuilt",
            r.rekeyed.len()
        );
    }
    freshet_embed::plan_code_change(project, previous, r.current, r.changed, incremental)
}

/// Load `--state`, saying which hosted version the plan is computed against.
///
/// The version is the thing to carry to the commit (`--expect-version`), so it is
/// printed every time, on stderr, where `--json` output is not disturbed.
fn load_state(location: &StateLocation, expect: Option<ExpectedVersion>) -> Result<state::Loaded> {
    let loaded = location.load(expect)?;
    if let Some(version) = loaded.hosted_version {
        match version {
            Some(v) => eprintln!("hosted state: version {v} at {}", location.describe()),
            None => eprintln!("hosted state: no commits yet (version none) at {}", location.describe()),
        }
    }
    Ok(loaded)
}

/// ` --expect-version N` for a hosted commit hint; empty for a file.
fn expect_flag(version: Option<String>) -> String {
    version.map(|v| format!(" --expect-version {v}")).unwrap_or_default()
}

/// Convert a `--budget` in whole CU to the milliCU the wire and the gate use.
///
/// Rejects negatives and NaN rather than letting them become a cap: `as u64`
/// saturates a negative to 0, which would silently turn `--budget -1` into "cap
/// everything" — a typo that reads as a working flag. A cap of exactly 0 is allowed
/// and means "admit only an empty plan", which is a legitimate thing to assert in CI.
fn budget_to_millicu(cu: f64) -> Result<u64> {
    if !cu.is_finite() || cu < 0.0 {
        anyhow::bail!("--budget must be a non-negative number of compute units, got {cu}");
    }
    // Round rather than truncate: `--budget 0.1` is 100 mCU, and truncation would
    // make a cap the user typed exactly slightly tighter than what they asked for.
    let millicu = (cu * 1000.0).round();
    // `f64 as u64` **saturates** rather than wrapping or panicking, so without this
    // check `--budget 1.9e16` would quietly become a cap of `u64::MAX` mCU — and the
    // headroom line would then report, and the gate enforce, a limit the caller never
    // asked for. Rejected rather than clamped, for the same reason the negative case
    // above is rejected: a cap that is silently not the requested one is worse than
    // no cap, because it reads as enforcement.
    if millicu > u64::MAX as f64 {
        anyhow::bail!(
            "--budget {cu} is too large to express in milliCU (the ceiling is about \
             1.8e16 CU); if you meant \"no cap\", omit the flag"
        );
    }
    Ok(millicu as u64)
}

fn cmd_plan(
    project_dir: &Path,
    state_arg: &str,
    commit: bool,
    expect: Option<ExpectedVersion>,
    json: bool,
    budget: Option<f64>,
    render: &RenderArgs,
) -> Result<()> {
    let Loaded { project, incremental, .. } = load_project(project_dir)?;
    let location = StateLocation::parse(state_arg)?;
    let prior = load_state(&location, expect)?;
    let freshet_embed::CodeChangePlan { plan, redefined, next_state } =
        diff_against_state(&project, &incremental, &prior.snapshot);

    let budget_millicu = budget.map(budget_to_millicu).transpose()?;
    // One call produces both the JSON and the headroom, so the number printed to a
    // human and the number a script parses out of `--json` are the same number.
    let mut resp = to_wire_with_estimate_and_budget(
        &project,
        &plan,
        &incremental,
        &CostModel::default(),
        false,
        false,
        budget_millicu,
    );
    let headroom = resp.headroom.clone();
    let over_budget = headroom.as_ref().is_some_and(|h| h.exceeded);

    // Render before printing anything, so an unrenderable plan prints its reasons and
    // not a plan that looks runnable. A refused plan is not rendered — the same rule
    // the wire applies — and a render failure never commits.
    if !over_budget {
        if let Some(opts) = render.options(redefined)? {
            if let Err(e) = freshet_embed::attach_sql(&mut resp, &project, &plan, &incremental, &opts) {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&resp)?);
    } else {
        print_plan_human(&project, &plan, &incremental);
        if let Some(h) = &headroom {
            print_headroom(h);
        }
        print_sql_human(&resp);
    }

    // The interlock, and the reason `--commit` is checked *after* the gate rather
    // than being rejected as a flag combination: `plan --budget N --commit` is a
    // perfectly sensible thing to run in CI every day, and it should commit on the
    // days the plan fits. It is only the over-budget day that must not write — a
    // commit there would record "everything is built" over models the operator was
    // just told they may not rebuild.
    if commit && !over_budget {
        if prior.hosted_version.is_some() && expect.is_none() {
            eprintln!(
                "note: committing against hosted state version {} as loaded just now; if the \
                 plan that ran was computed earlier, pass the version it printed with \
                 --expect-version so a commit that landed in between is caught",
                state::version_arg(prior.hosted_version.flatten())
            );
        }
        let context = state::commit_context(
            "plan --commit",
            serde_json::json!({ "models_rebuilt": plan.models.len() }),
        );
        let wrote = location.commit(&prior, &next_state, context)?;
        eprintln!("{}", wrote.message());
    } else if !commit && !over_budget && !plan.is_empty() {
        if let Some(version) = prior.expect_version_arg() {
            eprintln!(
                "once this plan has run, commit it with:\n  freshet plan --project {} --state {} --commit{}",
                project_dir.display(),
                location.describe(),
                expect_flag(Some(version)),
            );
        }
    }

    if over_budget {
        let h = headroom.expect("over_budget implies headroom");
        if commit {
            eprintln!("not committing: the plan was refused, and nothing has been rebuilt");
        }
        eprintln!(
            "error: plan needs {} but the budget is {} — scope the change down, restate \
             fewer partitions, or raise --budget",
            fmt_cu(h.used_millicu),
            fmt_cu(h.cap_millicu),
        );
        // Exit directly rather than `bail!`ing into `main`'s `anyhow::Result`.
        //
        // Being over budget is an expected operational outcome on a capped build — a
        // team can hit it on any Monday — not a failure of the program. Returned as
        // an `anyhow::Error` it acquires the `Error:` prefix and, wherever
        // `RUST_BACKTRACE` is set (which CI runners commonly do), a stack backtrace
        // through `cmd_plan`. A quota message that reads like a crash trains people
        // to ignore crashes.
        std::process::exit(1);
    }
    Ok(())
}

/// The north-star metric for one plan, at the default rate card: what it costs, what
/// rebuilding every model downstream of the change whole would cost, and the
/// difference — the rebuild work pruning avoided (never below zero).
fn rebuild_work_line(project: &Project, plan: &freshet_core::BackfillPlan) -> String {
    let rate = CostModel::default();
    let planned = freshet_core::estimate_compute_units(project, plan, &rate).total.0;
    let unpruned = freshet_core::estimate_unpruned(project, plan, &rate).total.0;
    let avoided = unpruned.saturating_sub(planned);
    let pct = if unpruned == 0 { 0.0 } else { avoided as f64 / unpruned as f64 * 100.0 };
    let cu = |millicu: u64| format!("{:.3} CU", millicu as f64 / 1000.0);
    format!(
        "rebuild work: {} planned, {} ({pct:.0}%) avoided — rebuilding everything \
         downstream of the change whole would be {}",
        cu(planned),
        cu(avoided),
        cu(unpruned)
    )
}

/// milliCU as the whole-CU figure a human reads, with the exact integer beside it.
///
/// The wire carries only `millicu` and leaves the display conversion to the edge —
/// this is that edge. Both numbers are shown because the CU figure is rounded and
/// the mCU one is what the gate actually compared.
fn fmt_cu(millicu: u64) -> String {
    format!("{:.3} CU ({millicu} mCU)", millicu as f64 / 1000.0)
}

/// The headroom line, and a bar so a trend is visible at a glance across runs.
fn print_headroom(h: &freshet_embed::WireHeadroom) {
    println!();
    if h.exceeded {
        println!(
            "budget: OVER by {} — used {} of {}",
            fmt_cu(h.used_millicu - h.cap_millicu),
            fmt_cu(h.used_millicu),
            fmt_cu(h.cap_millicu),
        );
        return;
    }
    // Percentages are derived here rather than carried on the wire, for the same
    // reason whole CU is. Guard the zero cap: `--budget 0` admits only an empty
    // plan, and 0/0 is not 0%.
    let pct = if h.cap_millicu == 0 {
        100.0
    } else {
        h.used_millicu as f64 / h.cap_millicu as f64 * 100.0
    };
    const WIDTH: usize = 24;
    let filled = ((pct / 100.0) * WIDTH as f64).round().min(WIDTH as f64) as usize;
    println!(
        "budget: {} left of {} — used {} ({pct:.1}%)",
        fmt_cu(h.remaining_millicu),
        fmt_cu(h.cap_millicu),
        fmt_cu(h.used_millicu),
    );
    println!("        [{}{}]", "#".repeat(filled), "-".repeat(WIDTH - filled));
}

/// Everything `submit` takes. A struct because clippy is right that eight
/// positional parameters is a trap, and because naming them at the call site is
/// the difference between reading the dispatch arm and decoding it.
#[cfg(feature = "submit")]
struct SubmitArgs<'a> {
    project_dir: &'a Path,
    state: &'a str,
    to: Option<&'a str>,
    command: Option<&'a str>,
    options_file: Option<&'a Path>,
    no_graph: bool,
    dry_run: bool,
    headers: &'a [String],
    render: &'a RenderArgs,
}

/// `submit` — plan, then hand the plan to whoever runs it.
///
/// The order matters: an empty plan returns success without opening a socket. A
/// caller planning on every commit hits that case constantly, and making it a
/// network round-trip (or worse, an error) would make `submit` unusable in the
/// loop it exists for.
#[cfg(feature = "submit")]
fn cmd_submit(args: SubmitArgs<'_>) -> Result<()> {
    let Loaded { project, incremental, .. } = load_project(args.project_dir)?;
    let location = StateLocation::parse(args.state)?;
    let prior = load_state(&location, None)?;
    let freshet_embed::CodeChangePlan { plan, redefined, .. } =
        diff_against_state(&project, &incremental, &prior.snapshot);

    // An empty plan opens no socket. But `--dry-run` promises the request body on
    // stdout, and the README pipes that straight into `dagron-state explain -`, so
    // it must stay JSON even when there is nothing to rebuild — prose there is a
    // parse error for whoever is downstream. The human note goes to stderr instead.
    if plan.is_empty() {
        if !args.dry_run {
            println!("nothing to backfill — state is up to date. Submitted nothing.");
            return Ok(());
        }
        eprintln!("nothing to backfill — state is up to date; the body below is empty.");
    }

    // The plan carries its own effort map (`cost`, the CU estimate at the default
    // rate card): part of the planner's contract, not the consumer's, and what a
    // consumer that runs plans on compute it supplies — dagron Cloud, for a workspace
    // in hosted compute mode — meters. A consumer that does not read it ignores it
    // (dagron-state does not mirror `cost` at all).
    let mut wire = freshet_embed::to_wire_with_estimate(&project, &plan, &incremental, &CostModel::default(), true, false);
    if let Some(opts) = args.render.options(redefined)? {
        freshet_embed::attach_sql(&mut wire, &project, &plan, &incremental, &opts)?;
    }
    let envelope = submit::Envelope {
        plan: wire,
        graph: (!args.no_graph).then(|| submit::project_graph(&project)),
        options: submit::build_options(args.command, args.options_file)?,
    };
    let body = serde_json::to_string_pretty(&envelope).context("serializing the request")?;

    if args.dry_run {
        println!("{body}");
        return Ok(());
    }

    // clap enforces this (`required_unless_present`); the check is here so the
    // invariant survives someone calling cmd_submit from a test or a future caller.
    let url = args.to.context("--to is required unless --dry-run")?;
    let reply = submit::post(url, args.headers, &body)?;

    if !(200..300).contains(&reply.status) {
        anyhow::bail!("{url} refused the plan (HTTP {}): {}", reply.status, reply.body.trim());
    }

    let n = plan.models.len();
    match reply.run_id() {
        Some(id) => println!("submitted {n} model(s) -> run {id}"),
        // Not an error: the response schema belongs to the consumer, not to us.
        None => println!("submitted {n} model(s); HTTP {} — {}", reply.status, reply.body.trim()),
    }
    eprintln!(
        "state not committed: the run has not finished. Once it has:\n  \
         freshet plan --project {} --state {} --commit{}",
        args.project_dir.display(),
        location.describe(),
        expect_flag(prior.expect_version_arg()),
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)] // one per flag; the dispatch arm names them all
fn cmd_restate(
    project_dir: &Path,
    model: &str,
    partitions: Vec<String>,
    json: bool,
    state_arg: Option<&str>,
    commit: bool,
    expect: Option<ExpectedVersion>,
    render: &RenderArgs,
) -> Result<()> {
    let Loaded { project, incremental, .. } = load_project(project_dir)?;
    // Parsed before planning, so a bad URL or a missing token fails before the plan
    // is printed as though it will be recorded.
    let location = state_arg.map(StateLocation::parse).transpose()?;
    let partition_count = partitions.len();
    // Honours declared grains and lookback windows (freshet-incremental); with none
    // declared it is the planner core's identity-mapped restatement exactly.
    let plan = freshet_embed::plan_restatement(&project, &incremental, model, partitions)
        .context("planning restatement")?;
    let mut resp = to_wire(&plan, &project, &incremental);
    // A restatement rewrites data, never definitions: nothing is recreated unless
    // named with --recreate.
    if let Some(opts) = render.options(BTreeSet::new())? {
        if let Err(e) = freshet_embed::attach_sql(&mut resp, &project, &plan, &incremental, &opts) {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
    if json {
        println!("{}", serde_json::to_string_pretty(&resp)?);
    } else {
        print_plan_human(&project, &plan, &incremental);
        print_sql_human(&resp);
    }

    // The ledger. Recording what is owed is written *before* the run — it only ever
    // adds work, so a run that dies is re-planned rather than forgotten. Settling is
    // written only when the operator says the run succeeded. Against hosted state
    // each write is a compare-and-swap over the version loaded here.
    if let Some(location) = &location {
        let prior = load_state(location, expect)?;
        let mut snap = prior.snapshot.clone();
        let detail = serde_json::json!({
            "model": model,
            "partitions": partition_count,
            "models": plan.models.len(),
        });
        if commit {
            freshet_incremental::record_built(&mut snap, &incremental, freshet_incremental::Grain::Day, &plan);
            let wrote = location
                .commit(&prior, &snap, state::commit_context("restate --commit", detail))
                .context("settling the restatement")?;
            if wrote.unchanged {
                eprintln!("{}", wrote.message());
            } else {
                eprintln!("settled {} model(s) in the ledger -> {}", plan.models.len(), wrote.to);
            }
        } else {
            freshet_incremental::record_pending(&mut snap, &incremental, freshet_incremental::Grain::Day, &plan);
            let wrote = location
                .commit(&prior, &snap, state::commit_context("restate", detail))
                .context("recording the restatement")?;
            eprintln!(
                "recorded as owed in {} — the next `freshet plan` re-plans it until you settle it with:\n  \
                 freshet restate --project {} --model {model} --partitions ... --state {} --commit{}",
                wrote.to,
                project_dir.display(),
                location.describe(),
                expect_flag(wrote.hosted_version.map(|v| state::version_arg(Some(v)))),
            );
        }
    }
    Ok(())
}

/// Each model's SQL as a script a human can read — and paste: every statement is
/// terminated, and a model header is a SQL comment.
fn print_sql_human(resp: &freshet_embed::PlanResponse) {
    let mut first = true;
    for m in &resp.models {
        let Some(sql) = &m.sql else { continue };
        if first {
            println!();
            first = false;
        }
        let caveat = if sql.atomic { "" } else { " — NOT atomic: a failure part-way leaves it half-written" };
        println!("-- {} ({}){caveat}", m.name, sql.dialect);
        for s in &sql.statements {
            println!("{s};");
        }
        println!();
    }
}

fn cmd_fingerprint(project_dir: &Path) -> Result<()> {
    let Loaded { project, .. } = load_project(project_dir)?;
    let report = AstFingerprinter::new().report(&project);
    for (name, fp) in &report.per_model {
        println!("{name:<24} {:016x}  [{:?}]", fp.fingerprint, fp.scheme);
    }
    let fallbacks: Vec<&String> = report.text_fallbacks().collect();
    if !fallbacks.is_empty() {
        eprintln!("\n{} model(s) fell back to the text scheme (SQL did not parse):", fallbacks.len());
        for f in fallbacks {
            eprintln!("  - {f}");
        }
    }
    Ok(())
}

fn cmd_graph(project_dir: &Path) -> Result<()> {
    let Loaded { project, .. } = load_project(project_dir)?;
    match project.topo_order() {
        Ok(order) => {
            for n in order {
                println!("{n}");
            }
            Ok(())
        }
        Err(cycle) => anyhow::bail!("project has a dependency cycle involving: {}", cycle.join(", ")),
    }
}

fn print_plan_human(project: &Project, plan: &freshet_core::BackfillPlan, cfg: &IncrementalConfig) {
    if plan.is_empty() {
        println!("nothing to backfill — state is up to date.");
        return;
    }
    let replace = freshet_incremental::replace::resolve_plan(project, cfg, plan);
    println!("backfill plan ({} model(s), topological order):", plan.models.len());
    for name in &plan.models {
        let unit = match plan.unit.get(name) {
            Some(freshet_core::BackfillUnit::Partitions(ps)) => format!("partitions[{}]", ps.join(",")),
            _ => "full".to_string(),
        };
        let why = match plan.reasons.get(name) {
            Some(freshet_core::BackfillReason::DirectlyChanged) => "directly changed".to_string(),
            Some(freshet_core::BackfillReason::Downstream { because_of, via_columns }) => {
                format!("downstream of {because_of} via [{}]", via_columns.join(","))
            }
            None => "selected".to_string(),
        };
        println!("  {name:<24} {unit:<28} ({why})");
        if let Some(op) = replace.get(name) {
            println!("  {:<24} └─ writes: {}", "", describe_replace(op));
        }
    }
    println!("{}", rebuild_work_line(project, plan));

    // Widenings are the expensive surprise, so they get their own block rather
    // than only a line buried per model: an operator scanning a 40-model plan
    // should not have to spot them.
    let widened: Vec<(&String, &freshet_incremental::ReplaceOp)> =
        replace.iter().filter(|(_, op)| op.is_widened()).collect();
    if !widened.is_empty() {
        eprintln!(
            "\n{} model(s) widened to a full refresh of the whole relation:",
            widened.len()
        );
        for (name, op) in widened {
            eprintln!("  {name:<24} {}", describe_widening(op.widened.as_ref().expect("widened")));
        }
    }
}

fn describe_replace(op: &freshet_incremental::ReplaceOp) -> String {
    use freshet_incremental::{ReplaceStrategy as S, ReplaceTarget};
    let strategy = match op.strategy {
        S::InsertOverwrite => "insert_overwrite",
        // The config spelling, not a prettier one: an operator reading this line
        // should be able to copy the word straight into `incremental.json`.
        S::DeleteInsert => "delete_insert",
        S::Merge => "merge",
        S::ReplaceWhere => "replace_where",
        S::FullRefresh => "full_refresh",
    };
    let scope = match &op.target {
        ReplaceTarget::Whole => "the whole relation".to_string(),
        ReplaceTarget::Partitions(ps) => match &op.partition_column {
            Some(col) => format!("{} partition(s) on `{col}`", ps.len()),
            None => format!("{} partition(s)", ps.len()),
        },
    };
    let key = if op.unique_key.is_empty() {
        String::new()
    } else {
        format!(" on key [{}]", op.unique_key.join(","))
    };
    format!("{strategy} over {scope}{key}")
}

fn describe_widening(w: &freshet_incremental::Widening) -> String {
    use freshet_incremental::Widening as W;
    match w {
        W::NotIncremental { materialization } => {
            format!("the model is a {materialization}, so it has no partitions to replace in place")
        }
        W::WholeModelInPlan => "the plan rebuilds this model in full, so there is nothing to scope to".into(),
        W::NoPartitionColumn { declared } => {
            format!("{declared:?} needs a partition_column and none is declared")
        }
        W::NoUniqueKey => "merge needs a unique_key and none is declared".into(),
    }
}

/// Load a project directory: `*.sql` models + optional `sources.json` +
/// optional `materializations.json` + optional `incremental.json`.
fn load_project(dir: &Path) -> Result<Loaded> {
    let mut models: Vec<(String, String)> = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading project dir {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("sql") {
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .context("non-utf8 model filename")?
                .to_string();
            let sql = std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
            models.push((name, sql));
        }
    }
    models.sort();

    let sources: Catalog = read_json_map(&dir.join("sources.json"))?.unwrap_or_default();
    let hints = read_materializations(&dir.join("materializations.json"))?;

    let project = build_project_with_hints(&models, &sources, &hints).context("building project from SQL")?;
    let incremental = read_incremental(&dir.join("incremental.json"))?;
    Ok(Loaded { project, incremental })
}

/// Everything a project directory yields that a caller still needs.
///
/// A struct rather than a tuple because it grew a second member and every call
/// site wants a different subset; positional `_`s would have made the next reader
/// count commas. The materialization hints are deliberately absent: they are
/// consumed by `build_project_with_hints` and were already discarded at every
/// call site when this returned a tuple.
struct Loaded {
    project: Project,
    incremental: IncrementalConfig,
}

/// Read `incremental.json`: per-model `grain`, per-edge lookback `edges`, and the
/// `replace` declaration. All three are applied.
fn read_incremental(path: &Path) -> Result<IncrementalConfig> {
    Ok(read_json_map::<IncrementalConfig>(path)?.unwrap_or_default())
}

fn read_json_map<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

fn read_materializations(path: &Path) -> Result<BTreeMap<String, Materialization>> {
    let raw: Option<BTreeMap<String, String>> = read_json_map(path)?;
    let mut out = BTreeMap::new();
    for (name, s) in raw.unwrap_or_default() {
        let mat = match s.to_ascii_lowercase().as_str() {
            "table" => Materialization::Table,
            "view" => Materialization::View,
            "incremental" => Materialization::Incremental,
            other => anyhow::bail!("{}: unknown materialization `{other}`", path.display()),
        };
        out.insert(name, mat);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_budget_converts_to_millicu_by_rounding() {
        assert_eq!(budget_to_millicu(5.0).unwrap(), 5_000);
        assert_eq!(budget_to_millicu(0.1).unwrap(), 100, "0.1 CU is 100 mCU, not 0");
        assert_eq!(budget_to_millicu(2.5).unwrap(), 2_500);
        assert_eq!(budget_to_millicu(0.0).unwrap(), 0, "a zero cap is legal: admit only an empty plan");
    }

    #[test]
    fn a_budget_that_is_not_a_quantity_is_rejected_by_name() {
        // `as u64` saturates a negative to 0, which would turn a typo into the
        // tightest possible cap while reading like a working flag.
        for bad in [-1.0, -0.001, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let err = budget_to_millicu(bad).unwrap_err().to_string();
            assert!(err.contains("non-negative"), "got: {err}");
        }
    }

    /// The saturating-cast case: `f64 as u64` clamps to `u64::MAX` instead of
    /// failing, so an absurd budget would silently become a cap of 1.8e19 mCU and
    /// the headroom line would report a limit nobody asked for.
    #[test]
    fn a_budget_too_large_for_millicu_is_rejected_rather_than_clamped() {
        for bad in [1e30, 1.9e16, f64::MAX] {
            let err = budget_to_millicu(bad).unwrap_err().to_string();
            assert!(err.contains("too large"), "got: {err}");
        }
        // Just under the ceiling still converts exactly, so the guard is not
        // rejecting budgets anyone could legitimately want.
        assert_eq!(budget_to_millicu(9.3e15).unwrap(), 9_300_000_000_000_000_000);
    }
}
