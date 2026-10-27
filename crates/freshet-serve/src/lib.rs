//! freshet-serve — the long-running planner daemon (core).
//!
//! A planner brain is cheap to call but a project is expensive to parse, so an
//! editor/CI loop wants the graph kept **warm** and only the *changed* models
//! reparsed. This crate holds an `Arc<Project>` in a cache and exposes an in-process
//! [`ProjectCache::handle`] (and [`ProjectCache::handle_json`]) request loop. The
//! default lane is **serde-only and transport-free**; the `http` feature adds an
//! axum daemon ([`http`]) over that same funnel, so the in-process and networked
//! lanes cannot answer differently. gRPC and file-watch remain off-by-default
//! roadmap features.
//!
//! ## Incremental reparse
//!
//! A [`Request::Reload`] carries the whole project but re-analyzes only the models
//! whose SQL changed plus their downstream closure, in topological order, and carries
//! every other model's lineage over from the previous reload. Wherever it cannot show
//! that the result is exactly what a full `build_project` would produce, it runs the
//! full build instead: on the first load, when the model names or the sources changed,
//! when a changed model does not parse or the edit made a cycle, and when a model that
//! reads a model outside its `depends_on` could see the edit. Over-rebuilding is safe
//! and an incremental project that differs from a full build is an under-rebuild, so
//! the fallback wins every doubt; a differential test (random projects, random edits,
//! each reload checked against a from-scratch build) is what gates the path.
//! Materialization hints and the incremental config never need a reparse — they are
//! re-applied or stored. [`Response::Reloaded`] reports how many models were reparsed,
//! and whether (and why) the full build ran.
//!
//! ## The plan -> commit handoff (correct across the request boundary)
//!
//! The planner core's discipline is "state advances only after a successful run". Across a
//! daemon that means: [`Request::Plan`] stages the post-run snapshot under a **token**
//! stamped with the project **revision**; the client runs the plan, then sends
//! [`Request::Commit`] with that token. If the project changed in between (its
//! revision moved), the token is **stale** and rejected — the client must re-plan.
//! This prevents a lost update where a plan computed against an old graph commits over
//! a newer one.
//!
//! The revision names the state plans are computed from — the project and the
//! incremental config — not the request that delivered it. A reload that changes
//! neither, such as an editor saving a file it did not edit, keeps the revision, so it
//! does not throw away a plan that is still exactly right.
//!
//! Staged plans are bounded twice: at most [`MAX_STAGED_PLANS`] at a time, oldest
//! evicted first, and none kept past a TTL ([`DEFAULT_STAGED_PLAN_TTL`] unless
//! [`ProjectCache::with_staged_plan_ttl`] says otherwise). A commit of an evicted token
//! is told it was evicted and why, which is a different answer from stale. The TTL
//! reads an injectable [`Clock`], so its tests advance time rather than sleep.

mod reparse;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use freshet_core::{plan_backfill, CostModel, Materialization, Project};
use freshet_embed::{to_wire_with_estimate, PlanResponse};
use freshet_fingerprint::AstFingerprinter;
use freshet_incremental::IncrementalConfig;
use freshet_sql::Catalog;
use freshet_state::{changed_models, MemoryStateBackend, StateSnapshot};
use serde::{Deserialize, Serialize};

use reparse::Built;

/// A request to the daemon.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// Replace the project. Only the models whose SQL changed, and their downstream
    /// closure, are re-analyzed; the crate docs say when the full build runs instead.
    Reload {
        models: Vec<(String, String)>,
        #[serde(default)]
        sources: Catalog,
        #[serde(default)]
        materializations: BTreeMap<String, String>,
        /// Per-model incremental declarations; only the `replace` half is read
        /// (the daemon plans through `plan_backfill`, not `plan_incremental`).
        /// Defaulted, so a pre-v3 caller's `Reload` is unchanged.
        #[serde(default)]
        incremental: IncrementalConfig,
    },
    /// Plan code changes vs. committed state; returns a commit token.
    ///
    /// The two estimate flags mirror `freshet_embed::PlanRequest` and default to
    /// `false` for the same reason: a caller that does not ask gets the response it
    /// got before the fields existed. `{"op":"plan"}` remains a complete request.
    Plan {
        #[serde(default)]
        include_cost: bool,
        #[serde(default)]
        include_queue: bool,
    },
    /// Plan a data restatement (no token — does not advance fingerprint state).
    Restate {
        model: String,
        partitions: Vec<String>,
        #[serde(default)]
        include_cost: bool,
        #[serde(default)]
        include_queue: bool,
    },
    /// Topological model order.
    Graph,
    /// Persist a previously-staged snapshot. Rejected if the project moved on.
    Commit { token: u64 },
}

/// A response from the daemon.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Response {
    /// Boxed: the plan is by far the largest variant, and a response is built once per
    /// request, so the indirection costs nothing that shows. Serialized identically.
    Plan { revision: u64, token: Option<u64>, plan: Box<PlanResponse> },
    Graph { revision: u64, order: Vec<String> },
    /// The fields after `models` arrived with incremental reparse. They are additive:
    /// a consumer that reads only `revision` and `models` reads what it always did.
    Reloaded {
        revision: u64,
        models: usize,
        /// Models this reload analyzed: all of them on a full rebuild, the changed ones
        /// plus their downstream closure otherwise, and none when no SQL changed.
        reparsed: usize,
        /// Whether the full `build_project` ran rather than the incremental reparse.
        full_rebuild: bool,
        /// Why it did; present only when `full_rebuild` is.
        #[serde(skip_serializing_if = "Option::is_none")]
        full_rebuild_reason: Option<String>,
    },
    Committed { revision: u64 },
    Error { message: String },
}

/// Upper bound on un-committed staged plans retained per daemon (memory backstop).
pub const MAX_STAGED_PLANS: usize = 64;

/// How long a staged plan waits for its commit before it is evicted.
///
/// Long on purpose. A plan is staged when it is computed and committed after it has
/// *run*, and a backfill can run for hours; evicting it first leaves the client unable
/// to record work it has already done, and its next plan proposes the same rebuild
/// again. Memory is what [`MAX_STAGED_PLANS`] bounds. The TTL only clears out plans
/// nobody is coming back for.
pub const DEFAULT_STAGED_PLAN_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// How many evicted tokens are remembered, so that a late commit hears "evicted" rather
/// than "unknown". Bounded like everything a long-running daemon accumulates; past it,
/// the oldest get the "unknown or already-committed" answer.
const MAX_EVICTION_RECORDS: usize = 1024;

/// Where the daemon reads "now" when it ages staged plans.
///
/// A trait so that a test, this crate's or an embedder's, can move time forward
/// explicitly: a TTL test that sleeps is slow or flaky, and usually both.
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

/// The monotonic system clock, which is what [`ProjectCache::new`] uses.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

struct StagedPlan {
    revision: u64,
    snapshot: StateSnapshot,
    staged_at: Instant,
}

/// Why a staged plan was dropped before its commit arrived.
#[derive(Clone, Copy, Debug)]
enum Eviction {
    /// It outlived the staged-plan TTL.
    Expired,
    /// [`MAX_STAGED_PLANS`] newer plans pushed it out.
    Capacity,
}

struct CacheState {
    project: Arc<Project>,
    /// Held across plans, like the project itself: replace declarations belong to
    /// the project, so a caller should not have to resend them on every `plan`.
    incremental: IncrementalConfig,
    /// Last committed state (fingerprints + watermarks).
    snapshot: StateSnapshot,
    /// Bumped whenever the project or the incremental config changes; stamps staged
    /// plans for the stale-guard.
    revision: u64,
    staged: BTreeMap<u64, StagedPlan>,
    /// Tokens evicted before their commit arrived, so the commit can say so.
    evicted: BTreeMap<u64, Eviction>,
    next_token: u64,
    /// What `project` was built from, for the next reload to reparse against. `None`
    /// until the first successful reload.
    built: Option<Arc<Built>>,
}

impl CacheState {
    fn evict(&mut self, token: u64, why: Eviction) {
        if self.staged.remove(&token).is_some() {
            self.evicted.insert(token, why);
            while self.evicted.len() > MAX_EVICTION_RECORDS {
                self.evicted.pop_first();
            }
        }
    }

    /// Evict every staged plan at least `ttl` old. A scan rather than a pop from the
    /// front, so it stays correct for a clock that is not monotonic; there are at most
    /// [`MAX_STAGED_PLANS`] entries to look at.
    fn evict_expired(&mut self, now: Instant, ttl: Duration) {
        let expired: Vec<u64> = self
            .staged
            .iter()
            .filter(|(_, plan)| now.saturating_duration_since(plan.staged_at) >= ttl)
            .map(|(&token, _)| token)
            .collect();
        for token in expired {
            self.evict(token, Eviction::Expired);
        }
    }
}

/// What a reload builds on, read under one lock acquisition so the three agree.
struct ReloadBase {
    built: Option<Arc<Built>>,
    project: Arc<Project>,
    incremental: IncrementalConfig,
}

/// The warm project cache + request loop.
pub struct ProjectCache {
    inner: RwLock<CacheState>,
    clock: Arc<dyn Clock>,
    staged_plan_ttl: Duration,
}

impl Default for ProjectCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectCache {
    /// A cache on the system clock with the [`DEFAULT_STAGED_PLAN_TTL`].
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock))
    }

    /// A cache that ages staged plans by `clock`.
    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        Self {
            inner: RwLock::new(CacheState {
                project: Arc::new(Project::default()),
                incremental: IncrementalConfig::default(),
                snapshot: StateSnapshot::default(),
                revision: 0,
                staged: BTreeMap::new(),
                evicted: BTreeMap::new(),
                next_token: 0,
                built: None,
            }),
            clock,
            staged_plan_ttl: DEFAULT_STAGED_PLAN_TTL,
        }
    }

    /// Evict a staged plan once it is `ttl` old instead of [`DEFAULT_STAGED_PLAN_TTL`].
    pub fn with_staged_plan_ttl(mut self, ttl: Duration) -> Self {
        self.staged_plan_ttl = ttl;
        self
    }

    /// Dispatch one request.
    pub fn handle(&self, req: Request) -> Response {
        match req {
            Request::Reload { models, sources, materializations, incremental } => {
                self.reload(models, sources, materializations, incremental)
            }
            Request::Plan { include_cost, include_queue } => self.plan(include_cost, include_queue),
            Request::Restate { model, partitions, include_cost, include_queue } => {
                self.restate(&model, partitions, include_cost, include_queue)
            }
            Request::Graph => self.graph(),
            Request::Commit { token } => self.commit(token),
        }
    }

    /// Transport-agnostic JSON dispatch (the future HTTP/gRPC layers call this).
    pub fn handle_json(&self, input: &str) -> String {
        let resp = match serde_json::from_str::<Request>(input) {
            Ok(req) => self.handle(req),
            Err(e) => Response::Error { message: format!("bad request: {e}") },
        };
        serde_json::to_string(&resp).unwrap_or_else(|_| r#"{"status":"error","message":"serialize failed"}"#.to_string())
    }

    /// Current project revision (for tests / observability).
    pub fn revision(&self) -> u64 {
        self.inner.read().unwrap().revision
    }

    // --- handlers ---

    fn reload(
        &self,
        models: Vec<(String, String)>,
        sources: Catalog,
        materializations: BTreeMap<String, String>,
        incremental: IncrementalConfig,
    ) -> Response {
        let hints = match parse_materializations(materializations) {
            Ok(h) => h,
            Err(e) => return Response::Error { message: e },
        };
        // Reparse against a snapshot, outside the lock, so plans keep being answered
        // meanwhile; the write lock in `install` covers the swap and little else.
        let base = self.reload_base();
        match reparse::reload(base.built.as_deref(), &models, &sources, &hints) {
            Ok(outcome) => self.install(&base, outcome, incremental),
            Err(e) => Response::Error { message: format!("build_project: {e}") },
        }
    }

    fn reload_base(&self) -> ReloadBase {
        let st = self.inner.read().unwrap();
        ReloadBase {
            built: st.built.clone(),
            project: st.project.clone(),
            incremental: st.incremental.clone(),
        }
    }

    /// Swap in a reparsed project, moving the revision if plans could now differ.
    fn install(
        &self,
        base: &ReloadBase,
        outcome: reparse::Outcome,
        incremental: IncrementalConfig,
    ) -> Response {
        let project = outcome.built.project.clone();
        let changed = plan_state_differs(&base.project, &base.incremental, &project, &incremental);

        let mut st = self.inner.write().unwrap();
        // Another reload may have landed while this one parsed. The revision has to
        // describe the change from the state being *replaced*: compared only with the
        // snapshot, a plan staged against that other reload's project would survive this
        // one replacing it. So compare again, against what is there now.
        let changed = if same_build(&st.built, &base.built) {
            changed
        } else {
            plan_state_differs(&st.project, &st.incremental, &project, &incremental)
        };
        st.project = project;
        st.built = Some(Arc::new(outcome.built));
        st.incremental = incremental;
        if changed {
            st.revision += 1;
        }
        // Staged plans stay: a token from an earlier revision is rejected as *stale* by
        // the revision stamp in `commit`, which is a different answer from an unknown
        // or evicted one. The size cap and the TTL are what retire them.
        Response::Reloaded {
            revision: st.revision,
            models: st.project.len(),
            reparsed: outcome.reparsed,
            full_rebuild: outcome.full_rebuild.is_some(),
            full_rebuild_reason: outcome.full_rebuild.map(|why| why.reason().to_string()),
        }
    }

    fn plan(&self, include_cost: bool, include_queue: bool) -> Response {
        let mut st = self.inner.write().unwrap();
        // v2 AST-canonical fingerprints (drop-in for freshet_state::fingerprint_project).
        let current = AstFingerprinter::new().fingerprint_project(&st.project);
        let changed = changed_models(&st.snapshot, &current);

        let mut cols: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for name in &changed {
            cols.insert(name.clone(), st.project.get(name).map(|m| m.output_columns()).unwrap_or_default());
        }
        let plan = plan_backfill(&st.project, &cols);

        let next = StateSnapshot { fingerprints: current, watermarks: st.snapshot.watermarks.clone() };
        let revision = st.revision;
        let token = st.next_token;
        st.next_token += 1;
        // Bounded retention: stale (un-committed) plans are kept so the revision
        // stale-guard can distinguish them from unknown tokens, but a long-running
        // daemon must not accumulate them forever. Expired plans go first; then, if
        // the cap is still reached, oldest-first (lowest token).
        let now = self.clock.now();
        st.evict_expired(now, self.staged_plan_ttl);
        while st.staged.len() >= MAX_STAGED_PLANS {
            let Some(oldest) = st.staged.keys().next().copied() else { break };
            st.evict(oldest, Eviction::Capacity);
        }
        st.staged.insert(token, StagedPlan { revision, snapshot: next, staged_at: now });

        let wire = to_wire_with_estimate(
            &st.project,
            &plan,
            &st.incremental,
            &CostModel::default(),
            include_cost,
            include_queue,
        );
        Response::Plan { revision, token: Some(token), plan: Box::new(wire) }
    }

    fn restate(
        &self,
        model: &str,
        partitions: Vec<String>,
        include_cost: bool,
        include_queue: bool,
    ) -> Response {
        // Both, under one lock acquisition. A restatement rebuilds DATA, so the replace
        // declarations describing how that data is written matter to it at least as much as
        // to a code-change plan — and `plan` a few lines up honours them. Passing the
        // default here instead (which is what this did) sent a restatement consumer a
        // response with no `replace` op and nothing saying why.
        let (project, incremental) = {
            let st = self.inner.read().unwrap();
            (st.project.clone(), st.incremental.clone())
        };
        let planner = freshet_api::BackfillPlanner::new((*project).clone(), MemoryStateBackend::default());
        match planner.plan_restatement(model, partitions) {
            Ok(plan) => {
                let wire = to_wire_with_estimate(
                    planner.project(),
                    &plan,
                    &incremental,
                    &CostModel::default(),
                    include_cost,
                    include_queue,
                );
                Response::Plan { revision: self.revision(), token: None, plan: Box::new(wire) }
            }
            Err(e) => Response::Error { message: e.to_string() },
        }
    }

    fn graph(&self) -> Response {
        let st = self.inner.read().unwrap();
        match st.project.topo_order() {
            Ok(order) => Response::Graph { revision: st.revision, order },
            Err(cycle) => Response::Error { message: format!("dependency cycle: {}", cycle.join(", ")) },
        }
    }

    fn commit(&self, token: u64) -> Response {
        let mut st = self.inner.write().unwrap();
        // Expire first: a plan past its TTL is not committable whether or not a sweep
        // has reached it yet.
        st.evict_expired(self.clock.now(), self.staged_plan_ttl);
        match st.staged.remove(&token) {
            None => {
                let message = match st.evicted.get(&token) {
                    Some(Eviction::Expired) => format!(
                        "expired plan: token {token} waited longer than the {} staged-plan TTL \
                         for its commit and was evicted; re-plan",
                        human_duration(self.staged_plan_ttl)
                    ),
                    Some(Eviction::Capacity) => format!(
                        "evicted plan: token {token} was dropped to keep at most \
                         {MAX_STAGED_PLANS} uncommitted plans staged; re-plan"
                    ),
                    None => format!("unknown or already-committed token {token}"),
                };
                Response::Error { message }
            }
            Some(sp) if sp.revision != st.revision => {
                Response::Error { message: "stale plan: project changed since plan; re-plan".to_string() }
            }
            Some(sp) => {
                st.snapshot = sp.snapshot;
                Response::Committed { revision: st.revision }
            }
        }
    }
}

/// Whether a plan computed from one state could differ from one computed from the
/// other: the project (every field of every model) and the incremental config are
/// everything `plan` and `restate` read.
fn plan_state_differs(
    project_a: &Project,
    incremental_a: &IncrementalConfig,
    project_b: &Project,
    incremental_b: &IncrementalConfig,
) -> bool {
    !reparse::same_project(project_a, project_b) || !same_incremental(incremental_a, incremental_b)
}

/// `IncrementalConfig` has no `PartialEq`, so compare what it serializes to. Everything
/// in it arrived through a request, i.e. through serde, so serde sees every field that
/// can differ. Failing to serialize reads as "changed": a needless revision bump costs a
/// client a re-plan, and a missed one lets a stale plan commit.
fn same_incremental(a: &IncrementalConfig, b: &IncrementalConfig) -> bool {
    match (serde_json::to_value(a), serde_json::to_value(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// Whether two reloads built on the same previous build. Each successful reload makes a
/// new `Arc`, so pointer identity is "no reload landed in between".
fn same_build(a: &Option<Arc<Built>>, b: &Option<Arc<Built>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}

/// `24h`, `90m`, `45s`: what an operator configured, in the units they would have used.
fn human_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs == 0 || d.subsec_nanos() != 0 {
        format!("{d:?}")
    } else if secs.is_multiple_of(3600) {
        format!("{}h", secs / 3600)
    } else if secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

fn parse_materializations(raw: BTreeMap<String, String>) -> Result<BTreeMap<String, Materialization>, String> {
    let mut out = BTreeMap::new();
    for (name, s) in raw {
        let mat = match s.to_ascii_lowercase().as_str() {
            "table" => Materialization::Table,
            "view" => Materialization::View,
            "incremental" => Materialization::Incremental,
            other => return Err(format!("model {name}: unknown materialization `{other}`")),
        };
        out.insert(name, mat);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// The HTTP transport
// ---------------------------------------------------------------------------

/// An axum daemon over [`ProjectCache`].
///
/// **This layer holds no planning logic and no request vocabulary of its own.**
/// There is one route that does work, `POST /rpc`, and its whole body is
/// [`ProjectCache::handle_json`] — the same funnel the in-process lane calls. A
/// REST surface with a route per operation would be friendlier to `curl` and would
/// be a second copy of [`Request`] to keep in sync; this repository already carries
/// one deliberate mirror of a planner contract (`dagron-state`), and it is guarded
/// by a golden fixture precisely because duplication is expensive. One funnel needs
/// no guard.
///
/// # Binding, and what it means
///
/// There is **no authentication here**, exactly as in `dagpane-serve`,
/// and for the same reason: the thing being served is a developer tool. So the
/// default bind is loopback, and [`warn_if_exposed`] prints a warning on any other
/// address rather than silently accepting it. A planner daemon reachable from a
/// network is a daemon anyone on that network can hand arbitrary SQL to parse and
/// can read every model's source from.
#[cfg(feature = "http")]
pub mod http {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::extract::{DefaultBodyLimit, State};
    use axum::http::{header, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use axum::Router;

    use super::{ProjectCache, Response};

    /// Largest request body accepted, in bytes.
    ///
    /// A plan request carries the SQL text of *every* model in the project, so axum's
    /// 2 MiB default is the wrong shape — a few thousand models is an ordinary dbt
    /// repository and would be rejected as malformed. 32 MiB is chosen to be far
    /// above any real project and far below "a client can exhaust the daemon's
    /// memory by describing one".
    pub const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

    /// The router, for a caller that wants to nest it or drive it in a test.
    pub fn router(cache: Arc<ProjectCache>) -> Router {
        Router::new()
            .route("/rpc", post(rpc))
            .route("/healthz", get(healthz))
            .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
            .with_state(cache)
    }

    /// Bind `addr` and serve until SIGINT / SIGTERM.
    pub async fn serve(cache: Arc<ProjectCache>, addr: SocketAddr) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        serve_on(cache, listener).await
    }

    /// Serve on a listener the caller already bound.
    ///
    /// This exists for tests, and the reason is worth stating because the obvious
    /// alternative is racy: binding port 0 to discover a free port and then dropping
    /// the listener so the server can rebind leaves a window in which the OS can hand
    /// that port to somebody else. Handing the bound listener over closes it. Same
    /// shape as `dagpane_serve::serve_on`.
    pub async fn serve_on(
        cache: Arc<ProjectCache>,
        listener: tokio::net::TcpListener,
    ) -> std::io::Result<()> {
        axum::serve(listener, router(cache))
            .with_graceful_shutdown(shutdown())
            .await
    }

    /// Print a warning when the daemon is bound anywhere but loopback.
    ///
    /// Returns whether it warned, so a caller (and a test) can assert on the
    /// decision rather than on stderr.
    pub fn warn_if_exposed(addr: SocketAddr) -> bool {
        if addr.ip().is_loopback() {
            return false;
        }
        eprintln!(
            "warning: binding {addr}, which is not loopback. freshet-serve has no \
             authentication: anyone who can reach this address can read every model's \
             SQL and submit arbitrary SQL for it to parse. Put it behind something that \
             authenticates, or bind 127.0.0.1."
        );
        true
    }

    async fn shutdown() {
        let ctrl_c = async {
            let _ = tokio::signal::ctrl_c().await;
        };
        #[cfg(unix)]
        let term = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut s) => {
                    s.recv().await;
                }
                // No handler is not a reason to refuse to shut down on ctrl-c.
                Err(_) => std::future::pending::<()>().await,
            }
        };
        #[cfg(not(unix))]
        let term = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {}
            _ = term => {}
        }
    }

    /// The whole transport: bytes in, [`ProjectCache::handle_json`], bytes out.
    ///
    /// Always 200 with a JSON body, including for a malformed request — the funnel's
    /// contract is that a failure is an `{"status":"error"}` document, not a status
    /// code, so that every binding reads errors the same way whether it reached the
    /// planner over HTTP, the C-ABI, or a direct call. The one exception is a body
    /// that is not UTF-8, which never reaches the funnel.
    ///
    /// # Why the funnel runs on the blocking pool
    ///
    /// [`ProjectCache::handle_json`] is synchronous and *slow on purpose*: a `reload`
    /// can parse every model on the calling thread (a full rebuild), and a `plan` holds
    /// the cache's `std::sync::RwLock` **write** guard across fingerprinting, backfill
    /// planning and wire conversion. Awaiting nothing, it would run to completion on the Tokio worker
    /// that picked up the request — pinning one of a small, fixed set of threads for the
    /// whole parse, and parking every other request behind it, `healthz` included, since
    /// reading the revision needs that same lock. `spawn_blocking` puts it on the pool
    /// that exists for exactly this, where a thread sitting on a lock costs nothing the
    /// runtime needs back.
    async fn rpc(State(cache): State<Arc<ProjectCache>>, body: axum::body::Bytes) -> impl IntoResponse {
        // `spawn_blocking` wants a `'static` closure, so the request has to be owned
        // rather than borrowed out of `body`. That costs nothing over the old
        // `str::from_utf8`: `Bytes -> Vec<u8>` hands the buffer over when this is the
        // only handle to it, and `String::from_utf8` validates it in place. Its `Display`
        // is `Utf8Error`'s, so the message below is byte-identical to the old one.
        let json = match String::from_utf8(body.into()) {
            Ok(req) => match tokio::task::spawn_blocking(move || cache.handle_json(&req)).await {
                Ok(json) => json,
                // The funnel itself never returns `Err`, so this is the blocking task
                // having panicked. Answering in the funnel's own error shape keeps the
                // "a failure is a document" contract true even for the one failure the
                // funnel cannot express; letting the panic escape would hand the client
                // a bodyless 500 it has no parser for.
                Err(e) => join_failed(&e),
            },
            Err(e) => format!(
                r#"{{"status":"error","message":"request body is not valid UTF-8: {e}"}}"#
            ),
        };
        ([(header::CONTENT_TYPE, "application/json")], json)
    }

    async fn healthz(State(cache): State<Arc<ProjectCache>>) -> impl IntoResponse {
        // Cheap as it looks, `revision()` takes the read lock, so it blocks for as long
        // as whatever holds the write guard — a `plan` fingerprinting the project, say.
        // On a worker thread that turns the liveness probe into a second stalled
        // request; on the blocking pool it is just a slow one.
        match tokio::task::spawn_blocking(move || cache.revision()).await {
            Ok(revision) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                format!(r#"{{"status":"ok","revision":{revision}}}"#),
            ),
            // Reading a `u64` panics only if the lock is poisoned, i.e. some earlier
            // request died holding it and the cache is now unusable. That is precisely
            // what a health check exists to report, so it is the one place in this
            // transport that answers with a non-200 — with a body, so the client learns
            // why and not merely that.
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "application/json")],
                join_failed(&e),
            ),
        }
    }

    /// Render a panicked blocking task as the funnel's error document.
    ///
    /// Built through `serde_json` rather than `format!` because `JoinError`'s `Display`
    /// embeds the panic message with `{:?}` quoting — pasting that between quotes by
    /// hand produces a document the client cannot parse, which is the worst possible
    /// answer to "something went wrong".
    fn join_failed(e: &tokio::task::JoinError) -> String {
        let resp = Response::Error { message: format!("planner task failed: {e}") };
        serde_json::to_string(&resp)
            .unwrap_or_else(|_| r#"{"status":"error","message":"planner task failed"}"#.to_string())
    }
}

/// The one property of the HTTP layer that `tests/http.rs` cannot see.
///
/// That suite asserts the transport is a faithful pipe, one request at a time, on a
/// runtime with as many workers as the machine has cores — where a handler that blocks
/// its worker still looks perfectly healthy. Whether the funnel runs *off* the worker
/// threads is only observable when workers are scarce and two requests overlap, which
/// needs reach into `ProjectCache`'s lock and so has to live in-crate.
#[cfg(all(test, feature = "http"))]
mod http_concurrency_tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::mpsc;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Serve `cache` on its own thread, on a runtime with exactly **one** worker.
    ///
    /// One worker is the whole experiment: it makes "this handler occupied a worker
    /// thread" mean "this handler occupied *the* worker thread", so the stall is
    /// deterministic instead of a race you have to be unlucky to catch.
    fn serve_single_worker(cache: Arc<ProjectCache>) -> SocketAddr {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                tx.send(listener.local_addr().unwrap()).unwrap();
                let _ = http::serve_on(cache, listener).await;
            });
        });
        rx.recv().unwrap()
    }

    /// `GET /healthz`, same `Connection: close` trick as `post_rpc`.
    async fn get_healthz(addr: SocketAddr) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let head =
            format!("GET /healthz HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        stream.write_all(head.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// `Connection: close` makes the body "everything until EOF", same trick as
    /// `tests/http.rs` — no chunked decoding, no HTTP client dependency.
    async fn post_rpc(addr: SocketAddr, body: &'static [u8]) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let head = format!(
            "POST /rpc HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf).into_owned()
    }

    #[tokio::test]
    async fn a_request_waiting_on_the_cache_does_not_stall_the_daemon() {
        let cache = Arc::new(ProjectCache::new());
        let addr = serve_single_worker(cache.clone());

        // Stand in for a reparse: hold the write guard `reload` and `plan` hold, for as
        // long as this test needs rather than as long as a parse happens to take.
        let guard = cache.inner.write().unwrap();

        let blocked = tokio::spawn(post_rpc(addr, br#"{"op":"graph"}"#));
        // Long enough for it to be connected, dispatched, and parked on the read lock.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // This request never reaches the cache — it dies on UTF-8 validation — so it can
        // only be answered if the server's single worker is still free to accept it.
        // With the funnel running inline it was not: the graph request owned that thread
        // and this connection sat unaccepted until the lock came free.
        let quick = tokio::time::timeout(Duration::from_secs(5), post_rpc(addr, b"\xff"))
            .await
            .expect("daemon answered nothing while one request waited on the cache lock");
        assert!(quick.contains("not valid UTF-8"), "{quick}");

        drop(guard);
        let done = tokio::time::timeout(Duration::from_secs(5), blocked).await.unwrap().unwrap();
        assert!(done.contains(r#""status":"graph""#), "{done}");
    }

    /// The same claim for `healthz`, which needs its own test rather than sharing `rpc`'s.
    ///
    /// `healthz` reads `revision()`, and that read waits on the write guard `reload` and
    /// `plan` hold — so a health check parked inline blocks the worker exactly as an RPC
    /// does, and a liveness probe that hangs while the daemon is merely busy is worse than
    /// one that reports slowly. Reverting only the `healthz` half of the offload leaves the
    /// test above green, which is what makes this one necessary and not a duplicate.
    #[tokio::test]
    async fn a_health_check_waiting_on_the_cache_does_not_stall_the_daemon() {
        let cache = Arc::new(ProjectCache::new());
        let addr = serve_single_worker(cache.clone());

        let guard = cache.inner.write().unwrap();

        let blocked = tokio::spawn(get_healthz(addr));
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Dies on UTF-8 validation, so it never touches the cache: it can only be answered
        // if the single worker is still free while the health check waits on the lock.
        let quick = tokio::time::timeout(Duration::from_secs(5), post_rpc(addr, b"\xff"))
            .await
            .expect("daemon answered nothing while a health check waited on the cache lock");
        assert!(quick.contains("not valid UTF-8"), "{quick}");

        drop(guard);
        let done = tokio::time::timeout(Duration::from_secs(5), blocked).await.unwrap().unwrap();
        assert!(done.contains(r#""status":"ok""#), "{done}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use freshet_core::ColumnRef;
    use freshet_embed::build_project_with_hints;
    use serde_json::json;

    use reparse::FullRebuild;

    fn reload(cache: &ProjectCache) -> Response {
        cache.handle(Request::Reload {
            models: vec![
                ("stg".into(), "select a from raw".into()),
                ("mart".into(), "select a from stg".into()),
            ],
            sources: Catalog::from([("raw".to_string(), vec!["a".to_string()])]),
            materializations: BTreeMap::new(),
            incremental: Default::default(),
        })
    }

    /// `stg` feeds both marts; `other` stands apart from all three.
    const CHAIN: &[(&str, &str)] = &[
        ("stg", "select a, b from raw"),
        ("mart_a", "select a from stg"),
        ("mart_b", "select b from stg"),
        ("other", "select a from raw"),
    ];

    fn raw() -> Catalog {
        Catalog::from([("raw".to_string(), vec!["a".to_string(), "b".to_string()])])
    }

    fn owned(models: &[(&str, &str)]) -> Vec<(String, String)> {
        models.iter().map(|(name, sql)| (name.to_string(), sql.to_string())).collect()
    }

    /// `models`, with `name`'s SQL replaced.
    fn edit<'a>(models: &[(&'a str, &'a str)], name: &str, sql: &'a str) -> Vec<(&'a str, &'a str)> {
        models.iter().map(|&(n, s)| (n, if n == name { sql } else { s })).collect()
    }

    fn load(cache: &ProjectCache, models: &[(&str, &str)]) -> Response {
        load_with(cache, models, raw(), BTreeMap::new(), IncrementalConfig::default())
    }

    fn load_with(
        cache: &ProjectCache,
        models: &[(&str, &str)],
        sources: Catalog,
        materializations: BTreeMap<String, String>,
        incremental: IncrementalConfig,
    ) -> Response {
        cache.handle(Request::Reload { models: owned(models), sources, materializations, incremental })
    }

    /// The report a successful reload carries; `full_rebuild` is the reason, if any.
    #[derive(Debug)]
    struct Reloaded {
        revision: u64,
        reparsed: usize,
        full_rebuild: Option<String>,
    }

    fn reloaded(response: Response) -> Reloaded {
        match response {
            Response::Reloaded { revision, reparsed, full_rebuild, full_rebuild_reason, .. } => {
                assert_eq!(full_rebuild, full_rebuild_reason.is_some(), "a reason exactly when full");
                Reloaded { revision, reparsed, full_rebuild: full_rebuild_reason }
            }
            other => panic!("expected Reloaded, got {other:?}"),
        }
    }

    fn plan_token(cache: &ProjectCache) -> u64 {
        match cache.handle(Request::Plan { include_cost: false, include_queue: false }) {
            Response::Plan { token: Some(t), .. } => t,
            other => panic!("expected Plan, got {other:?}"),
        }
    }

    fn commit_error(cache: &ProjectCache, token: u64) -> String {
        match cache.handle(Request::Commit { token }) {
            Response::Error { message } => message,
            other => panic!("expected the commit of token {token} to fail, got {other:?}"),
        }
    }

    fn project(cache: &ProjectCache) -> Arc<Project> {
        cache.inner.read().unwrap().project.clone()
    }

    /// The daemon holds exactly what a from-scratch build makes of `models`.
    fn assert_full_build(cache: &ProjectCache, models: &[(&str, &str)]) {
        let full = build_project_with_hints(&owned(models), &raw(), &BTreeMap::new()).unwrap();
        assert!(reparse::same_project(&project(cache), &full), "differs from a full build");
    }

    #[test]
    fn reload_then_plan_then_commit() {
        let cache = ProjectCache::new();
        assert!(matches!(reload(&cache), Response::Reloaded { models: 2, .. }));

        // Cold plan: both models, plus a commit token.
        let token = match cache.handle(Request::Plan { include_cost: false, include_queue: false }) {
            Response::Plan { token: Some(t), plan, .. } => {
                assert_eq!(plan.models.len(), 2);
                t
            }
            other => panic!("expected Plan, got {other:?}"),
        };

        assert!(matches!(cache.handle(Request::Commit { token }), Response::Committed { .. }));

        // Nothing changed since commit -> empty plan.
        match cache.handle(Request::Plan { include_cost: false, include_queue: false }) {
            Response::Plan { plan, .. } => assert!(plan.models.is_empty()),
            other => panic!("expected empty Plan, got {other:?}"),
        }
    }

    #[test]
    fn a_reload_that_changes_the_project_leaves_earlier_tokens_stale() {
        let cache = ProjectCache::new();
        reload(&cache);
        let token = plan_token(&cache);
        cache.handle(Request::Reload {
            models: vec![
                ("stg".into(), "select a from raw".into()),
                ("mart".into(), "select a as x from stg".into()),
            ],
            sources: Catalog::from([("raw".to_string(), vec!["a".to_string()])]),
            materializations: BTreeMap::new(),
            incremental: Default::default(),
        });
        let message = commit_error(&cache, token);
        assert!(message.starts_with("stale plan"), "{message}");
        assert!(!message.contains("expired") && !message.contains("evicted"), "{message}");
    }

    #[test]
    fn an_identical_reload_keeps_the_revision_and_the_outstanding_tokens() {
        let cache = ProjectCache::new();
        let first = reloaded(reload(&cache));
        let token = plan_token(&cache);

        let again = reloaded(reload(&cache));
        assert_eq!(again.revision, first.revision, "nothing a plan reads changed");
        assert_eq!((again.reparsed, again.full_rebuild), (0, None));
        assert!(matches!(cache.handle(Request::Commit { token }), Response::Committed { .. }));
    }

    #[test]
    fn an_edit_reparses_the_edited_model_and_its_downstream_closure_and_nothing_else() {
        let cache = ProjectCache::new();
        let first = reloaded(load(&cache, CHAIN));
        assert_eq!(first.full_rebuild.as_deref(), Some(FullRebuild::FirstLoad.reason()));
        assert_eq!(first.reparsed, 4);

        // Nothing reads `mart_a`, so its edit reaches no one else.
        let leaf = edit(CHAIN, "mart_a", "select a as x from stg");
        let r = reloaded(load(&cache, &leaf));
        assert_eq!((r.reparsed, r.full_rebuild, r.revision), (1, None, first.revision + 1));
        assert_full_build(&cache, &leaf);

        // Both marts read `stg`, and `mart_b` reads the column this drops; `other`
        // reads neither.
        let root = edit(&leaf, "stg", "select a from raw");
        let r = reloaded(load(&cache, &root));
        assert_eq!((r.reparsed, r.full_rebuild), (3, None));
        let mart_b = project(&cache).get("mart_b").unwrap().clone();
        assert!(mart_b.columns[0].upstream.is_empty(), "`stg.b` is gone, so is `mart_b.b`'s lineage");
        assert_full_build(&cache, &root);
    }

    #[test]
    fn a_materialization_or_config_change_moves_the_revision_without_a_reparse() {
        let cache = ProjectCache::new();
        let first = reloaded(load(&cache, CHAIN));

        let view = BTreeMap::from([("mart_a".to_string(), "view".to_string())]);
        let r = reloaded(load_with(&cache, CHAIN, raw(), view, IncrementalConfig::default()));
        assert_eq!((r.reparsed, r.full_rebuild, r.revision), (0, None, first.revision + 1));
        assert_eq!(project(&cache).get("mart_a").unwrap().materialization, Materialization::View);

        // The same hint spelled in capitals is the same project.
        let shouted = BTreeMap::from([("mart_a".to_string(), "VIEW".to_string())]);
        let same = reloaded(load_with(&cache, CHAIN, raw(), shouted.clone(), IncrementalConfig::default()));
        assert_eq!(same.revision, r.revision);

        let config: IncrementalConfig =
            serde_json::from_value(json!({"models": {"mart_a": {"edges": {"stg": {"window": 3}}}}})).unwrap();
        let c = reloaded(load_with(&cache, CHAIN, raw(), shouted, config));
        assert_eq!((c.reparsed, c.full_rebuild, c.revision), (0, None, r.revision + 1));
    }

    #[test]
    fn a_sources_change_rebuilds_in_full_and_moves_the_revision_only_if_lineage_did() {
        let cache = ProjectCache::new();
        let first = reloaded(load(&cache, CHAIN));
        let token = plan_token(&cache);

        // Any model may read any source, so the full build runs. None reads this one,
        // though: the project comes out identical and the plan staged before it stands.
        let mut unread = raw();
        unread.insert("unread".to_string(), vec!["z".to_string()]);
        let r = reloaded(load_with(&cache, CHAIN, unread, BTreeMap::new(), IncrementalConfig::default()));
        assert_eq!(r.full_rebuild.as_deref(), Some(FullRebuild::SourcesChanged.reason()));
        assert_eq!(r.revision, first.revision);
        assert!(matches!(cache.handle(Request::Commit { token }), Response::Committed { .. }));

        // Dropping a column `stg` reads does move lineage.
        let narrow = Catalog::from([("raw".to_string(), vec!["a".to_string()])]);
        let r = reloaded(load_with(&cache, CHAIN, narrow, BTreeMap::new(), IncrementalConfig::default()));
        assert_eq!(r.full_rebuild.as_deref(), Some(FullRebuild::SourcesChanged.reason()));
        assert_eq!(r.revision, first.revision + 1);
    }

    #[test]
    fn adding_or_removing_a_model_rebuilds_in_full() {
        let cache = ProjectCache::new();
        reloaded(load(&cache, CHAIN));

        let mut grown = CHAIN.to_vec();
        grown.push(("extra", "select a from other"));
        let r = reloaded(load(&cache, &grown));
        assert_eq!(r.full_rebuild.as_deref(), Some(FullRebuild::ModelSetChanged.reason()));
        assert_eq!(r.reparsed, 5);

        let r = reloaded(load(&cache, CHAIN));
        assert_eq!(r.full_rebuild.as_deref(), Some(FullRebuild::ModelSetChanged.reason()));
    }

    /// The dbt import CTE: `z_mart` reads `orders` through a CTE of the same name. That
    /// is an ordinary edge, so an edit to `orders` reparses `z_mart` incrementally.
    #[test]
    fn an_import_cte_is_an_edge_and_reloads_incrementally() {
        let models = [
            ("orders", "select a, b from raw"),
            ("z_mart", "with orders as (select a from orders) select a from orders"),
            ("other", "select b from raw"),
        ];
        let cache = ProjectCache::new();
        reloaded(load(&cache, &models));
        let z_mart = project(&cache).get("z_mart").unwrap().clone();
        assert_eq!(z_mart.depends_on, vec!["orders".to_string()], "the CTE body reads the model");
        assert_eq!(z_mart.columns[0].upstream, vec![ColumnRef::new("orders", "a")]);

        // Renaming `orders.a` reaches `z_mart` through the edge.
        let renamed = edit(&models, "orders", "select a as x, b from raw");
        let r = reloaded(load(&cache, &renamed));
        assert_eq!((r.reparsed, r.full_rebuild), (2, None));
        assert!(project(&cache).get("z_mart").unwrap().columns[0].upstream.is_empty());
        assert_full_build(&cache, &renamed);
    }

    /// A CTE that takes a model's name without reading it: the engine's scan of the text
    /// cannot tell that from a read outside `depends_on`, so an edit that could reach it
    /// costs a full rebuild — never a wrong incremental one.
    #[test]
    fn a_cte_shadowing_a_model_forces_a_full_rebuild_when_an_edit_could_reach_it() {
        let models = [
            ("orders", "select a, b from raw"),
            ("z_mart", "with orders as (select a from raw) select a from orders"),
            ("other", "select b from raw"),
        ];
        let cache = ProjectCache::new();
        reloaded(load(&cache, &models));
        assert!(project(&cache).get("z_mart").unwrap().depends_on.is_empty(), "it reads only raw");

        let renamed = edit(&models, "orders", "select a as x, b from raw");
        let r = reloaded(load(&cache, &renamed));
        assert_eq!(r.full_rebuild.as_deref(), Some(FullRebuild::ReadsOutsideDependsOn.reason()));
        assert_full_build(&cache, &renamed);

        // An edit it cannot see stays incremental.
        let elsewhere = edit(&renamed, "other", "select b as y from raw");
        let r = reloaded(load(&cache, &elsewhere));
        assert_eq!((r.reparsed, r.full_rebuild), (1, None));
        assert_full_build(&cache, &elsewhere);
    }

    #[test]
    fn a_reload_that_fails_leaves_the_project_and_its_revision_alone() {
        let cache = ProjectCache::new();
        let first = reloaded(load(&cache, CHAIN));
        match load(&cache, &edit(CHAIN, "mart_b", "selec b from stg")) {
            Response::Error { message } => assert!(message.starts_with("build_project:"), "{message}"),
            other => panic!("expected a build error, got {other:?}"),
        }
        assert_eq!(cache.revision(), first.revision);
        assert_full_build(&cache, CHAIN);

        // The failure left no build behind, so going back is an identical reload.
        let back = reloaded(load(&cache, CHAIN));
        assert_eq!((back.revision, back.reparsed, back.full_rebuild), (first.revision, 0, None));
    }

    #[test]
    fn a_dependency_cycle_is_reported_by_the_full_build() {
        let cache = ProjectCache::new();
        reloaded(load(&cache, CHAIN));
        match load(&cache, &edit(CHAIN, "stg", "select a, b from mart_a")) {
            Response::Error { message } => assert!(message.contains("dependency cycle"), "{message}"),
            other => panic!("expected a cycle error, got {other:?}"),
        }
    }

    #[test]
    fn a_reload_racing_another_is_measured_against_what_it_replaces() {
        let cache = ProjectCache::new();
        reloaded(load(&cache, CHAIN));

        // Reload A snapshots the project and reparses it unchanged: on its own, an
        // identical reload, which would keep the revision...
        let base = cache.reload_base();
        let outcome =
            reparse::reload(base.built.as_deref(), &owned(CHAIN), &raw(), &BTreeMap::new()).unwrap();

        // ...but reload B lands first with an edit, and a plan is staged against it.
        let b = reloaded(load(&cache, &edit(CHAIN, "mart_a", "select a as x from stg")));
        let token = plan_token(&cache);

        // A replaces B's project with a different one, so B's plan has gone stale.
        let a = reloaded(cache.install(&base, outcome, IncrementalConfig::default()));
        assert_eq!(a.revision, b.revision + 1);
        assert!(commit_error(&cache, token).starts_with("stale plan"));
    }

    #[test]
    fn graph_is_topologically_ordered() {
        let cache = ProjectCache::new();
        reload(&cache);
        match cache.handle(Request::Graph) {
            Response::Graph { order, .. } => {
                let pos = |n: &str| order.iter().position(|m| m == n).unwrap();
                assert!(pos("stg") < pos("mart"));
            }
            other => panic!("expected Graph, got {other:?}"),
        }
    }

    #[test]
    fn the_reloaded_response_adds_its_report_to_the_fields_it_always_had() {
        let cache = ProjectCache::new();
        let body = r#"{"op":"reload","models":[["stg","select a from raw"]],"sources":{"raw":["a"]}}"#;
        let first: serde_json::Value = serde_json::from_str(&cache.handle_json(body)).unwrap();
        assert_eq!(first["status"], "reloaded");
        assert_eq!(first["revision"], 1);
        assert_eq!(first["models"], 1);
        assert_eq!(first["reparsed"], 1);
        assert_eq!(first["full_rebuild"], true);
        assert!(first["full_rebuild_reason"].as_str().unwrap().starts_with("first load"), "{first}");

        let again: serde_json::Value = serde_json::from_str(&cache.handle_json(body)).unwrap();
        assert_eq!(again["revision"], 1);
        assert_eq!(again["reparsed"], 0);
        assert_eq!(again["full_rebuild"], false);
        assert!(again.get("full_rebuild_reason").is_none(), "absent, not null: {again}");
    }

    // --- staged-plan eviction ---

    /// A clock that moves only when told to.
    struct ManualClock {
        start: Instant,
        elapsed: Mutex<Duration>,
    }

    impl ManualClock {
        fn new() -> Arc<Self> {
            Arc::new(Self { start: Instant::now(), elapsed: Mutex::new(Duration::ZERO) })
        }

        fn advance(&self, by: Duration) {
            *self.elapsed.lock().unwrap() += by;
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> Instant {
            self.start + *self.elapsed.lock().unwrap()
        }
    }

    const MINUTE: Duration = Duration::from_secs(60);

    /// A loaded cache on `clock`, with a one-minute staged-plan TTL.
    fn cache_on(clock: &Arc<ManualClock>) -> ProjectCache {
        let cache = ProjectCache::with_clock(clock.clone()).with_staged_plan_ttl(MINUTE);
        reload(&cache);
        cache
    }

    #[test]
    fn a_plan_can_be_committed_until_its_ttl_runs_out() {
        let clock = ManualClock::new();
        let cache = cache_on(&clock);
        let token = plan_token(&cache);
        clock.advance(MINUTE - Duration::from_secs(1));
        assert!(matches!(cache.handle(Request::Commit { token }), Response::Committed { .. }));
    }

    #[test]
    fn a_plan_past_its_ttl_is_evicted_and_its_commit_says_expired_not_stale() {
        let clock = ManualClock::new();
        let cache = cache_on(&clock);
        let token = plan_token(&cache);
        clock.advance(MINUTE);

        let message = commit_error(&cache, token);
        assert!(message.starts_with("expired plan"), "{message}");
        assert!(message.contains("the 1m staged-plan TTL"), "names the TTL: {message}");
        assert!(!message.contains("stale"), "{message}");
        // Still the answer on a retry: evicted is not the same as committed.
        assert_eq!(commit_error(&cache, token), message);
    }

    #[test]
    fn a_plan_pushed_out_by_the_size_cap_is_reported_as_evicted() {
        let cache = ProjectCache::new();
        reload(&cache);
        let oldest = plan_token(&cache);
        let newest = (0..MAX_STAGED_PLANS).map(|_| plan_token(&cache)).last().unwrap();

        let message = commit_error(&cache, oldest);
        assert!(message.starts_with("evicted plan"), "{message}");
        assert!(!message.contains("stale"), "{message}");
        assert!(matches!(cache.handle(Request::Commit { token: newest }), Response::Committed { .. }));
    }

    #[test]
    fn a_token_that_was_never_issued_is_unknown() {
        let cache = ProjectCache::new();
        reload(&cache);
        assert!(commit_error(&cache, 7).starts_with("unknown"));
    }
}
