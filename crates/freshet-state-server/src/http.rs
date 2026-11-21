//! The HTTP transport over [`Store`]: the three calls, the console's two reads, a
//! health check, and the tokens.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use freshet_state::StateSnapshot;
use serde_json::{json, Value};

use crate::console::{self, ConsoleFiles};
use crate::store::{valid_name, Commit, Store};

/// The token that may read and commit. Unset (with [`READ_TOKEN_ENV`] unset too), the
/// server answers anyone.
pub const TOKEN_ENV: &str = "FRESHET_STATE_SERVER_TOKEN";
/// A token that may read state and history but not commit: for jobs that only plan.
pub const READ_TOKEN_ENV: &str = "FRESHET_STATE_SERVER_READ_TOKEN";

/// Largest commit accepted, in bytes. A snapshot is about 40 bytes a model, so this is
/// room for well over a million models; the CLI reads replies up to the same size.
pub const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// History's page size when the request names none, and the most one page holds.
pub const DEFAULT_LIMIT: usize = 20;
pub const MAX_LIMIT: usize = 100;

/// The fields a snapshot carries. A commit with any other is refused: storing it
/// without them would drop them for good.
const SNAPSHOT_FIELDS: [&str; 2] = ["fingerprints", "watermarks"];

/// Who may do what.
#[derive(Clone, Default)]
pub struct Tokens {
    /// May read and commit.
    pub commit: Option<String>,
    /// May read only.
    pub read: Option<String>,
}

impl Tokens {
    /// Whether any token is required at all.
    pub fn required(&self) -> bool {
        self.commit.is_some() || self.read.is_some()
    }
}

#[derive(Clone)]
struct App {
    store: Arc<Mutex<Store>>,
    tokens: Arc<Tokens>,
}

#[derive(Clone, Copy, PartialEq)]
enum Access {
    Read,
    Commit,
}

/// The API, for a caller that serves it itself or drives it in a test.
pub fn router(store: Store, tokens: Tokens) -> Router {
    Router::new()
        .route(
            "/healthz",
            get(|| async { json_reply(StatusCode::OK, json!({"status": "ok"})) }),
        )
        .route("/v1", get(states))
        .route("/v1/", get(states))
        .route("/v1/{name}/current", get(current))
        .route("/v1/{name}/commit", post(commit))
        .route("/v1/{name}/history", get(history))
        .route("/v1/{name}/versions/{version}", get(version))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(App {
            store: Arc::new(Mutex::new(store)),
            tokens: Arc::new(tokens),
        })
}

/// The API, and the console's files at `/console/` when there are any to serve.
pub fn app(store: Store, tokens: Tokens, console: Option<ConsoleFiles>) -> Router {
    let api = router(store, tokens);
    match console {
        Some(files) => api.merge(console::routes(files)),
        None => api,
    }
}

/// Bind `addr` and serve `app` until SIGINT or SIGTERM.
pub async fn serve(app: Router, addr: SocketAddr) -> std::io::Result<()> {
    serve_app_on(app, tokio::net::TcpListener::bind(addr).await?).await
}

/// Serve the API on a listener the caller already bound (a test binds port 0 and
/// keeps it, rather than freeing a port for someone else to take before the server
/// rebinds it).
pub async fn serve_on(
    store: Store,
    tokens: Tokens,
    listener: tokio::net::TcpListener,
) -> std::io::Result<()> {
    serve_app_on(router(store, tokens), listener).await
}

/// Serve `app` (from [`app`] or [`router`]) on a listener the caller already bound.
pub async fn serve_app_on(app: Router, listener: tokio::net::TcpListener) -> std::io::Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await
}

/// Warn when bound beyond loopback with no token; note that the token crosses the
/// network in clear text when there is one. Returns whether it warned.
pub fn warn_if_exposed(addr: SocketAddr, tokens: &Tokens) -> bool {
    if addr.ip().is_loopback() {
        return false;
    }
    if tokens.required() {
        eprintln!(
            "note: binding {addr}, which is not loopback. It is plain HTTP, so the bearer token \
             crosses the network in clear text: terminate TLS in front of the server."
        );
        return false;
    }
    eprintln!(
        "warning: binding {addr}, which is not loopback, with no token. Anyone who can reach \
         this address can read every state and commit over it. Set {TOKEN_ENV} (and \
         {READ_TOKEN_ENV} for jobs that only plan), or bind 127.0.0.1."
    );
    true
}

/// `GET /v1/`: every state with a commit, and what the caller's token may do, so a
/// console can say whether it holds the read-only token or the one that commits.
async fn states(State(app): State<App>, headers: HeaderMap) -> Response {
    if let Some(refused) = check(&app.tokens, &headers, Access::Read) {
        return refused;
    }
    let access = if !app.tokens.required() {
        "open"
    } else if presented(&app.tokens, &headers) == Some(Access::Commit) {
        "commit"
    } else {
        "read"
    };
    run(app.store, move |store| match store.states() {
        Ok(states) => json_reply(StatusCode::OK, json!({"states": states, "access": access})),
        Err(e) => server_error(e),
    })
    .await
}

async fn current(State(app): State<App>, Path(name): Path<String>, headers: HeaderMap) -> Response {
    if let Some(refused) = check(&app.tokens, &headers, Access::Read).or_else(|| check_name(&name))
    {
        return refused;
    }
    run(app.store, move |store| match store.current(&name) {
        Ok((version, snapshot)) => json_reply(
            StatusCode::OK,
            json!({"version": version, "snapshot": snapshot}),
        ),
        Err(e) => server_error(e),
    })
    .await
}

async fn commit(
    State(app): State<App>,
    Path(name): Path<String>,
    request: axum::extract::Request,
) -> Response {
    let (parts, body) = request.into_parts();
    // Refused before a byte of the body is read: a caller without a token that may
    // commit cannot make the server buffer `MAX_BODY_BYTES` to be told no.
    if let Some(refused) =
        check(&app.tokens, &parts.headers, Access::Commit).or_else(|| check_name(&name))
    {
        return refused;
    }
    let body = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(body) => body,
        Err(e) => {
            return refuse(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("the commit was not read (the limit is {MAX_BODY_BYTES} bytes): {e}"),
            )
        }
    };
    let (expected, snapshot, context) = match read_commit(&body) {
        Ok(parts) => parts,
        Err((status, message)) => return refuse(status, message),
    };
    run(app.store, move |store| match store.commit(&name, expected, snapshot, context, SystemTime::now()) {
        Ok(Commit::Landed(version)) => json_reply(StatusCode::OK, json!({"version": version})),
        Ok(Commit::Conflict { expected, found }) => json_reply(
            StatusCode::CONFLICT,
            json!({
                "error": format!(
                    "state `{name}` is at version {}, not {}: another commit landed after this plan was computed; re-plan",
                    show(found),
                    show(expected)
                ),
                "expected": expected,
                "found": found,
            }),
        ),
        Err(e) => server_error(e),
    })
    .await
}

async fn history(
    State(app): State<App>,
    Path(name): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if let Some(refused) = check(&app.tokens, &headers, Access::Read).or_else(|| check_name(&name))
    {
        return refused;
    }
    let limit = match query.get("limit").map(|l| l.parse::<usize>()) {
        None => DEFAULT_LIMIT,
        Some(Ok(l)) if l >= 1 => l.min(MAX_LIMIT),
        Some(_) => {
            return refuse(
                StatusCode::BAD_REQUEST,
                "`limit` must be a positive integer".to_string(),
            )
        }
    };
    let before = match query.get("before").map(|b| b.parse::<i64>()) {
        None => None,
        Some(Ok(b)) if b >= 1 => Some(b),
        Some(_) => {
            return refuse(
                StatusCode::BAD_REQUEST,
                "`before` must be a positive version".to_string(),
            )
        }
    };
    run(app.store, move |store| {
        match store.history(&name, limit, before) {
            Ok(page) => json_reply(
                StatusCode::OK,
                json!({"commits": page.commits, "limit": limit, "next_before": page.next_before}),
            ),
            Err(e) => server_error(e),
        }
    })
    .await
}

/// `GET /v1/{name}/versions/{v}`: one kept commit with its snapshot.
async fn version(
    State(app): State<App>,
    Path((name, version)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Some(refused) = check(&app.tokens, &headers, Access::Read).or_else(|| check_name(&name))
    {
        return refused;
    }
    let version = match version.parse::<i64>() {
        Ok(v) if v >= 1 => v,
        _ => {
            return refuse(
                StatusCode::BAD_REQUEST,
                format!("`{version}` is not a version: versions count up from 1"),
            )
        }
    };
    run(app.store, move |store| match store.version(&name, version) {
        Ok(Some((entry, snapshot))) => json_reply(
            StatusCode::OK,
            json!({
                "version": entry.version,
                "committed_at": entry.committed_at,
                "summary": entry.summary,
                "snapshot": snapshot,
            }),
        ),
        Ok(None) => refuse(
            StatusCode::NOT_FOUND,
            format!("state `{name}` keeps no version {version}: it never landed, or retention (`--keep`) has dropped it"),
        ),
        Err(e) => server_error(e),
    })
    .await
}

/// `{"expected_version": int|null, "snapshot": {...}, "summary": any}`, checked; else
/// the status and the words to refuse it with.
fn read_commit(body: &[u8]) -> Result<(Option<i64>, StateSnapshot, Value), (StatusCode, String)> {
    let bad = |message: String| (StatusCode::BAD_REQUEST, message);
    let request: Value =
        serde_json::from_slice(body).map_err(|e| bad(format!("the commit is not JSON: {e}")))?;
    let expected = match request.get("expected_version") {
        Some(Value::Null) => None,
        Some(v) => match v.as_i64() {
            Some(n) => Some(n),
            None => {
                return Err(bad(format!(
                    "`expected_version` must be null or an integer, not {v}"
                )))
            }
        },
        None => return Err(bad(
            "the commit names no `expected_version`: null for a state's first commit, else the \
                 version the plan was computed against"
                .to_string(),
        )),
    };
    let Some(fields) = request.get("snapshot").and_then(Value::as_object) else {
        return Err(bad(
            "the commit's `snapshot` is missing or not an object".to_string()
        ));
    };
    let unknown: Vec<&str> = fields
        .keys()
        .map(String::as_str)
        .filter(|k| !SNAPSHOT_FIELDS.contains(k))
        .collect();
    if !unknown.is_empty() {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "the snapshot carries field(s) {unknown:?} this server cannot store; refusing it \
                 rather than dropping them (upgrade freshet-state-server)"
            ),
        ));
    }
    let snapshot: StateSnapshot =
        serde_json::from_value(Value::Object(fields.clone())).map_err(|e| {
            bad(format!(
                "the commit's `snapshot` is not a planner state snapshot: {e}"
            ))
        })?;
    let context = request.get("summary").cloned().unwrap_or(Value::Null);
    Ok((expected, snapshot, context))
}

/// The store does file IO under a lock, so it runs on the blocking pool.
async fn run(
    store: Arc<Mutex<Store>>,
    f: impl FnOnce(&mut Store) -> Response + Send + 'static,
) -> Response {
    tokio::task::spawn_blocking(move || {
        let mut store = store
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        f(&mut store)
    })
    .await
    .unwrap_or_else(|e| {
        refuse(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the request failed: {e}"),
        )
    })
}

/// Which of the server's tokens `headers` carry, if either.
fn presented(tokens: &Tokens, headers: &HeaderMap) -> Option<Access> {
    let given = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.as_bytes().strip_prefix(b"Bearer "));
    let matches = |token: &Option<String>| {
        token
            .as_deref()
            .is_some_and(|t| given.is_some_and(|g| same(g, t.as_bytes())))
    };
    if matches(&tokens.commit) {
        Some(Access::Commit)
    } else if matches(&tokens.read) {
        Some(Access::Read)
    } else {
        None
    }
}

/// The refusal, if `headers` do not carry a token allowed to do what is asked.
fn check(tokens: &Tokens, headers: &HeaderMap, wants: Access) -> Option<Response> {
    if !tokens.required() {
        return None;
    }
    match presented(tokens, headers) {
        Some(Access::Commit) => return None,
        Some(Access::Read) if wants == Access::Read => return None,
        Some(Access::Read) => {
            return Some(refuse(
                StatusCode::FORBIDDEN,
                format!("this token may read state but not commit it: commit with {TOKEN_ENV}'s"),
            ))
        }
        None => {}
    }
    let mut refused = refuse(
        StatusCode::UNAUTHORIZED,
        format!(
            "unauthorized: send Authorization: Bearer <the {TOKEN_ENV} or {READ_TOKEN_ENV} token>"
        ),
    );
    refused.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        header::HeaderValue::from_static("Bearer"),
    );
    Some(refused)
}

/// The refusal, if `name` cannot name a state.
fn check_name(name: &str) -> Option<Response> {
    if valid_name(name) {
        None
    } else {
        Some(refuse(
            StatusCode::NOT_FOUND,
            format!("`{name}` is not a state name: letters, digits, `.`, `_` and `-`, starting with a letter or digit"),
        ))
    }
}

/// Equal, in time that depends only on the lengths: timing a refusal says nothing about
/// where a guess first went wrong.
fn same(given: &[u8], token: &[u8]) -> bool {
    given.len() == token.len()
        && given
            .iter()
            .zip(token)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

fn show(version: Option<i64>) -> String {
    version.map_or_else(|| "none (never committed)".to_string(), |v| v.to_string())
}

fn json_reply(status: StatusCode, body: Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

fn refuse(status: StatusCode, message: String) -> Response {
    json_reply(status, json!({"error": message}))
}

fn server_error(e: anyhow::Error) -> Response {
    eprintln!("error: {e:#}");
    refuse(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
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
