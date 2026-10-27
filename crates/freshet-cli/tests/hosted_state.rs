//! `--state https://…` — hosted state — through the real binary, against a fake of
//! the protocol dagron Cloud's edge serves (`GET …/current`, `POST …/commit`).
//!
//! The fake is not a mock that answers what a test scripts: it keeps a version and a
//! snapshot and does the compare-and-swap for real, so a lost race here is lost the
//! way it is in production — including the one the race test provokes with two
//! `freshet` processes that plan against the same version and commit at once.
//! What the server *does* with a commit (Postgres, scopes, metering) is tested on
//! the hosted state service's (dagron Cloud) side; the question here is what the CLI sends, what it keeps,
//! and how it fails.

// Hosted state is optional (`--no-default-features` drops the HTTP client).
#![cfg(feature = "hosted")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Condvar, Mutex};

use serde_json::{json, Value};

const TOKEN: &str = "tok-5f1a9c-hosted";
const EXIT_STATE_CONFLICT: i32 = 3;

// ---------------------------------------------------------------- the fake --

/// One request, as the fake saw it.
#[derive(Clone, Debug)]
struct Seen {
    method: String,
    path: String,
    authorization: Option<String>,
    body: String,
}

#[derive(Default)]
struct Store {
    version: Option<i64>,
    snapshot: Value,
    seen: Vec<Seen>,
    /// Bodies of the commits that landed.
    landed: Vec<Value>,
    /// Hold every `GET …/current` reply until this many have arrived: lines two
    /// planners up on the same version.
    hold_gets: usize,
    gets: usize,
    /// Another writer commits right after the next `GET` is answered.
    commit_after_next_get: bool,
    /// Serve a snapshot carrying a field no freshet knows.
    extra_field: bool,
}

struct FakeHosted {
    addr: SocketAddr,
    store: Arc<(Mutex<Store>, Condvar)>,
}

impl FakeHosted {
    fn start() -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let store = Arc::new((
            Mutex::new(Store { snapshot: json!({ "fingerprints": {}, "watermarks": {} }), ..Store::default() }),
            Condvar::new(),
        ));
        let shared = Arc::clone(&store);
        // Detached, like the submit tests' stub: the process reaps it.
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let store = Arc::clone(&shared);
                // A thread per connection: the race test holds two replies open at once.
                std::thread::spawn(move || serve(stream, &store));
            }
        });
        Self { addr, store }
    }

    fn url(&self) -> String {
        format!("http://{}/state/v1", self.addr)
    }

    fn with<T>(&self, f: impl FnOnce(&mut Store) -> T) -> T {
        f(&mut self.store.0.lock().unwrap())
    }

    fn version(&self) -> Option<i64> {
        self.with(|s| s.version)
    }

    fn snapshot(&self) -> Value {
        self.with(|s| s.snapshot.clone())
    }

    fn seen(&self) -> Vec<Seen> {
        self.with(|s| s.seen.clone())
    }

    fn posts(&self) -> usize {
        self.seen().iter().filter(|r| r.method == "POST").count()
    }

    fn landed(&self) -> Vec<Value> {
        self.with(|s| s.landed.clone())
    }
}

fn serve(mut stream: TcpStream, store: &(Mutex<Store>, Condvar)) {
    let Some(req) = read_request(&mut stream) else { return };
    let (lock, ready) = store;
    let mut s = lock.lock().unwrap();
    s.seen.push(req.clone());
    let (status, body) = if req.authorization.as_deref() != Some(&format!("Bearer {TOKEN}")) {
        (401, json!({ "error": "invalid api token" }))
    } else {
        match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/state/v1/current") => {
                s.gets += 1;
                ready.notify_all();
                while s.gets < s.hold_gets {
                    s = ready.wait(s).unwrap();
                }
                let mut snapshot = s.snapshot.clone();
                if s.extra_field {
                    snapshot["lineage"] = json!({});
                }
                let reply = json!({ "version": s.version, "snapshot": snapshot });
                if std::mem::take(&mut s.commit_after_next_get) {
                    // Someone else's commit lands while this caller plans.
                    s.version = Some(s.version.unwrap_or(0) + 1);
                    s.snapshot["fingerprints"]["someone_else"] = json!(1);
                }
                (200, reply)
            }
            ("POST", "/state/v1/commit") => {
                let commit: Value = serde_json::from_str(&req.body).unwrap();
                let expected = commit["expected_version"].as_i64();
                if expected != s.version {
                    let found = s.version;
                    (409, json!({
                        "error": format!(
                            "hosted state conflict for tenant `ws-test`: expected {expected:?}, found {found:?} \
                             (another commit landed after this plan was computed; re-plan)"
                        ),
                        "expected": expected,
                        "found": found,
                    }))
                } else {
                    let version = s.version.unwrap_or(0) + 1;
                    s.version = Some(version);
                    s.snapshot = commit["snapshot"].clone();
                    s.landed.push(commit);
                    (200, json!({ "version": version }))
                }
            }
            _ => (404, json!({ "error": "no such hosted state route" })),
        }
    };
    drop(s);
    let body = body.to_string();
    let reason = match status { 200 => "OK", 401 => "Unauthorized", 409 => "Conflict", _ => "Not Found" };
    let _ = write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.flush();
}

fn read_request(stream: &mut TcpStream) -> Option<Seen> {
    let mut reader = BufReader::new(stream.try_clone().ok()?);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let mut parts = line.split_whitespace();
    let (method, path) = (parts.next()?.to_string(), parts.next()?.to_string());
    let (mut length, mut authorization) = (0usize, None);
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).ok()? == 0 {
            break;
        }
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((k, v)) = header.split_once(':') {
            if k.eq_ignore_ascii_case("content-length") {
                length = v.trim().parse().unwrap_or(0);
            } else if k.eq_ignore_ascii_case("authorization") {
                authorization = Some(v.trim().to_string());
            }
        }
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).ok()?;
    Some(Seen { method, path, authorization, body: String::from_utf8_lossy(&body).into_owned() })
}

// ---------------------------------------------------------------- fixtures --

/// Four models: two roots, a fan-in mart, and a model on a branch the edit below
/// never touches — so a warm plan has something to skip.
fn project(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("freshet-hosted-{case}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write(&dir, "stg_orders.sql", "select order_id, customer_id, amount from raw_orders");
    write(&dir, "dim_customer.sql", "select customer_id, region from raw_customers");
    write(
        &dir,
        "mart_revenue.sql",
        "select c.region, sum(o.amount) as revenue from stg_orders o \
         join dim_customer c on o.customer_id = c.customer_id group by c.region",
    );
    write(&dir, "untouched_side.sql", "select customer_id from dim_customer");
    write(
        &dir,
        "sources.json",
        r#"{"raw_orders":["order_id","customer_id","amount"],"raw_customers":["customer_id","region"]}"#,
    );
    dir
}

/// Incremental models with a lookback window, so a restatement leaves a ledger.
fn incremental_project(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("freshet-hosted-{case}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write(&dir, "events.sql", "select dt, n from raw_events");
    write(&dir, "rolling.sql", "select dt, n from events");
    write(&dir, "sources.json", r#"{"raw_events": ["dt", "n"]}"#);
    write(&dir, "materializations.json", r#"{"events": "incremental", "rolling": "incremental"}"#);
    write(&dir, "incremental.json", r#"{"models": {"rolling": {"grain": "Day", "edges": {"events": {"window": 2}}}}}"#);
    dir
}

fn write(dir: &Path, name: &str, body: &str) {
    std::fs::write(dir.join(name), body).unwrap();
}

fn command(args: &[&str]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_freshet"));
    cmd.args(args).env("FRESHET_STATE_TOKEN", TOKEN);
    cmd
}

fn freshet(args: &[&str]) -> Output {
    command(args).output().unwrap()
}

fn stdout(out: &Output) -> String {
    String::from_utf8(out.stdout.clone()).unwrap()
}

fn stderr(out: &Output) -> String {
    String::from_utf8(out.stderr.clone()).unwrap()
}

fn ok(out: Output) -> Output {
    assert!(out.status.success(), "freshet failed ({:?}): {}", out.status.code(), stderr(&out));
    out
}

fn models(plan: &Value) -> Vec<String> {
    plan["models"].as_array().unwrap().iter().map(|m| m["name"].as_str().unwrap().to_string()).collect()
}

fn plan_json(dir: &Path, state: &str) -> Value {
    let out = ok(freshet(&["plan", "--project", dir.to_str().unwrap(), "--state", state, "--json"]));
    serde_json::from_str(&stdout(&out)).unwrap()
}

fn commit(dir: &Path, state: &str, extra: &[&str]) -> Output {
    let mut args = vec!["plan", "--project", dir.to_str().unwrap(), "--state", state, "--commit"];
    args.extend_from_slice(extra);
    freshet(&args)
}

// ----------------------------------------------------------------- reading --

#[test]
fn a_cold_plan_reads_hosted_state_and_writes_nothing() {
    let (hosted, dir) = (FakeHosted::start(), project("cold"));
    let out = ok(freshet(&["plan", "--project", dir.to_str().unwrap(), "--state", &hosted.url(), "--json"]));

    let plan: Value = serde_json::from_str(&stdout(&out)).expect("stdout stays the plan JSON");
    assert_eq!(models(&plan).len(), 4, "no state yet: everything is new");
    let err = stderr(&out);
    assert!(err.contains("hosted state: no commits yet (version none)"), "{err}");
    assert!(err.contains("--commit --expect-version none"), "the commit to run later is spelt out: {err}");

    let seen = hosted.seen();
    assert_eq!(seen.len(), 1, "one GET, no commit: {seen:?}");
    assert_eq!((seen[0].method.as_str(), seen[0].path.as_str()), ("GET", "/state/v1/current"));
    assert_eq!(seen[0].authorization.as_deref(), Some(format!("Bearer {TOKEN}").as_str()));
    assert_eq!(hosted.version(), None);
}

/// The mapping between a state file and hosted state is the identity: the snapshot a
/// hosted commit stores is byte-for-byte the document `FileStateBackend` writes —
/// fingerprints and the restatement ledger alike.
#[test]
fn a_hosted_commit_stores_exactly_what_a_state_file_would() {
    let (hosted, dir) = (FakeHosted::start(), incremental_project("identity"));
    let file = dir.join("state.json");
    let file = file.to_str().unwrap();
    let p = dir.to_str().unwrap();

    for state in [file, hosted.url().as_str()] {
        ok(commit(&dir, state, &[]));
        ok(freshet(&["restate", "--project", p, "--model", "events", "--partitions", "2026-06-20", "--state", state]));
    }
    let on_disk: Value = serde_json::from_str(&std::fs::read_to_string(file).unwrap()).unwrap();
    assert!(
        !on_disk["watermarks"].as_object().unwrap().is_empty(),
        "the restatement left a ledger to carry: {on_disk}"
    );
    assert_eq!(hosted.snapshot(), on_disk, "hosted state holds the state file's document, unchanged");
    assert_eq!(hosted.version(), Some(2), "one plan commit, one ledger write");

    let first = &hosted.landed()[0];
    assert_eq!(first["expected_version"], Value::Null, "the first commit expects no prior state");
    assert_eq!(first["summary"]["command"], "plan --commit");
    assert_eq!(first["summary"]["models_rebuilt"], 2);
    assert!(first["summary"]["client"].as_str().unwrap().starts_with("freshet/"));
    let second = &hosted.landed()[1];
    assert_eq!(second["expected_version"], 1, "the ledger write compares against what it loaded");
    assert_eq!(second["summary"]["command"], "restate");
}

#[test]
fn a_warm_plan_diffs_against_the_hosted_snapshot() {
    let (hosted, dir) = (FakeHosted::start(), project("warm"));
    let out = ok(commit(&dir, &hosted.url(), &[]));
    assert!(stderr(&out).contains("committed state -> hosted state version 1"), "{}", stderr(&out));
    assert!(models(&plan_json(&dir, &hosted.url())).is_empty(), "committed: nothing left to do");

    write(&dir, "stg_orders.sql", "select order_id, customer_id, amount, amount * 2 as doubled from raw_orders");
    let plan = plan_json(&dir, &hosted.url());
    assert_eq!(
        models(&plan),
        ["stg_orders", "mart_revenue"],
        "what changed since the hosted commit and its downstream; the rest is skipped"
    );
}

// ------------------------------------------------------------- credentials --

#[test]
fn the_token_comes_from_the_environment_and_is_never_printed() {
    let (hosted, dir) = (FakeHosted::start(), project("token"));
    let p = dir.to_str().unwrap();

    let out = command(&["plan", "--project", p, "--state", &hosted.url()])
        .env_remove("FRESHET_STATE_TOKEN")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("FRESHET_STATE_TOKEN"), "{}", stderr(&out));
    assert!(hosted.seen().is_empty(), "nothing is sent without a token");

    let out = command(&["plan", "--project", p, "--state", &hosted.url(), "--commit"])
        .env("FRESHET_STATE_TOKEN", "tok-wrong-7d2e")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1), "a refused token is an error, not a conflict");
    let err = stderr(&out);
    assert!(err.contains("HTTP 401") && err.contains("invalid api token"), "the server's own words: {err}");
    assert!(!err.contains("tok-wrong-7d2e") && !stdout(&out).contains("tok-wrong-7d2e"), "{err}");

    let out = ok(commit(&dir, &hosted.url(), &[]));
    assert!(!stderr(&out).contains(TOKEN) && !stdout(&out).contains(TOKEN));
}

#[test]
fn a_state_url_carrying_credentials_is_refused_without_echoing_them() {
    let dir = project("userinfo");
    let out = freshet(&["plan", "--project", dir.to_str().unwrap(), "--state", "https://ci:hunter2@127.0.0.1:1/state/v1"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("credentials in the URL are refused"), "{}", stderr(&out));
    assert!(!stderr(&out).contains("hunter2"), "{}", stderr(&out));
}

// ----------------------------------------------------- the compare-and-swap --

#[test]
fn a_commit_that_loses_the_race_exits_3_says_re_plan_and_writes_nothing() {
    let (hosted, dir) = (FakeHosted::start(), project("lost"));
    ok(commit(&dir, &hosted.url(), &[]));
    write(&dir, "dim_customer.sql", "select customer_id, region, 1 as tier from raw_customers");
    // Another job commits while this one plans.
    hosted.with(|s| s.commit_after_next_get = true);

    let out = commit(&dir, &hosted.url(), &[]);
    assert_eq!(out.status.code(), Some(EXIT_STATE_CONFLICT), "{}", stderr(&out));
    let err = stderr(&out);
    assert!(err.contains("another commit landed after this plan was computed; re-plan"), "{err}");
    assert!(err.contains("computed against version 1") && err.contains("now at version 2"), "{err}");
    assert!(err.contains("Nothing was committed"), "{err}");
    assert!(!err.contains("Stack backtrace"), "an expected outcome, not a crash: {err}");
    assert_eq!(hosted.version(), Some(2), "only the other job's commit");
    assert_eq!(hosted.landed().len(), 1);

    // Re-planning against the winner's state is the answer, and it lands.
    let out = ok(commit(&dir, &hosted.url(), &[]));
    assert!(stderr(&out).contains("hosted state version 3"), "{}", stderr(&out));
    assert!(hosted.snapshot()["fingerprints"].get("someone_else").is_none(), "the re-plan committed its own state");
}

/// The window that matters is plan → run elsewhere → commit. `--expect-version`
/// carries the planned-against version across it, and a commit that landed in
/// between is caught before anything is sent.
#[test]
fn an_expect_version_the_state_has_moved_past_exits_3_before_posting() {
    let (hosted, dir) = (FakeHosted::start(), project("expect"));
    ok(commit(&dir, &hosted.url(), &["--expect-version", "none"]));
    // Someone else's later commit: version 2.
    write(&dir, "dim_customer.sql", "select customer_id, region, 2 as tier from raw_customers");
    ok(commit(&dir, &hosted.url(), &[]));

    write(&dir, "untouched_side.sql", "select customer_id, 1 as flag from dim_customer");
    let posts = hosted.posts();
    let out = commit(&dir, &hosted.url(), &["--expect-version", "1"]);
    assert_eq!(out.status.code(), Some(EXIT_STATE_CONFLICT), "{}", stderr(&out));
    assert!(stderr(&out).contains("computed against version 1, and the state is now at version 2"), "{}", stderr(&out));
    assert_eq!(hosted.posts(), posts, "refused before any commit was sent");

    ok(commit(&dir, &hosted.url(), &["--expect-version", "2"]));
    assert_eq!(hosted.version(), Some(3));
    let out = commit(&dir, &hosted.url(), &["--expect-version", "none"]);
    assert_eq!(out.status.code(), Some(EXIT_STATE_CONFLICT), "none means no state, and there is some");
}

/// A commit that would change nothing is not sent: it would record nothing, and a
/// version bump would make every other writer pinned to the current version
/// conflict — and re-run its plan — for no reason.
#[test]
fn an_unchanged_snapshot_is_not_committed_and_the_version_does_not_move() {
    let (hosted, dir) = (FakeHosted::start(), project("unchanged"));
    ok(commit(&dir, &hosted.url(), &[]));
    let posts = hosted.posts();
    let out = ok(commit(&dir, &hosted.url(), &["--expect-version", "1"]));
    assert!(stderr(&out).contains("state unchanged"), "{}", stderr(&out));
    assert!(stderr(&out).contains("nothing to commit"), "{}", stderr(&out));
    assert_eq!((hosted.posts(), hosted.version()), (posts, Some(1)));
}

#[test]
fn expect_version_is_for_hosted_commits_only() {
    let dir = project("expectfile");
    let file = dir.join("state.json");
    let out = commit(&dir, file.to_str().unwrap(), &["--expect-version", "1"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("--expect-version applies to hosted state"), "{}", stderr(&out));

    let out = freshet(&["plan", "--project", dir.to_str().unwrap(), "--expect-version", "1"]);
    assert_eq!(out.status.code(), Some(2), "without --commit it is a usage error: {}", stderr(&out));
    let out = freshet(&["plan", "--project", dir.to_str().unwrap(), "--commit", "--expect-version", "0"]);
    assert_eq!(out.status.code(), Some(2), "0 is not a version: {}", stderr(&out));
}

/// The race end to end: two CI jobs on two revisions of the project — one edits
/// `stg_orders`, the other does not — run `freshet plan --commit` against the same
/// hosted state at once. The fake holds both reads until both have arrived, so both
/// plan against the same version. Exactly one commit lands; the other exits 3 having
/// written nothing, and its re-plan against the winner's state rebuilds what the two
/// revisions disagree about — an over-rebuild, never a silently lost commit.
#[test]
fn two_concurrent_commits_one_lands_the_other_exits_3_and_its_re_plan_lands() {
    let hosted = FakeHosted::start();
    let (a, b) = (project("race-a"), project("race-b"));
    write(&b, "stg_orders.sql", "select order_id, customer_id, amount, 'eur' as currency from raw_orders");
    hosted.with(|s| s.hold_gets = 2);
    let url = hosted.url();
    let jobs: Vec<_> = [&a, &b]
        .into_iter()
        .map(|dir| {
            command(&["plan", "--project", dir.to_str().unwrap(), "--state", &url, "--commit"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    let outs: Vec<Output> = jobs.into_iter().map(|j| j.wait_with_output().unwrap()).collect();
    let codes: Vec<Option<i32>> = outs.iter().map(|o| o.status.code()).collect();
    let mut sorted = codes.clone();
    sorted.sort();
    assert_eq!(sorted, vec![Some(0), Some(EXIT_STATE_CONFLICT)], "{:?}", outs.iter().map(stderr).collect::<Vec<_>>());
    let lost = codes.iter().position(|c| *c == Some(EXIT_STATE_CONFLICT)).unwrap();
    let loser_err = stderr(&outs[lost]);
    assert!(loser_err.contains("another commit landed after this plan was computed; re-plan"), "{loser_err}");
    assert!(loser_err.contains("computed against no state, and the state is now at version 1"), "{loser_err}");
    assert_eq!(hosted.version(), Some(1), "exactly one commit landed");
    assert_eq!(hosted.landed().len(), 1);

    // The loser re-plans against the winner's commit and lands.
    hosted.with(|s| s.hold_gets = 0);
    let loser_dir = [&a, &b][lost];
    let out = ok(commit(loser_dir, &url, &[]));
    assert!(stderr(&out).contains("committed state -> hosted state version 2"), "{}", stderr(&out));
    assert_eq!(
        hosted.landed()[1]["summary"]["models_rebuilt"], 2,
        "stg_orders, where the revisions disagree, and its downstream mart"
    );
}

// -------------------------------------------------------------- lossless --

#[test]
fn hosted_state_this_freshet_cannot_keep_whole_is_refused_before_planning() {
    let (hosted, dir) = (FakeHosted::start(), project("extra"));
    hosted.with(|s| s.extra_field = true);
    let out = commit(&dir, &hosted.url(), &[]);
    assert_eq!(out.status.code(), Some(1));
    let err = stderr(&out);
    assert!(err.contains("lineage") && err.contains("upgrade freshet"), "{err}");
    assert_eq!(hosted.posts(), 0, "planning without the field, then committing, would drop it for good");
}

// ---------------------------------------------------- restate and submit --

#[test]
fn a_restatement_records_and_settles_its_ledger_in_hosted_state() {
    let (hosted, dir) = (FakeHosted::start(), incremental_project("restate"));
    let (p, url) = (dir.to_str().unwrap(), hosted.url());
    ok(commit(&dir, &url, &[]));

    let out = ok(freshet(&["restate", "--project", p, "--model", "events", "--partitions", "2026-06-20", "--state", &url]));
    let err = stderr(&out);
    assert!(err.contains("recorded as owed in hosted state version 2"), "{err}");
    assert!(err.contains("--commit --expect-version 2"), "{err}");

    // The run "dies": the ledger is what the next plan reads, from hosted state.
    let replan = plan_json(&dir, &url);
    let unit = |m: &str| replan["models"].as_array().unwrap().iter().find(|x| x["name"] == m).unwrap()["unit"].clone();
    assert_eq!(unit("events"), json!({ "partitions": ["2026-06-20"] }));
    assert_eq!(unit("rolling"), json!({ "partitions": ["2026-06-20", "2026-06-21"] }));

    ok(freshet(&[
        "restate", "--project", p, "--model", "events", "--partitions", "2026-06-20", "--state", &url, "--commit",
        "--expect-version", "2",
    ]));
    assert_eq!(hosted.version(), Some(3));
    assert!(models(&plan_json(&dir, &url)).is_empty(), "settled");
}

/// `submit` plans against hosted state, never writes it, carries the plan's own CU
/// estimate for a consumer that meters it, and names the version to commit against.
#[cfg(feature = "submit")]
#[test]
fn submit_plans_against_hosted_state_and_leaves_it_alone() {
    let (hosted, dir) = (FakeHosted::start(), project("submit"));
    let (p, url) = (dir.to_str().unwrap(), hosted.url());
    ok(commit(&dir, &url, &[]));
    write(&dir, "stg_orders.sql", "select order_id, customer_id, amount, 1 as v from raw_orders");

    let out = ok(freshet(&["submit", "--project", p, "--state", &url, "--dry-run"]));
    let body: Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(models(&body["plan"]), ["stg_orders", "mart_revenue"]);
    assert_eq!(body["plan"]["cost"]["total_millicu"], 2000, "two tables at the default rate card");

    // A consumer to submit to: one canned 201.
    let consumer = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let to = format!("http://{}/api/state/plans/submit", consumer.local_addr().unwrap());
    std::thread::spawn(move || {
        if let Ok((mut stream, _)) = consumer.accept() {
            let _ = read_request(&mut stream);
            let reply = r#"{"run_id":"run-9"}"#;
            let _ = write!(stream, "HTTP/1.1 201 Created\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}", reply.len());
        }
    });
    let out = ok(freshet(&["submit", "--project", p, "--state", &url, "--to", &to, "--command", "echo hi"]));
    assert!(stdout(&out).contains("run-9"), "{}", stdout(&out));
    let err = stderr(&out);
    assert!(err.contains("not committed"), "{err}");
    assert!(err.contains(&format!("--state {url} --commit --expect-version 1")), "{err}");
    assert_eq!(hosted.posts(), 1, "only the priming commit: submit never writes state");
    assert_eq!(hosted.version(), Some(1));
}
