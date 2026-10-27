//! What `freshet submit` puts on the wire, and what it does with the answer.
//!
//! Two halves, deliberately kept apart:
//!
//! * **Shape** — asserted through `--dry-run`, which builds the exact request body
//!   and prints it instead of sending it. No socket, no server, no flake.
//! * **Transport** — asserted against [`Stub`], forty lines of `TcpListener` that
//!   record the raw request and reply with whatever the test says. A real HTTP
//!   server would hide the very bytes these tests exist to pin down.
//!
//! The stub is not a fake dagron and does not pretend to be one: it never compiles
//! anything. Whether a plan compiles is `dagron-state`'s question, tested on that
//! side against dagron's own parser. The question *here* is narrower — does the CLI
//! send the right thing to the right place, and report the answer honestly.

// The subcommand under test is optional (`--no-default-features` drops the HTTP
// client entirely), so this file has to be optional with it.
#![cfg(feature = "submit")]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};

use serde_json::Value;

// ---------------------------------------------------------------- fixtures --

/// A four-model project: two roots, a fan-in mart, and a model on a branch the
/// edits below never touch — so a warm plan has something to *skip*.
fn project(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("freshet-submit-{case}"));
    let models = dir.join("models");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&models).unwrap();

    write(&models, "stg_orders.sql", "select order_id, customer_id, amount from raw_orders");
    write(&models, "dim_customer.sql", "select customer_id, region from raw_customers");
    write(
        &models,
        "mart_revenue.sql",
        "select c.region, sum(o.amount) as revenue from stg_orders o \
         join dim_customer c on o.customer_id = c.customer_id group by c.region",
    );
    write(&models, "untouched_side.sql", "select customer_id from dim_customer");
    write(
        &models,
        "sources.json",
        r#"{"raw_orders":["order_id","customer_id","amount"],
            "raw_customers":["customer_id","region"]}"#,
    );
    dir
}

fn write(dir: &Path, name: &str, body: &str) {
    std::fs::write(dir.join(name), body).unwrap();
}

fn freshet(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_freshet")).args(args).output().unwrap()
}

fn stdout(out: &Output) -> String {
    String::from_utf8(out.stdout.clone()).unwrap()
}

fn stderr(out: &Output) -> String {
    String::from_utf8(out.stderr.clone()).unwrap()
}

/// Commit the cold plan, then edit exactly one root. The warm plan that follows
/// rebuilds `stg_orders` and its downstream `mart_revenue`, and skips the other two.
fn warm_up(dir: &Path) {
    let models = dir.join("models");
    let state = dir.join("state.json");
    let out = freshet(&[
        "plan",
        "--project",
        models.to_str().unwrap(),
        "--state",
        state.to_str().unwrap(),
        "--commit",
    ]);
    assert!(out.status.success(), "priming plan failed: {}", stderr(&out));
    write(&models, "stg_orders.sql", "select order_id, customer_id, amount, currency from raw_orders");
    write(
        &models,
        "sources.json",
        r#"{"raw_orders":["order_id","customer_id","amount","currency"],
            "raw_customers":["customer_id","region"]}"#,
    );
}

/// The request body `submit` would have sent.
fn envelope(dir: &Path, extra: &[&str]) -> Value {
    let models = dir.join("models");
    let state = dir.join("state.json");
    let mut args = vec![
        "submit",
        "--project",
        models.to_str().unwrap(),
        "--state",
        state.to_str().unwrap(),
        "--dry-run",
    ];
    args.extend_from_slice(extra);
    let out = freshet(&args);
    assert!(out.status.success(), "submit --dry-run failed: {}", stderr(&out));
    serde_json::from_str(&stdout(&out)).expect("--dry-run must print parseable JSON")
}

fn names(plan: &Value) -> Vec<String> {
    plan["plan"]["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap().to_string())
        .collect()
}

// -------------------------------------------------------------------- shape --

#[test]
fn the_body_is_the_plan_the_planner_already_speaks() {
    let dir = project("body");
    warm_up(&dir);
    let env = envelope(&dir, &["--command", "echo hi"]);

    assert_eq!(names(&env), ["stg_orders", "mart_revenue"]);
    // Contract v2: the edges travel with the plan, so a consumer needs nothing else
    // to order the run. This is the M6 change being used for what it was for.
    assert_eq!(env["plan"]["models"][0]["depends_on"], serde_json::json!([]));
    assert_eq!(env["plan"]["models"][1]["depends_on"], serde_json::json!(["stg_orders"]));
}

#[test]
fn the_graph_carries_the_models_the_plan_skipped() {
    let dir = project("graph");
    warm_up(&dir);
    let env = envelope(&dir, &[]);

    let graph: BTreeMap<String, Vec<String>> = serde_json::from_value(env["graph"].clone()).unwrap();
    assert_eq!(
        graph.keys().collect::<Vec<_>>(),
        ["dim_customer", "mart_revenue", "stg_orders", "untouched_side"],
        "all four models, not the two being rebuilt — this is the whole point of \
         sending a graph: only it can answer \"2 of 4\", and nothing but the \
         planner has it"
    );
    assert_eq!(graph["mart_revenue"], ["dim_customer", "stg_orders"]);
    assert_eq!(names(&env).len(), 2, "while the plan itself still rebuilds only two");
}

#[test]
fn no_graph_leaves_the_key_out_entirely() {
    let dir = project("nograph");
    warm_up(&dir);
    let env = envelope(&dir, &["--no-graph"]);
    assert!(env.get("graph").is_none(), "got: {env}");
}

#[test]
fn placeholders_reach_the_wire_unexpanded() {
    let dir = project("placeholders");
    warm_up(&dir);
    let env = envelope(&dir, &["--command", "dbt run --select {{ model }} --vars {{ partitions }}"]);

    assert_eq!(
        env["options"]["command_template"],
        serde_json::json!(["sh", "-c", "dbt run --select {{ model }} --vars {{ partitions }}"]),
        "`{{{{ model }}}}` is the consumer's templating. If freshet ever expands it \
         here, it has started speaking a language it does not own — and it would \
         have to expand it once per model, which is a run graph, which is exactly \
         the thing that lives on the other side of this contract."
    );
}

#[test]
fn no_options_means_no_options_key() {
    let dir = project("nooptions");
    warm_up(&dir);
    let env = envelope(&dir, &[]);
    assert!(
        env.get("options").is_none(),
        "an empty `{{}}` would be a malformed options object to a consumer that \
         requires a field inside it; omitting the key lets its own default and its \
         own error message apply. got: {env}"
    );
}

#[test]
fn an_options_file_is_forwarded_whole() {
    let dir = project("optionsfile");
    warm_up(&dir);
    let opts = dir.join("opts.json");
    std::fs::write(
        &opts,
        r#"{"workflow_name":"nightly","tags":["state-plan","nightly"],"max_attempts":3}"#,
    )
    .unwrap();

    let env = envelope(&dir, &["--options-file", opts.to_str().unwrap(), "--command", "echo hi"]);
    assert_eq!(env["options"]["workflow_name"], "nightly");
    assert_eq!(env["options"]["max_attempts"], 3);
    assert_eq!(env["options"]["tags"], serde_json::json!(["state-plan", "nightly"]));
    assert_eq!(
        env["options"]["command_template"],
        serde_json::json!(["sh", "-c", "echo hi"]),
        "--command wins over the same key in the file"
    );
}

// ---------------------------------------------------------------- transport --

/// A recorded HTTP request, split just far enough to assert on.
struct Seen {
    request_line: String,
    headers: Vec<(String, String)>,
    body: String,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// A listener that answers every request with one canned response and remembers
/// what it was asked.
struct Stub {
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Stub {
    fn replying(status: u16, reason: &str, body: &str) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));

        let recorder = Arc::clone(&seen);
        let response = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        // Detached on purpose: a test that expects *no* request must not have to
        // join a thread that is still blocked in accept(). The process reaps it.
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                if let Some(req) = read_request(&mut stream) {
                    recorder.lock().unwrap().push(req);
                }
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });

        Self { addr, seen }
    }

    fn url(&self) -> String {
        format!("http://{}/api/state/plans/submit", self.addr)
    }

    fn requests(&self) -> std::sync::MutexGuard<'_, Vec<Seen>> {
        self.seen.lock().unwrap()
    }
}

fn read_request(stream: &mut TcpStream) -> Option<Seen> {
    let mut reader = BufReader::new(stream.try_clone().ok()?);

    let mut request_line = String::new();
    reader.read_line(&mut request_line).ok()?;

    let mut headers = Vec::new();
    let mut length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).ok()? == 0 {
            break;
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            if k.eq_ignore_ascii_case("content-length") {
                length = v.trim().parse().unwrap_or(0);
            }
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }

    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).ok()?;
    Some(Seen {
        request_line: request_line.trim_end().to_string(),
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

fn submit_to(dir: &Path, url: &str, extra: &[&str]) -> Output {
    let models = dir.join("models");
    let state = dir.join("state.json");
    let mut args = vec![
        "submit",
        "--project",
        models.to_str().unwrap(),
        "--state",
        state.to_str().unwrap(),
        "--to",
        url,
    ];
    args.extend_from_slice(extra);
    freshet(&args)
}

#[test]
fn it_posts_json_to_exactly_the_url_it_was_given() {
    let dir = project("post");
    warm_up(&dir);
    let stub = Stub::replying(201, "Created", r#"{"run_id":"run_7","model_count":2}"#);

    let out = submit_to(&dir, &stub.url(), &["--command", "echo hi"]);
    assert!(out.status.success(), "{}", stderr(&out));

    let seen = stub.requests();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].request_line, "POST /api/state/plans/submit HTTP/1.1");
    assert_eq!(seen[0].header("content-type"), Some("application/json"));

    let body: Value = serde_json::from_str(&seen[0].body).expect("the body must be JSON");
    assert_eq!(names(&body), ["stg_orders", "mart_revenue"]);
}

#[test]
fn the_run_id_the_consumer_names_is_the_one_reported() {
    let dir = project("runid");
    warm_up(&dir);
    let stub = Stub::replying(201, "Created", r#"{"run_id":"run_7","model_count":2}"#);

    let out = submit_to(&dir, &stub.url(), &["--command", "echo hi"]);
    assert!(stdout(&out).contains("run_7"), "got: {}", stdout(&out));
    assert!(stdout(&out).contains("2 model(s)"), "got: {}", stdout(&out));
}

#[test]
fn a_response_without_a_run_id_is_still_a_success() {
    let dir = project("noid");
    warm_up(&dir);
    // A consumer that is not dagron, answering in its own shape. Rejecting this
    // would be freshet asserting a response schema it does not own.
    let stub = Stub::replying(200, "OK", r#"{"accepted":true}"#);

    let out = submit_to(&dir, &stub.url(), &["--command", "echo hi"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stdout(&out).contains("accepted"), "the raw answer is shown: {}", stdout(&out));
}

#[test]
fn a_refusal_is_reported_in_the_consumers_own_words() {
    let dir = project("refused");
    warm_up(&dir);
    let stub = Stub::replying(400, "Bad Request", "options.command_template is required");

    let out = submit_to(&dir, &stub.url(), &[]);
    assert!(!out.status.success(), "a refusal must not exit 0");
    assert!(
        stderr(&out).contains("options.command_template is required"),
        "the consumer's message is the useful part of a 4xx and must survive: {}",
        stderr(&out)
    );
    assert!(stderr(&out).contains("400"), "got: {}", stderr(&out));
}

#[test]
fn custom_headers_are_sent_as_given() {
    let dir = project("headers");
    warm_up(&dir);
    let stub = Stub::replying(201, "Created", r#"{"run_id":"r1"}"#);

    let out = submit_to(
        &dir,
        &stub.url(),
        &["--command", "echo hi", "--header", "Authorization: Bearer tok123", "--header", "X-Trace: abc"],
    );
    assert!(out.status.success(), "{}", stderr(&out));

    let seen = stub.requests();
    assert_eq!(seen[0].header("authorization"), Some("Bearer tok123"));
    assert_eq!(seen[0].header("x-trace"), Some("abc"));
}

#[test]
fn a_dry_run_opens_no_socket() {
    let dir = project("drysocket");
    warm_up(&dir);
    let stub = Stub::replying(201, "Created", r#"{"run_id":"nope"}"#);

    let out = submit_to(&dir, &stub.url(), &["--dry-run", "--command", "echo hi"]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stub.requests().is_empty(), "--dry-run must not reach the network");
    serde_json::from_str::<Value>(&stdout(&out)).expect("it prints the body it would have sent");
}

#[test]
fn a_dry_run_prints_json_even_when_there_is_nothing_to_rebuild() {
    // The gap the other dry-run tests miss: every helper here plans against a fresh
    // state file, so none of them ever reaches submit's empty-plan branch. The
    // README pipes `--dry-run` stdout into `dagron-state explain -`, and prose there
    // is a parse error for whoever is downstream.
    let dir = project("dryempty");
    let models = dir.join("models");
    let state = dir.join("state.json");
    let primed = freshet(&[
        "plan",
        "--project",
        models.to_str().unwrap(),
        "--state",
        state.to_str().unwrap(),
        "--commit",
    ]);
    assert!(primed.status.success(), "{}", stderr(&primed));
    // No edit follows, so the plan is empty.

    let stub = Stub::replying(500, "Internal Server Error", "should never be asked");
    let out = submit_to(&dir, &stub.url(), &["--dry-run", "--command", "echo hi"]);

    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stub.requests().is_empty(), "still no socket");

    let body: Value = serde_json::from_str(&stdout(&out))
        .expect("stdout must stay machine-readable on an empty plan");
    assert_eq!(
        body["plan"]["models"].as_array().unwrap().len(),
        0,
        "and it should be an honest empty plan, not a fabricated one"
    );
    assert!(
        stderr(&out).contains("nothing to backfill"),
        "the human note moves to stderr rather than vanishing: {}",
        stderr(&out)
    );
}

#[test]
fn an_up_to_date_project_submits_nothing() {
    let dir = project("uptodate");
    let models = dir.join("models");
    let state = dir.join("state.json");
    let primed = freshet(&[
        "plan",
        "--project",
        models.to_str().unwrap(),
        "--state",
        state.to_str().unwrap(),
        "--commit",
    ]);
    assert!(primed.status.success(), "{}", stderr(&primed));
    // No edit follows, so there is nothing to rebuild.

    let stub = Stub::replying(500, "Internal Server Error", "should never be asked");
    let out = submit_to(&dir, &stub.url(), &["--command", "echo hi"]);

    assert!(out.status.success(), "an empty plan is the good outcome: {}", stderr(&out));
    assert!(
        stub.requests().is_empty(),
        "a caller planning on every commit hits this constantly; making it a \
         round-trip — or an error — would make submit unusable in that loop"
    );
    assert!(stdout(&out).contains("nothing to backfill"), "got: {}", stdout(&out));
}

#[test]
fn a_successful_submit_does_not_advance_state() {
    let dir = project("nocommit");
    warm_up(&dir);
    let state = dir.join("state.json");
    let before = std::fs::read_to_string(&state).unwrap();

    let stub = Stub::replying(201, "Created", r#"{"run_id":"run_9"}"#);
    let out = submit_to(&dir, &stub.url(), &["--command", "echo hi"]);
    assert!(out.status.success(), "{}", stderr(&out));

    assert_eq!(
        std::fs::read_to_string(&state).unwrap(),
        before,
        "the run has been created, not finished. Advancing fingerprints here would \
         record models as rebuilt that a failed run never touched — the under-rebuild \
         bug this planner exists to prevent."
    );
    assert!(
        stderr(&out).contains("not committed"),
        "and it should say so rather than leaving the operator to notice: {}",
        stderr(&out)
    );
}
