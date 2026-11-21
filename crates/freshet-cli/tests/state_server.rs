//! Hosted state end to end: the real `freshet` binary against the real
//! `freshet-state-server`, started in this process on an ephemeral port.
//!
//! `tests/hosted_state.rs` pins the client against a fake that can provoke what a
//! real server rarely does on cue (a race, a reply it cannot keep). This file asks
//! the other question: does the server a team would actually run agree with the
//! client at every step of a real week — commit, edit, plan, conflict, history, pages,
//! retention, and a token that may only read.

#![cfg(feature = "hosted")]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use freshet_state_server::http::{self, Tokens};
use freshet_state_server::store::Store;
use serde_json::Value;

const EXIT_STATE_CONFLICT: i32 = 3;

/// A server keeping `keep` commits a state; the URL of the state called `case` on it.
fn server(case: &str, tokens: Tokens, keep: usize) -> String {
    let dir = std::env::temp_dir().join(format!(
        "freshet-state-server-cli-{case}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let store = Store::open(&dir, keep).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                http::serve_on(
                    store,
                    tokens,
                    tokio::net::TcpListener::from_std(listener).unwrap(),
                )
                .await
                .unwrap();
            });
    });
    format!("http://{addr}/v1/{case}")
}

fn project(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "freshet-state-server-project-{case}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write(
        &dir,
        "stg_orders.sql",
        "select order_id, customer_id, amount from raw_orders",
    );
    write(
        &dir,
        "dim_customer.sql",
        "select customer_id, region from raw_customers",
    );
    write(
        &dir,
        "mart_revenue.sql",
        "select c.region, sum(o.amount) as revenue from stg_orders o \
         join dim_customer c on o.customer_id = c.customer_id group by c.region",
    );
    write(
        &dir,
        "sources.json",
        r#"{"raw_orders":["order_id","customer_id","amount"],"raw_customers":["customer_id","region"]}"#,
    );
    dir
}

fn write(dir: &Path, name: &str, body: &str) {
    std::fs::write(dir.join(name), body).unwrap();
}

fn freshet_as(token: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_freshet"))
        .args(args)
        .env("FRESHET_STATE_TOKEN", token)
        .env_remove("RUST_BACKTRACE")
        .output()
        .unwrap()
}

fn freshet(args: &[&str]) -> Output {
    freshet_as("any-token", args)
}

fn ok(out: Output) -> String {
    assert!(
        out.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        text(&out.stdout),
        text(&out.stderr)
    );
    text(&out.stdout)
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn planned(dir: &Path, state: &str) -> Vec<String> {
    let out = ok(freshet(&[
        "plan",
        "--project",
        dir.to_str().unwrap(),
        "--state",
        state,
        "--json",
    ]));
    let plan: Value = serde_json::from_str(&out).unwrap();
    plan["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap().to_string())
        .collect()
}

fn commit(dir: &Path, state: &str, extra: &[&str]) -> Output {
    let mut args = vec![
        "plan",
        "--project",
        dir.to_str().unwrap(),
        "--state",
        state,
        "--commit",
    ];
    args.extend_from_slice(extra);
    freshet(&args)
}

#[test]
fn a_week_of_commits_against_a_server_the_team_runs() {
    let (state, dir) = (server("week", Tokens::default(), 100), project("week"));

    assert_eq!(
        planned(&dir, &state),
        ["dim_customer", "stg_orders", "mart_revenue"],
        "cold: everything"
    );
    ok(commit(&dir, &state, &[]));
    assert!(planned(&dir, &state).is_empty(), "committed: nothing left");

    write(
        &dir,
        "stg_orders.sql",
        "select order_id, customer_id, amount from raw_orders where amount > 0",
    );
    assert_eq!(
        planned(&dir, &state),
        ["stg_orders", "mart_revenue"],
        "the edit and what reads it"
    );
    let out = commit(&dir, &state, &["--expect-version", "1"]);
    assert!(
        text(&out.stderr).contains("version 2"),
        "{}",
        text(&out.stderr)
    );
    assert!(planned(&dir, &state).is_empty());

    // A job that planned against version 1 lost the race.
    write(
        &dir,
        "dim_customer.sql",
        "select customer_id, region, 1 as one from raw_customers",
    );
    let out = commit(&dir, &state, &["--expect-version", "1"]);
    assert_eq!(
        out.status.code(),
        Some(EXIT_STATE_CONFLICT),
        "{}",
        text(&out.stderr)
    );
    assert!(
        text(&out.stderr).contains("re-plan"),
        "{}",
        text(&out.stderr)
    );

    let history = ok(freshet(&["history", "--state", &state]));
    let lines: Vec<&str> = history.lines().collect();
    assert_eq!(
        lines[0],
        format!("{state}: 2 commit(s), newest first"),
        "{history}"
    );
    assert!(
        lines[1].starts_with("  v2 ") && lines[1].contains("1 changed (stg_orders)"),
        "{history}"
    );
    assert!(
        lines[2].starts_with("  v1 ") && lines[2].contains("3 changed"),
        "{history}"
    );
    assert!(
        lines[1].contains(r#""command":"plan --commit""#),
        "the committer's words: {history}"
    );
    assert_eq!(lines.len(), 3, "nothing older to point at: {history}");
}

#[test]
fn history_pages_with_before_and_says_what_retention_dropped() {
    let (state, dir) = (server("pages", Tokens::default(), 3), project("pages"));
    for v in 0..4 {
        write(
            &dir,
            "stg_orders.sql",
            &format!("select order_id, customer_id, amount + {v} as amount from raw_orders"),
        );
        let expect = v.to_string();
        let extra: &[&str] = if v == 0 {
            &[]
        } else {
            &["--expect-version", &expect]
        };
        ok(commit(&dir, &state, extra));
    }

    let first = ok(freshet(&["history", "--state", &state, "--limit", "2"]));
    assert!(
        first.contains("  v4 ") && first.contains("  v3 ") && !first.contains("  v2 "),
        "{first}"
    );
    assert!(
        first.contains(&format!(
            "… older commits: freshet history --state {state} --before 3"
        )),
        "{first}"
    );

    let rest = ok(freshet(&[
        "history", "--state", &state, "--limit", "2", "--before", "3",
    ]));
    assert!(rest.contains("  v2 ") && !rest.contains("  v3 "), "{rest}");
    assert!(
        rest.contains("(commits before v2 are no longer kept"),
        "kept 3 of 4: {rest}"
    );

    let json: Value = serde_json::from_str(&ok(freshet(&[
        "history", "--state", &state, "--limit", "2", "--json",
    ])))
    .unwrap();
    assert_eq!(json["next_before"], 3);
}

#[test]
fn a_read_only_token_plans_but_cannot_commit() {
    let tokens = Tokens {
        commit: Some("writer-7c1e".into()),
        read: Some("reader-04ab".into()),
    };
    let (state, dir) = (server("readonly", tokens, 100), project("readonly"));
    let path = dir.to_str().unwrap();

    ok(freshet_as(
        "writer-7c1e",
        &["plan", "--project", path, "--state", &state, "--commit"],
    ));
    let plan = ok(freshet_as(
        "reader-04ab",
        &["plan", "--project", path, "--state", &state],
    ));
    assert!(plan.contains("nothing to backfill"), "{plan}");
    ok(freshet_as("reader-04ab", &["history", "--state", &state]));

    write(
        &dir,
        "stg_orders.sql",
        "select order_id, customer_id, amount * 2 as amount from raw_orders",
    );
    let out = freshet_as(
        "reader-04ab",
        &["plan", "--project", path, "--state", &state, "--commit"],
    );
    assert!(!out.status.success());
    let err = text(&out.stderr);
    assert!(
        err.contains("HTTP 403") && err.contains("may read state but not commit"),
        "{err}"
    );
    assert!(
        !err.contains("reader-04ab"),
        "the token is never printed: {err}"
    );

    let out = freshet_as("guess", &["history", "--state", &state]);
    assert!(
        text(&out.stderr).contains("HTTP 401"),
        "{}",
        text(&out.stderr)
    );
}
