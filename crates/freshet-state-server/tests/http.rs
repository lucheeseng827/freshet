//! The server over a real socket, spoken to with the client freshet-cli uses.
//!
//! The store's own rules (compare-and-swap, retention, the diff, crash safety) are
//! pinned in `src/store.rs`. What is pinned here is the protocol around them: status
//! codes, bodies, tokens, and what a bad request is told.

use freshet_state_server::console::ConsoleFiles;
use freshet_state_server::http::{self, Tokens};
use freshet_state_server::store::Store;
use serde_json::{json, Value};

/// A server on an ephemeral port, keeping `keep` commits a state. Its URL.
fn server(case: &str, tokens: Tokens, keep: usize) -> String {
    serve(case, tokens, keep, None)
}

fn serve(case: &str, tokens: Tokens, keep: usize, console: Option<ConsoleFiles>) -> String {
    let dir =
        std::env::temp_dir().join(format!("freshet-state-http-{case}-{}", std::process::id()));
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
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                match console {
                    None => http::serve_on(store, tokens, listener).await.unwrap(),
                    Some(files) => {
                        http::serve_app_on(http::app(store, tokens, Some(files)), listener)
                            .await
                            .unwrap()
                    }
                }
            });
    });
    format!("http://{addr}")
}

fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into()
}

/// (status, body as JSON, the WWW-Authenticate header) of a GET.
fn get(url: &str, token: Option<&str>) -> (u16, Value, Option<String>) {
    let mut req = agent().get(url);
    if let Some(t) = token {
        req = req.header("authorization", &format!("Bearer {t}"));
    }
    reply(req.call().unwrap())
}

fn post(url: &str, token: Option<&str>, body: &str) -> (u16, Value, Option<String>) {
    let mut req = agent().post(url).header("content-type", "application/json");
    if let Some(t) = token {
        req = req.header("authorization", &format!("Bearer {t}"));
    }
    reply(req.send(body).unwrap())
}

fn reply(mut resp: ureq::http::Response<ureq::Body>) -> (u16, Value, Option<String>) {
    let status = resp.status().as_u16();
    let challenge = resp
        .headers()
        .get("www-authenticate")
        .map(|v| v.to_str().unwrap().to_string());
    let text = resp.body_mut().read_to_string().unwrap();
    (
        status,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
        challenge,
    )
}

fn commit_body(expected: Value, fingerprints: Value) -> String {
    json!({"expected_version": expected, "snapshot": {"fingerprints": fingerprints, "watermarks": {}}, "summary": {"command": "test"}})
        .to_string()
}

#[test]
fn a_state_commits_by_compare_and_swap_and_keeps_u64s_exact() {
    let base = format!("{}/v1/analytics", server("cas", Tokens::default(), 10));

    let (status, body, _) = get(&format!("{base}/current"), None);
    assert_eq!(
        (status, body),
        (
            200,
            json!({"version": null, "snapshot": {"fingerprints": {}, "watermarks": {}}})
        )
    );

    // The digits go over the wire as text, so compare text: a parse through f64 would
    // round them and still compare equal to itself.
    let big = r#"{"expected_version":null,"snapshot":{"fingerprints":{"m":16899773155021735464},"watermarks":{}},"summary":null}"#;
    assert_eq!(post(&format!("{base}/commit"), None, big).0, 200);
    let text = agent()
        .get(&format!("{base}/current"))
        .call()
        .unwrap()
        .body_mut()
        .read_to_string()
        .unwrap();
    assert!(text.contains(r#""m":16899773155021735464"#), "{text}");
    assert!(text.contains(r#""version":1"#), "{text}");

    let (status, body, _) = post(
        &format!("{base}/commit"),
        None,
        &commit_body(json!(null), json!({"m": 1})),
    );
    assert_eq!(status, 409);
    assert_eq!(
        (body["expected"].clone(), body["found"].clone()),
        (json!(null), json!(1))
    );
    assert!(
        body["error"].as_str().unwrap().contains("re-plan"),
        "{body}"
    );

    let (status, body, _) = post(
        &format!("{base}/commit"),
        None,
        &commit_body(json!(1), json!({"m": 2, "n": 3})),
    );
    assert_eq!((status, body), (200, json!({"version": 2})));

    let (_, body, _) = get(&format!("{base}/history"), None);
    assert_eq!(body["limit"], 20);
    assert_eq!(body["next_before"], Value::Null);
    let commits = body["commits"].as_array().unwrap();
    assert_eq!(
        commits
            .iter()
            .map(|c| c["version"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        [2, 1]
    );
    assert_eq!(commits[0]["summary"]["changed"], json!(["m", "n"]));
    assert_eq!(commits[0]["summary"]["context"], json!({"command": "test"}));
    assert!(commits[0]["committed_at"].as_str().unwrap().ends_with('Z'));
}

#[test]
fn states_are_separate_and_history_pages_with_before() {
    let root = server("pages", Tokens::default(), 10);
    for v in 0..3 {
        let expected = if v == 0 { json!(null) } else { json!(v) };
        assert_eq!(
            post(
                &format!("{root}/v1/a/commit"),
                None,
                &commit_body(expected, json!({"m": v}))
            )
            .0,
            200
        );
    }
    assert_eq!(
        get(&format!("{root}/v1/b/current"), None).1["version"],
        Value::Null,
        "b is its own state"
    );

    let (_, first, _) = get(&format!("{root}/v1/a/history?limit=2"), None);
    assert_eq!(first["commits"].as_array().unwrap().len(), 2);
    assert_eq!(first["next_before"], 2);
    let (_, rest, _) = get(&format!("{root}/v1/a/history?limit=2&before=2"), None);
    assert_eq!(rest["commits"][0]["version"], 1);
    assert_eq!(rest["next_before"], Value::Null);
    assert_eq!(
        get(&format!("{root}/v1/a/history?limit=1000"), None).1["limit"],
        100,
        "a page is capped"
    );
}

#[test]
fn tokens_decide_who_reads_and_who_commits() {
    let root = server(
        "tokens",
        Tokens {
            commit: Some("c-token".into()),
            read: Some("r-token".into()),
        },
        10,
    );
    let base = format!("{root}/v1/p");

    let (status, body, challenge) = get(&format!("{base}/current"), None);
    assert_eq!((status, challenge.as_deref()), (401, Some("Bearer")));
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("FRESHET_STATE_SERVER_TOKEN"),
        "{body}"
    );
    assert_eq!(
        get(&format!("{base}/current"), Some("c-tokem")).0,
        401,
        "a near miss is a miss"
    );

    assert_eq!(get(&format!("{base}/current"), Some("r-token")).0, 200);
    assert_eq!(get(&format!("{base}/history"), Some("r-token")).0, 200);
    let (status, body, _) = post(
        &format!("{base}/commit"),
        Some("r-token"),
        &commit_body(json!(null), json!({})),
    );
    assert_eq!(status, 403);
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("may read state but not commit"),
        "{body}"
    );

    assert_eq!(
        post(
            &format!("{base}/commit"),
            Some("c-token"),
            &commit_body(json!(null), json!({}))
        )
        .0,
        200
    );
    assert_eq!(
        get(&format!("{root}/healthz"), None).0,
        200,
        "a liveness probe has no token"
    );
}

#[test]
fn a_commit_without_a_token_is_refused_before_its_body_is_read() {
    use std::io::{Read, Write};
    // Declares the largest commit accepted and sends none of it: read first, the
    // server would wait for 64 MiB that never come; refused first, it answers now.
    let root = server(
        "unread",
        Tokens {
            commit: Some("c-token".into()),
            read: None,
        },
        10,
    );
    let addr = root.trim_start_matches("http://");
    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let head = format!(
        "POST /v1/p/commit HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        http::MAX_BODY_BYTES
    );
    stream.write_all(head.as_bytes()).unwrap();
    let mut buf = [0u8; 64];
    let n = stream
        .read(&mut buf)
        .expect("answered without waiting for the body");
    let status = String::from_utf8_lossy(&buf[..n]).into_owned();
    assert!(status.starts_with("HTTP/1.1 401"), "{status}");
}

#[test]
fn a_bad_request_is_told_what_is_wrong() {
    let root = server("bad", Tokens::default(), 10);
    let commit = format!("{root}/v1/p/commit");
    let error = |(status, body, _): (u16, Value, Option<String>)| {
        (status, body["error"].as_str().unwrap_or("").to_string())
    };

    let (status, message) = error(post(&commit, None, "{not json"));
    assert_eq!(status, 400);
    assert!(message.contains("not JSON"), "{message}");

    let (status, message) = error(post(
        &commit,
        None,
        r#"{"snapshot":{"fingerprints":{},"watermarks":{}}}"#,
    ));
    assert_eq!(status, 400);
    assert!(message.contains("no `expected_version`"), "{message}");

    let (status, message) = error(post(
        &commit,
        None,
        &commit_body(json!(null), json!({"m": -1})),
    ));
    assert_eq!(status, 400);
    assert!(
        message.contains("not a planner state snapshot"),
        "{message}"
    );

    let extra =
        r#"{"expected_version":null,"snapshot":{"fingerprints":{},"watermarks":{},"schemes":{}}}"#;
    let (status, message) = error(post(&commit, None, extra));
    assert_eq!(status, 422);
    assert!(message.contains("cannot store"), "{message}");
    assert_eq!(
        get(&format!("{root}/v1/p/current"), None).1["version"],
        Value::Null,
        "nothing refused was written"
    );

    assert_eq!(get(&format!("{root}/v1/.lock/current"), None).0, 404);
    assert_eq!(get(&format!("{root}/v1/p/history?limit=0"), None).0, 400);
    assert_eq!(get(&format!("{root}/v1/p/history?before=abc"), None).0, 400);
}

#[test]
fn the_listing_names_each_state_and_says_what_the_token_may_do() {
    let root = server(
        "listing",
        Tokens {
            commit: Some("c-token".into()),
            read: Some("r-token".into()),
        },
        10,
    );
    for (name, models) in [("b", json!({"m": 1, "n": 2})), ("a", json!({"m": 1}))] {
        assert_eq!(
            post(
                &format!("{root}/v1/{name}/commit"),
                Some("c-token"),
                &commit_body(json!(null), models)
            )
            .0,
            200
        );
    }
    assert_eq!(
        get(&format!("{root}/v1/"), None).0,
        401,
        "the listing is a read like any other"
    );

    let (status, body, _) = get(&format!("{root}/v1/"), Some("r-token"));
    assert_eq!(status, 200);
    assert_eq!(body["access"], "read");
    let states = body["states"].as_array().unwrap();
    assert_eq!(
        states
            .iter()
            .map(|s| (
                s["name"].as_str().unwrap(),
                s["version"].as_i64().unwrap(),
                s["models"].as_u64().unwrap()
            ))
            .collect::<Vec<_>>(),
        [("a", 1, 1), ("b", 1, 2)]
    );
    assert!(states[0]["committed_at"].as_str().unwrap().ends_with('Z'));

    assert_eq!(
        get(&format!("{root}/v1"), Some("c-token")).1["access"],
        "commit"
    );
    assert_eq!(
        get(
            &format!("{}/v1/", server("listing-open", Tokens::default(), 10)),
            None
        )
        .1,
        json!({"states": [], "access": "open"}),
        "no tokens: anyone may, and the console says so"
    );
}

#[test]
fn a_kept_version_is_read_back_exactly_and_a_dropped_one_says_why() {
    let base = format!("{}/v1/p", server("versions", Tokens::default(), 2));
    let big = r#"{"expected_version":null,"snapshot":{"fingerprints":{"m":16899773155021735464},"watermarks":{"m":"{\"v\":1}"}},"summary":{"command":"first"}}"#;
    assert_eq!(post(&format!("{base}/commit"), None, big).0, 200);
    for v in 1..=2 {
        assert_eq!(
            post(
                &format!("{base}/commit"),
                None,
                &commit_body(json!(v), json!({"m": v + 1}))
            )
            .0,
            200
        );
    }

    let (status, body, _) = get(&format!("{base}/versions/2"), None);
    assert_eq!(status, 200);
    assert_eq!(body["snapshot"]["fingerprints"], json!({"m": 2}));
    assert_eq!(body["summary"]["changed"], json!(["m"]));
    assert!(body["committed_at"].as_str().unwrap().ends_with('Z'));

    let (status, body, _) = get(&format!("{base}/versions/1"), None);
    assert_eq!(status, 404, "keep 2: version 1 is gone");
    assert!(body["error"].as_str().unwrap().contains("--keep"), "{body}");
    assert_eq!(get(&format!("{base}/versions/0"), None).0, 400);
    assert_eq!(get(&format!("{base}/versions/x"), None).0, 400);
    assert_eq!(
        get(
            &format!("{}/v1/.lock/versions/1", base.trim_end_matches("/v1/p")),
            None
        )
        .0,
        404
    );

    // The newest version holds the first commit's u64 no longer; read one that does.
    let exact = format!("{}/v1/q", base.trim_end_matches("/v1/p"));
    assert_eq!(post(&format!("{exact}/commit"), None, big).0, 200);
    let text = agent()
        .get(&format!("{exact}/versions/1"))
        .call()
        .unwrap()
        .body_mut()
        .read_to_string()
        .unwrap();
    assert!(text.contains(r#""m":16899773155021735464"#), "{text}");
}

fn console_dir(case: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "freshet-state-console-{case}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("assets")).unwrap();
    std::fs::write(
        root.join("index.html"),
        "<!doctype html><title>Freshet</title>",
    )
    .unwrap();
    std::fs::write(root.join("assets/app.js"), "export {};").unwrap();
    std::fs::write(root.join(".secret"), "no").unwrap();
    root
}

/// (status, content-type, CSP, location) of a GET that does not follow redirects.
fn fetch(url: &str) -> (u16, Option<String>, Option<String>, Option<String>) {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .max_redirects(0)
        .build()
        .into();
    let resp = agent.get(url).call().unwrap();
    let h = |n: &str| {
        resp.headers()
            .get(n)
            .map(|v| v.to_str().unwrap().to_string())
    };
    (
        resp.status().as_u16(),
        h("content-type"),
        h("content-security-policy"),
        h("location"),
    )
}

#[test]
fn the_console_is_served_beside_the_api_under_its_policy() {
    let files = ConsoleFiles::new(console_dir("served")).unwrap();
    let root = serve(
        "console",
        Tokens {
            commit: Some("c-token".into()),
            read: None,
        },
        10,
        Some(files),
    );
    let (status, kind, csp, _) = fetch(&format!("{root}/console/"));
    assert_eq!(status, 200, "the page needs no token: what it reads does");
    assert_eq!(kind.as_deref(), Some("text/html; charset=utf-8"));
    assert!(csp
        .unwrap()
        .starts_with("default-src 'self'; script-src 'self'"));
    let (status, kind, _, _) = fetch(&format!("{root}/console/assets/app.js"));
    assert_eq!(
        (status, kind.as_deref()),
        (200, Some("text/javascript; charset=utf-8"))
    );

    assert_eq!(fetch(&format!("{root}/")).3.as_deref(), Some("/console/"));
    assert_eq!(
        fetch(&format!("{root}/console")).3.as_deref(),
        Some("/console/")
    );
    for missing in [
        "/console/.secret",
        "/console/../index.html",
        "/console/assets/nope.js",
        "/console/history",
    ] {
        assert_eq!(fetch(&format!("{root}{missing}")).0, 404, "{missing}");
    }
    assert_eq!(
        get(&format!("{root}/v1/"), None).0,
        401,
        "the API is unchanged beside it"
    );
}

#[test]
fn without_a_console_there_is_no_console() {
    let root = server("no-console", Tokens::default(), 10);
    assert_eq!(fetch(&format!("{root}/console/")).0, 404);
    assert_eq!(fetch(&format!("{root}/")).0, 404);
}
