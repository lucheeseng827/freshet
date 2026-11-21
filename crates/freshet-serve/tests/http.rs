//! The HTTP transport, over a real socket.
//!
//! These assert on bytes that actually crossed a TCP connection rather than on the
//! handler function that would have produced them. The transport's whole job is to be
//! a faithful pipe to `ProjectCache::handle_json`, and "faithful" is only checkable
//! from the outside.

#![cfg(feature = "http")]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use freshet_serve::{http, ProjectCache, Request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Start a daemon on an ephemeral port with a three-model project loaded.
async fn daemon() -> SocketAddr {
    daemon_with(None).await
}

/// [`daemon`], requiring `token` on `/rpc` when there is one.
async fn daemon_with(token: Option<&str>) -> SocketAddr {
    let cache = Arc::new(ProjectCache::new());
    cache.handle(Request::Reload {
        models: vec![
            ("stg".into(), "select a, b from raw".into()),
            ("mart_a".into(), "select a as x from stg".into()),
            ("mart_b".into(), "select b as y from stg".into()),
        ],
        sources: BTreeMap::from([("raw".to_string(), vec!["a".to_string(), "b".to_string()])]),
        materializations: BTreeMap::new(),
        incremental: Default::default(),
    });

    // Bind here and hand the listener over: binding to find a free port and then
    // dropping it leaves a window for another process to take that port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let token = token.map(str::to_string);
    tokio::spawn(async move { http::serve_on_with(cache, listener, token).await });
    addr
}

/// One exchange with extra request headers; the status line and headers, and the body.
async fn exchange(addr: SocketAddr, method: &str, path: &str, extra: &str, body: &[u8]) -> (String, String) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n{extra}\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (headers, body) = text.split_once("\r\n\r\n").expect("headers, blank line, body");
    (headers.to_string(), body.to_string())
}

/// A whole HTTP/1.1 exchange in twenty lines. `Connection: close` means the body is
/// "everything until EOF", which sidesteps chunked decoding.
async fn request(addr: SocketAddr, method: &str, path: &str, body: &[u8]) -> String {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (headers, body) = text
        .split_once("\r\n\r\n")
        .expect("headers, blank line, body");
    assert!(
        headers.contains("200 OK"),
        "expected 200, got headers:\n{headers}"
    );
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("content-type: application/json"),
        "every response is JSON; headers:\n{headers}"
    );
    body.to_string()
}

async fn rpc(addr: SocketAddr, body: &str) -> serde_json::Value {
    serde_json::from_str(&request(addr, "POST", "/rpc", body.as_bytes()).await).unwrap()
}

#[tokio::test]
async fn a_plan_over_http_carries_the_effort_map_and_the_queue() {
    let addr = daemon().await;
    let v = rpc(
        addr,
        r#"{"op":"plan","include_cost":true,"include_queue":true}"#,
    )
    .await;

    assert_eq!(v["status"], "plan");
    assert_eq!(
        v["plan"]["models"].as_array().unwrap().len(),
        3,
        "cold plan: all three"
    );
    assert_eq!(v["plan"]["cold"], true, "and it says so (contract v6)");

    // Three Table models on the default rate card: one whole CU (1_000 mCU) each.
    assert_eq!(v["plan"]["cost"]["total_millicu"], 3_000);
    let per_model = v["plan"]["cost"]["per_model"].as_array().unwrap();
    assert_eq!(per_model.len(), 3);
    assert_eq!(per_model[0]["basis"], "table_full");

    // stg, then the two independent marts.
    let waves = v["plan"]["queue"]["waves"].as_array().unwrap();
    assert_eq!(waves.len(), 2, "critical path is two deep");
    assert_eq!(waves[0].as_array().unwrap().len(), 1);
    assert_eq!(waves[1].as_array().unwrap().len(), 2);
}

/// The default response must stay exactly what it was before cost existed — the same
/// guarantee `freshet-embed` asserts, checked here through the transport.
#[tokio::test]
async fn a_plan_that_did_not_ask_for_cost_does_not_carry_it() {
    let addr = daemon().await;
    let raw = request(addr, "POST", "/rpc", br#"{"op":"plan"}"#).await;
    assert!(
        !raw.contains("cost"),
        "unasked-for cost must not appear: {raw}"
    );
    assert!(
        !raw.contains("queue"),
        "unasked-for queue must not appear: {raw}"
    );
}

/// A failure is a document, not a status code: every binding reads errors the same
/// way whether it reached the planner over HTTP, the C-ABI, or a direct call.
#[tokio::test]
async fn a_malformed_request_is_an_error_document_with_a_200() {
    let addr = daemon().await;
    let v = rpc(addr, "{not json").await;
    assert_eq!(v["status"], "error");
    assert!(v["message"].as_str().unwrap().contains("bad request"));
}

#[tokio::test]
async fn a_body_that_is_not_utf8_is_refused_before_the_funnel() {
    let addr = daemon().await;
    let body = request(addr, "POST", "/rpc", &[0x7b, 0xff, 0xfe, 0x7d]).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["status"], "error");
    assert!(v["message"].as_str().unwrap().contains("UTF-8"));
}

#[tokio::test]
async fn healthz_reports_the_project_revision() {
    let addr = daemon().await;
    let v: serde_json::Value =
        serde_json::from_str(&request(addr, "GET", "/healthz", b"").await).unwrap();
    assert_eq!(v["status"], "ok");
    assert_eq!(v["revision"], 1, "one reload happened at startup");
}

/// The plan -> commit handoff survives the request boundary, which is the whole reason
/// the daemon stages snapshots under a revision-stamped token.
#[tokio::test]
async fn plan_then_commit_over_http_settles_the_project() {
    let addr = daemon().await;
    let token = rpc(addr, r#"{"op":"plan"}"#).await["token"]
        .as_u64()
        .expect("a commit token");

    let committed = rpc(addr, &format!(r#"{{"op":"commit","token":{token}}}"#)).await;
    assert_eq!(committed["status"], "committed");

    // Nothing changed since the commit, so there is nothing left to rebuild.
    let after = rpc(addr, r#"{"op":"plan","include_cost":true}"#).await;
    assert!(after["plan"]["models"].as_array().unwrap().is_empty());
    assert_eq!(
        after["plan"]["cost"]["total_millicu"], 0,
        "an empty plan costs nothing"
    );
}

/// A reload's report crosses the wire beside the fields every client already reads, and
/// an unchanged project keeps its revision, which is what keeps a client's token good.
#[tokio::test]
async fn a_reload_over_http_reports_what_it_reparsed() {
    let addr = daemon().await;
    let same = r#"{"op":"reload","sources":{"raw":["a","b"]},"models":[
        ["stg","select a, b from raw"],
        ["mart_a","select a as x from stg"],
        ["mart_b","select b as y from stg"]]}"#;
    let v = rpc(addr, same).await;
    assert_eq!(v["status"], "reloaded");
    assert_eq!(v["models"], 3);
    assert_eq!(v["revision"], 1, "the project the daemon started with, unchanged");
    assert_eq!(v["reparsed"], 0);
    assert_eq!(v["full_rebuild"], false);

    let edited = same.replace("select a as x from stg", "select a as z from stg");
    let v = rpc(addr, &edited).await;
    assert_eq!(v["revision"], 2);
    assert_eq!(v["reparsed"], 1, "only mart_a, which nothing reads");
    assert_eq!(v["full_rebuild"], false);
}

#[test]
fn loopback_is_silent_and_anything_else_without_a_token_warns() {
    assert!(!http::warn_if_exposed("127.0.0.1:8788".parse().unwrap(), false));
    assert!(!http::warn_if_exposed("[::1]:8788".parse().unwrap(), false));
    assert!(http::warn_if_exposed("0.0.0.0:8788".parse().unwrap(), false));
    assert!(http::warn_if_exposed("192.168.1.10:8788".parse().unwrap(), false));
    // With a token it notes the clear-text transport, but the exposure is guarded.
    assert!(!http::warn_if_exposed("0.0.0.0:8788".parse().unwrap(), true));
}

const TOKEN: &str = "s3cret-token-for-the-test";

#[tokio::test]
async fn with_a_token_rpc_refuses_a_request_that_does_not_carry_it() {
    let addr = daemon_with(Some(TOKEN)).await;
    let plan = br#"{"op":"plan"}"#;
    for (why, extra) in [
        ("no header", String::new()),
        ("wrong token", "Authorization: Bearer nope\r\n".to_string()),
        ("same length, other bytes", format!("Authorization: Bearer {}\r\n", "x".repeat(TOKEN.len()))),
        ("not a bearer token", format!("Authorization: Basic {TOKEN}\r\n")),
    ] {
        let (headers, body) = exchange(addr, "POST", "/rpc", &extra, plan).await;
        assert!(headers.starts_with("HTTP/1.1 401"), "{why}: {headers}");
        assert!(headers.to_ascii_lowercase().contains("www-authenticate: bearer"), "{why}: {headers}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["status"], "error", "{why}");
        assert!(v["message"].as_str().unwrap().contains("FRESHET_SERVE_TOKEN"), "{why}: {body}");
    }
}

#[tokio::test]
async fn without_the_token_a_request_is_refused_before_its_body_is_read() {
    // A request that declares the largest body accepted and sends none of it. Were the
    // body read before the token was checked, the daemon would wait for 32 MiB that
    // never come; refused first, the answer is immediate.
    let addr = daemon_with(Some("s3cret")).await;
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST /rpc HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n",
        http::MAX_BODY_BYTES
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 64];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("answered without waiting for the body")
        .unwrap();
    let status = String::from_utf8_lossy(&buf[..n]).into_owned();
    assert!(status.starts_with("HTTP/1.1 401"), "{status}");
}

#[tokio::test]
async fn with_a_token_rpc_answers_a_request_that_carries_it_and_healthz_stays_open() {
    let addr = daemon_with(Some(TOKEN)).await;
    let (headers, body) =
        exchange(addr, "POST", "/rpc", &format!("Authorization: Bearer {TOKEN}\r\n"), br#"{"op":"plan"}"#).await;
    assert!(headers.starts_with("HTTP/1.1 200"), "{headers}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["status"], "plan", "{body}");
    // A liveness probe has no token to send, and learns only the revision.
    let (headers, body) = exchange(addr, "GET", "/healthz", "", b"").await;
    assert!(headers.starts_with("HTTP/1.1 200"), "{headers}");
    assert!(body.contains(r#""status":"ok""#), "{body}");
}
