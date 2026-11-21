//! `--watch` on a real directory: an edit is a reload, a broken edit keeps the
//! previous project serving, a file the project does not read reloads nothing, and
//! dropping the watch stops it.

#![cfg(feature = "watch")]

use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use freshet_serve::{watch, ProjectCache, ProjectFiles, Request, Response};

fn dir(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("freshet-watch-{case}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write(dir: &Path, name: &str, body: &str) {
    std::fs::write(dir.join(name), body).unwrap();
}

/// A loaded cache over `dir`, and a watch on it reporting each reload to the channel.
fn watched(dir: &Path) -> (Arc<ProjectCache>, mpsc::Receiver<Response>, watch::Watching) {
    write(dir, "sources.json", r#"{"raw": ["a", "b"]}"#);
    write(dir, "stg.sql", "select a, b from raw");
    write(dir, "mart.sql", "select a from stg");
    let files = ProjectFiles {
        dir: dir.to_path_buf(),
        sources: Some(dir.join("sources.json")),
        materializations: None,
        incremental: None,
    };
    let cache = Arc::new(ProjectCache::new());
    assert!(matches!(cache.handle(files.reload_request().unwrap()), Response::Reloaded { .. }));
    let (tx, rx) = mpsc::channel();
    let watching = watch::start(cache.clone(), files, Duration::from_millis(50), move |r| {
        let _ = tx.send(r.clone());
    })
    .unwrap();
    (cache, rx, watching)
}

fn next(rx: &mpsc::Receiver<Response>) -> Response {
    rx.recv_timeout(Duration::from_secs(10)).expect("a reload within 10 s of the edit")
}

fn graph(cache: &ProjectCache) -> Vec<String> {
    match cache.handle(Request::Graph) {
        Response::Graph { order, .. } => order,
        other => panic!("{other:?}"),
    }
}

#[test]
fn an_edit_is_a_reload_and_a_broken_one_keeps_the_previous_project() {
    let d = dir("edit");
    let (cache, rx, _watching) = watched(&d);
    let before = cache.revision();

    write(&d, "mart.sql", "select a, b as bb from stg");
    match next(&rx) {
        Response::Reloaded { reparsed, full_rebuild, .. } => {
            assert_eq!((reparsed, full_rebuild), (1, false), "only the edited model is reparsed");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(cache.revision(), before + 1);

    // Caught mid-edit: the reload fails, and the last good project keeps serving.
    write(&d, "mart.sql", "selec a from");
    assert!(matches!(next(&rx), Response::Error { .. }));
    assert_eq!(cache.revision(), before + 1);
    assert_eq!(graph(&cache), ["stg", "mart"]);

    // A new model and a changed source file are reloads too.
    write(&d, "mart.sql", "select a from stg");
    write(&d, "extra.sql", "select b from raw");
    assert!(matches!(next(&rx), Response::Reloaded { .. }));
    while rx.recv_timeout(Duration::from_millis(300)).is_ok() {}
    assert!(graph(&cache).contains(&"extra".to_string()));
    write(&d, "sources.json", r#"{"raw": ["a", "b", "c"]}"#);
    assert!(matches!(next(&rx), Response::Reloaded { full_rebuild: true, .. }));
}

#[test]
fn a_file_the_project_does_not_read_reloads_nothing_and_dropping_the_watch_stops_it() {
    let d = dir("quiet");
    let (cache, rx, watching) = watched(&d);
    let before = cache.revision();
    write(&d, "notes.txt", "not a model");
    assert!(rx.recv_timeout(Duration::from_millis(500)).is_err(), "a .txt file is not the project");

    drop(watching);
    write(&d, "mart.sql", "select b from stg");
    assert!(rx.recv_timeout(Duration::from_millis(500)).is_err(), "no watch, no reload");
    assert_eq!(cache.revision(), before);
}
