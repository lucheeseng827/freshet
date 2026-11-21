//! The console's files (`apps/freshet-console`), served at `/console/` on the same
//! origin as the API, so the page needs no CORS and its token never crosses an origin.
//!
//! The console is a static page that reads state and history; it never commits, so a
//! state has one write path whether or not anyone has the page open. Serving it is
//! off until `--console` names its directory.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use axum::extract::Path as UrlPath;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::Router;

/// Everything is same-origin: scripts, styles, fonts and the API the page calls.
/// `data:` images are the grain texture's inline SVG. No inline script or inline
/// style is allowed, so the page sets its few dynamic sizes through the CSSOM.
pub const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self'; \
img-src 'self' data:; font-src 'self'; connect-src 'self'; object-src 'none'; \
base-uri 'none'; frame-ancestors 'none'; form-action 'self'";

/// The console's directory, checked.
pub struct ConsoleFiles {
    root: PathBuf,
}

impl ConsoleFiles {
    /// Refuses a directory that does not exist or has no `index.html`: a console that
    /// is configured and cannot be served is a 404 nobody can explain.
    pub fn new(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        let root = std::fs::canonicalize(dir)
            .with_context(|| format!("the console directory {}", dir.display()))?;
        if !root.is_dir() {
            bail!(
                "the console directory {} is not a directory",
                root.display()
            );
        }
        if !root.join("index.html").is_file() {
            bail!(
                "the console directory {} has no index.html: point it at apps/freshet-console",
                root.display()
            );
        }
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The file a request path names, or `None`. Every segment must be a plain name
    /// (not empty, not a dot-file, no backslash, NUL or drive colon), and the path
    /// must still sit under the root once symlinks are followed. The page routes
    /// itself with `#/…`, so a missing file is a 404, never `index.html`.
    pub fn resolve(&self, rel: &str) -> Option<PathBuf> {
        let rel = rel.trim_matches('/');
        if rel.is_empty() {
            return Some(self.root.join("index.html"));
        }
        let mut candidate = PathBuf::new();
        for seg in rel.split('/') {
            if seg.is_empty() || seg.starts_with('.') || seg.contains(['\\', '\0', ':']) {
                return None;
            }
            candidate.push(seg);
        }
        let real = std::fs::canonicalize(self.root.join(candidate)).ok()?;
        (real.starts_with(&self.root) && real.is_file()).then_some(real)
    }
}

/// `/` and `/console` → `/console/`, and every file under it.
pub fn routes(files: ConsoleFiles) -> Router {
    let files = Arc::new(files);
    let at_root = files.clone();
    Router::new()
        .route("/", get(|| async { Redirect::temporary("/console/") }))
        .route(
            "/console",
            get(|| async { Redirect::permanent("/console/") }),
        )
        .route(
            "/console/",
            get(move || serve(at_root.clone(), String::new())),
        )
        .route(
            "/console/{*path}",
            get(move |UrlPath(path): UrlPath<String>| serve(files.clone(), path)),
        )
}

async fn serve(files: Arc<ConsoleFiles>, rel: String) -> Response {
    let Some(path) = files.resolve(&rel) else {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    };
    let body = match tokio::fs::read(&path).await {
        Ok(body) => body,
        Err(_) => return (StatusCode::NOT_FOUND, "not found").into_response(),
    };
    let mut resp = body.into_response();
    let headers = resp.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(content_type(&path)),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CSP),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    // The page is a handful of small files; serving the newest one every time is
    // cheaper than explaining a stale script after an upgrade.
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    resp
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("json") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("webp") => "image/webp",
        Some("png") => "image/png",
        Some("woff2") => "font/woff2",
        Some("txt" | "md") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn console(case: &str) -> (PathBuf, ConsoleFiles) {
        let root =
            std::env::temp_dir().join(format!("freshet-console-{case}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("assets/js")).unwrap();
        std::fs::write(root.join("index.html"), "<!doctype html>").unwrap();
        std::fs::write(root.join("assets/js/app.js"), "export {};").unwrap();
        std::fs::write(root.join(".secret"), "x").unwrap();
        let files = ConsoleFiles::new(&root).unwrap();
        (root, files)
    }

    #[test]
    fn a_path_resolves_inside_the_directory_or_not_at_all() {
        let (root, files) = console("resolve");
        let root = std::fs::canonicalize(root).unwrap();
        assert_eq!(files.resolve(""), Some(root.join("index.html")));
        assert_eq!(files.resolve("/"), Some(root.join("index.html")));
        assert_eq!(
            files.resolve("assets/js/app.js"),
            Some(root.join("assets/js/app.js"))
        );
        for bad in [
            "../index.html",
            "assets/../../etc/passwd",
            ".secret",
            "assets//js/app.js",
            "assets\\js\\app.js",
            "c:/x",
            "assets/js",
            "missing.js",
            "overview",
        ] {
            assert_eq!(files.resolve(bad), None, "{bad}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_out_of_the_directory_is_not_followed() {
        let (root, files) = console("symlink");
        std::os::unix::fs::symlink("/etc/hostname", root.join("assets/out.txt")).unwrap();
        assert_eq!(files.resolve("assets/out.txt"), None);
    }

    #[test]
    fn a_directory_without_an_index_is_refused() {
        let root =
            std::env::temp_dir().join(format!("freshet-console-noindex-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let err = ConsoleFiles::new(&root).err().expect("refused").to_string();
        assert!(err.contains("has no index.html"), "{err}");
        assert!(ConsoleFiles::new("/definitely/not/here").is_err());
    }
}
