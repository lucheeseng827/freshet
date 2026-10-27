//! freshet-wasm — the browser/Node "what will this PR rebuild?" surface.
//!
//! Stateless marshalling over [`freshet_embed::plan_from_json`]: a CI bot or a
//! review UI hands in the changed project + the prior snapshot and gets back the
//! minimal, column-pruned backfill — no server, no Python, no dbt install.
//!
//! All wasm-bindgen glue is gated behind the off-by-default `wasm` feature, so a
//! bare `cargo build` never selects a `wasm32` target. With the feature off this is a
//! plain library exposing the same [`plan`] function (handy for native tests). The
//! real artifact is produced with `wasm-pack build --features wasm`.

/// The pure-Rust core. Identical behaviour in every build; the `wasm` glue just
/// re-exports it across the JS boundary.
pub fn plan(request_json: &str) -> String {
    freshet_embed::plan_from_json(request_json)
}

#[cfg(feature = "wasm")]
mod glue {
    use wasm_bindgen::prelude::*;

    /// `plan(requestJson: string): string` — see `freshet_embed` for the wire shape.
    #[wasm_bindgen]
    pub fn plan(request_json: &str) -> String {
        super::plan(request_json)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn core_plan_returns_json() {
        let out = super::plan(r#"{"models":[{"name":"m","sql":"select 1 as x"}]}"#);
        assert!(out.contains("\"models\""));
    }
}
