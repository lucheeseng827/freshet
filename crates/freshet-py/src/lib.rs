//! freshet-py — the pip-installable Python surface.
//!
//! The dbt/SQLMesh audience is Python; this is the "`pip install` a minimal,
//! column-pruned backfill instead of a `dbt run --full-refresh`" wheel. Marshalling
//! only — everything routes through [`freshet_embed::plan_from_json`].
//!
//! pyo3 is optional and behind the off-by-default `extension-module` feature, so a
//! bare `cargo build` never links libpython. The wheel is built with
//! `maturin build --features extension-module` and, thanks to `abi3-py38`, a single
//! wheel covers CPython 3.8+. With the feature off this is a plain library exposing
//! the same [`plan`] core (for native tests).
//!
//! ## The Python API
//!
//! ```python
//! import freshet_planner
//!
//! freshet_planner.plan(request_json: str) -> str      # the wire response, verbatim
//! freshet_planner.plan_dict(request: dict) -> dict    # parsed; raises PlannerError
//! freshet_planner.PlannerError                        # .code, .message, .response
//! freshet_planner.WIRE_CONTRACT_VERSION               # "planner-embed/5"
//! ```
//!
//! The two entry points differ on purpose. `plan` is the cross-binding surface: it
//! returns exactly what the C ABI, the WASM build and the CLI return for the same
//! request, errors included as the response's `error` field — which is what lets one
//! golden fixture pin all of them. `plan_dict` is the Pythonic one: a planner error
//! (a request it cannot parse, SQL it cannot analyze, a dialect it cannot render)
//! raises [`PlannerError`](glue) instead of handing back a dict whose `error` key a
//! caller has to remember to check.

/// The pure-Rust core. The pyo3 glue re-exports this across the Python boundary.
pub fn plan(request_json: &str) -> String {
    freshet_embed::plan_from_json(request_json)
}

/// The `error` of a wire response, as `(code, message)` — what `plan_dict` raises.
pub fn wire_error(response: &serde_json::Value) -> Option<(String, String)> {
    let error = response.get("error")?;
    let text = |key: &str| error.get(key).and_then(|v| v.as_str()).unwrap_or_default().to_string();
    Some((text("code"), text("message")))
}

#[cfg(feature = "extension-module")]
mod glue {
    use pyo3::create_exception;
    use pyo3::exceptions::PyException;
    use pyo3::prelude::*;
    use pyo3::types::PyDict;

    create_exception!(
        freshet_planner,
        PlannerError,
        PyException,
        "The planner could not answer the request. `code` is the wire error code \
         (`bad_request`, `plan_error`, `render_error`, …), `message` says why, and \
         `response` is the full response dict."
    );

    /// `freshet_planner.plan(request_json: str) -> str` — the wire response, verbatim.
    #[pyfunction]
    fn plan(request_json: &str) -> String {
        super::plan(request_json)
    }

    /// `freshet_planner.plan_dict(request: dict) -> dict` — convenience over the JSON core.
    /// Takes a `PyDict` so a non-dict argument is rejected at the boundary rather than
    /// failing later inside `json.dumps`; raises `PlannerError` on an error response.
    #[pyfunction]
    fn plan_dict(py: Python<'_>, request: &Bound<'_, PyDict>) -> PyResult<PyObject> {
        let json = py.import_bound("json")?;
        let req_json: String = json.call_method1("dumps", (request,))?.extract()?;
        let resp_json = super::plan(&req_json);
        let response = json.call_method1("loads", (resp_json.as_str(),))?;
        let parsed: serde_json::Value = serde_json::from_str(&resp_json)
            .map_err(|e| PlannerError::new_err(format!("unparseable planner response: {e}")))?;
        if let Some((code, message)) = super::wire_error(&parsed) {
            let err = PlannerError::new_err(format!("{code}: {message}"));
            let value = err.value_bound(py);
            value.setattr("code", code)?;
            value.setattr("message", message)?;
            value.setattr("response", response)?;
            return Err(err);
        }
        Ok(response.into())
    }

    #[pymodule]
    fn freshet_planner(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_function(wrap_pyfunction!(plan, m)?)?;
        m.add_function(wrap_pyfunction!(plan_dict, m)?)?;
        m.add("PlannerError", m.py().get_type_bound::<PlannerError>())?;
        m.add("WIRE_CONTRACT_VERSION", freshet_embed::WIRE_CONTRACT_VERSION)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn core_plan_returns_json() {
        let out = super::plan(r#"{"models":[{"name":"m","sql":"select 1 as x"}]}"#);
        assert!(out.contains("\"models\""));
    }

    #[test]
    fn an_error_response_names_its_code_and_message() {
        let out = super::plan("not json");
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let (code, message) = super::wire_error(&v).expect("a bad request is an error response");
        assert_eq!(code, "bad_request");
        assert!(!message.is_empty());

        let ok: serde_json::Value =
            serde_json::from_str(&super::plan(r#"{"models":[{"name":"m","sql":"select 1 as x"}]}"#)).unwrap();
        assert_eq!(super::wire_error(&ok), None);
    }
}
