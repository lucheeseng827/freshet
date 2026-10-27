//! freshet-ffi — the C ABI for the backfill planner.
//!
//! Four symbols, marshalling-only (zero planning logic — everything routes through
//! [`freshet_embed::plan_from_json`]). The header is `include/planner.h`, generated
//! by `cbindgen` from this file (CI fails if it drifts):
//!
//! ```c
//! uint32_t    planner_abi_version(void);
//! const char *planner_contract_version(void);          // static; never free it
//! char       *planner_plan(const char *request_json);   // caller must free with:
//! void        planner_string_free(char *ptr);
//! ```
//!
//! ## Memory & safety contract
//!
//! * **Rust allocates, Rust frees.** Every non-null string returned by
//!   [`planner_plan`] MUST be released with [`planner_string_free`] and nothing else
//!   (never the C runtime's `free`). Passing a foreign or already-freed pointer is
//!   undefined behaviour.
//! * **Panics never cross the boundary.** Every entry point is wrapped in
//!   [`std::panic::catch_unwind`]; a panic becomes a JSON `error` response, not an
//!   unwind into C.
//! * **Inputs are validated.** A null or non-UTF-8 `request_json` yields a JSON error
//!   string, not a crash.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;

/// [`freshet_embed::WIRE_CONTRACT_VERSION`], NUL-terminated for C.
const CONTRACT_VERSION: &CStr = match CStr::from_bytes_with_nul(
    concat!("planner-embed/5", "\0").as_bytes(),
) {
    Ok(v) => v,
    Err(_) => panic!("the contract version must be a C string"),
};

/// ABI version. Bump on any breaking change to a symbol's signature or the wire
/// contract's required fields.
#[no_mangle]
pub extern "C" fn planner_abi_version() -> u32 {
    1
}

/// The wire contract revision [`planner_plan`] speaks, e.g. `"planner-embed/5"`. A
/// static string: do **not** pass it to [`planner_string_free`]. Adding this symbol
/// did not change any existing one, so the ABI version stays 1.
#[no_mangle]
pub extern "C" fn planner_contract_version() -> *const c_char {
    CONTRACT_VERSION.as_ptr()
}

/// Plan a backfill. `request_json` is a NUL-terminated UTF-8 [`freshet_embed`]
/// request. Returns a newly-allocated NUL-terminated JSON response that the caller
/// MUST free with [`planner_string_free`]. Returns null only if the response could
/// not be allocated as a C string.
///
/// # Safety
/// `request_json` must be either null or a valid pointer to a NUL-terminated byte
/// string that stays alive for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn planner_plan(request_json: *const c_char) -> *mut c_char {
    let json = std::panic::catch_unwind(|| {
        if request_json.is_null() {
            return error_json("null request pointer");
        }
        // SAFETY: contract requires a valid NUL-terminated string when non-null.
        let raw = unsafe { CStr::from_ptr(request_json) };
        match raw.to_str() {
            Ok(s) => freshet_embed::plan_from_json(s),
            Err(_) => error_json("request was not valid UTF-8"),
        }
    })
    .unwrap_or_else(|_| error_json("internal panic"));

    match CString::new(json) {
        Ok(cs) => cs.into_raw(),
        // The JSON contained an interior NUL (shouldn't happen) — give up safely.
        Err(_) => std::ptr::null_mut(),
    }
}

/// Free a string returned by [`planner_plan`]. Null is a safe no-op (double-free
/// guard). Calling this on any other pointer is undefined behaviour.
///
/// # Safety
/// `ptr` must be either null or a pointer previously returned by [`planner_plan`] and
/// not yet freed.
#[no_mangle]
pub unsafe extern "C" fn planner_string_free(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: ptr came from CString::into_raw in planner_plan.
    unsafe {
        drop(CString::from_raw(ptr));
    }
}

fn error_json(message: &str) -> String {
    // Build via serde so the message is always correctly escaped.
    serde_json::json!({ "models": [], "error": { "code": "ffi_error", "message": message } }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // We can exercise the ABI from Rust by round-tripping through the raw pointers.
    #[test]
    fn plan_and_free_roundtrip() {
        let req = CString::new(r#"{"models":[{"name":"m","sql":"select 1 as x"}]}"#).unwrap();
        let out = unsafe { planner_plan(req.as_ptr()) };
        assert!(!out.is_null());
        let s = unsafe { CStr::from_ptr(out) }.to_str().unwrap().to_string();
        assert!(s.contains("\"models\""));
        unsafe { planner_string_free(out) }; // must not double-free / leak under ASAN
    }

    #[test]
    fn null_request_is_a_clean_error() {
        let out = unsafe { planner_plan(std::ptr::null()) };
        let s = unsafe { CStr::from_ptr(out) }.to_str().unwrap().to_string();
        assert!(s.contains("ffi_error"));
        unsafe { planner_string_free(out) };
    }

    #[test]
    fn free_null_is_a_noop() {
        unsafe { planner_string_free(std::ptr::null_mut()) };
    }

    #[test]
    fn abi_version_is_stable() {
        assert_eq!(planner_abi_version(), 1);
    }

    #[test]
    fn the_contract_version_is_the_funnel_s() {
        let v = unsafe { CStr::from_ptr(planner_contract_version()) };
        assert_eq!(v.to_str().unwrap(), freshet_embed::WIRE_CONTRACT_VERSION);
    }
}
