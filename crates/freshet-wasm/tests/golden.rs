//! The native half of the cross-binding golden test.
//!
//! `freshet-embed` promises that every binding — C-ABI, WASM, Python, CLI — marshals
//! through one entry point so they all answer identically. That promise had no test.
//! This is half of it: the same request fixture the browser binding runs, through the
//! NATIVE build, asserted against the same response fixture.
//!
//! The other half is `apps/effortmap-web/test/wasm.test.mjs`, which runs the compiled
//! `wasm32` artifact over the identical files. Both sides are pinned to the fixture
//! rather than to each other, so a drift in either surface fails on that surface and
//! names itself.
//!
//! The fixtures live with the web app because that is what regenerates them; the paths
//! below are the only coupling, and a moved fixture fails loudly at `include_str!`.

const REQUEST: &str = include_str!("../../../apps/effortmap-web/test/golden/request.json");
const RESPONSE: &str = include_str!("../../../apps/effortmap-web/test/golden/response.json");

/// Compare as parsed JSON, not as text: the fixture is pretty-printed for review and
/// the binding emits compact output, and neither of those is the contract. Key order
/// is not the contract either — `serde_json::Value` compares maps structurally.
fn same_json(a: &str, b: &str) -> bool {
    let (a, b) = (
        serde_json::from_str::<serde_json::Value>(a).expect("left is JSON"),
        serde_json::from_str::<serde_json::Value>(b).expect("right is JSON"),
    );
    a == b
}

#[test]
fn the_native_binding_reproduces_the_golden_response() {
    let got = freshet_wasm::plan(REQUEST);
    assert!(
        same_json(&got, RESPONSE),
        "the native binding drifted from the golden fixture.\n\
         If the contract changed on purpose, regenerate the fixture from the wasm \
         binding (see apps/effortmap-web/test/wasm.test.mjs) and check the browser \
         half in the same commit.\n\ngot: {got}"
    );
}

#[test]
fn planning_is_pure_across_calls() {
    assert_eq!(freshet_wasm::plan(REQUEST), freshet_wasm::plan(REQUEST));
}
