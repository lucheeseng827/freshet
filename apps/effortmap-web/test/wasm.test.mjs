/* The wasm binding, against the golden fixture.
 *
 * `freshet-embed`'s own docstring says every binding — C-ABI, WASM, Python, CLI —
 * marshals through one entry point so that "every surface answers identically (a
 * cross-binding golden test)". This is one half of that test. The other half is
 * `crates/freshet-wasm/tests/golden.rs`, which runs the SAME request through the
 * NATIVE build and asserts the SAME file. If either surface drifts, one of the two
 * fails, and the fixture is the thing they are both pinned to rather than to each
 * other.
 *
 * Regenerate the fixture only when the contract deliberately changes:
 *
 *   node --input-type=module -e "…initSync…; fs.writeFileSync('test/golden/response.json', …)"
 *
 * `node --test test/*.test.mjs` — requires `scripts/build.sh` to have run.
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { fileURLToPath } from "node:url";

import { initSync, plan } from "../assets/freshet_wasm.js";
import { summary, queueRows } from "../assets/effortmap.js";

const here = (p) => fileURLToPath(new URL(p, import.meta.url));

// `--target web` fetches the module by URL in a browser; in node we hand it the bytes
// directly with initSync, so one build serves both the page and this test.
initSync({ module: fs.readFileSync(here("../assets/freshet_wasm_bg.wasm")) });

const REQUEST = fs.readFileSync(here("golden/request.json"), "utf8");
const GOLDEN = fs.readFileSync(here("golden/response.json"), "utf8").trim();

test("the wasm binding reproduces the golden response byte for byte", () => {
  // Compared as TEXT, not as parsed JSON, and that is the whole point: fingerprints
  // are u64 and exceed 2^53, so `JSON.parse` rounds them. A parsed comparison rounds
  // both sides identically and passes while the state is being silently corrupted —
  // which is exactly what this test caught the first time it ran.
  assert.equal(plan(REQUEST).trim(), GOLDEN);
});

test("planning is deterministic across calls", () => {
  // Same request twice: the planner is pure, so a second call cannot differ. This
  // would catch state leaking into the module across calls.
  assert.equal(plan(REQUEST), plan(REQUEST));
});

test("the golden plan is the shape the page renders", () => {
  const p = JSON.parse(plan(REQUEST));
  const s = summary(p);
  assert.equal(s.models, 3);
  assert.equal(s.totalCu, 3);
  assert.equal(s.depth, 2, "stg_orders, then the two marts");
  assert.equal(s.width, 2);
  assert.deepEqual(queueRows(p).map((r) => r.target), ["stg_orders", "customer_ltv", "daily_revenue"]);
});

test("a restatement fans out to one item per partition", () => {
  const req = { ...JSON.parse(REQUEST), restate: { model: "stg_orders", partitions: ["d1", "d2", "d3"] } };
  const p = JSON.parse(plan(JSON.stringify(req)));
  const stg = queueRows(p).filter((r) => r.model === "stg_orders");
  assert.equal(stg.length, 3);
  assert.deepEqual(stg.map((r) => r.target), ["stg_orders[d1]", "stg_orders[d2]", "stg_orders[d3]"]);
  // The identity mapping carried those partitions into the incremental downstream,
  // but not into `customer_ltv`, which is a table and rebuilds whole.
  assert.equal(queueRows(p).filter((r) => r.model === "daily_revenue").length, 3);
  assert.equal(queueRows(p).filter((r) => r.model === "customer_ltv").length, 1);
});

test("a malformed request is an error document, not a throw", () => {
  // The funnel's contract: failure is data. A page that has to try/catch around the
  // planner is a page that will forget to somewhere.
  const out = JSON.parse(plan("{not json"));
  assert.equal(out.error.code, "bad_request");
  assert.deepEqual(out.models, []);
});

test("cost and queue stay absent unless asked for", () => {
  const req = JSON.parse(REQUEST);
  delete req.include_cost;
  delete req.include_queue;
  const raw = plan(JSON.stringify(req));
  assert.ok(!raw.includes("cost"), raw);
  assert.ok(!raw.includes("queue"), raw);
});

// ── the u64 fingerprint round-trip ────────────────────────────────────────────

test("a fingerprint survives a round-trip through the page exactly", async () => {
  const { plan: runPlan, rawField, withRawField } = await import("../assets/planner.js");
  const request = JSON.parse(REQUEST);

  const first = runPlan(request);
  assert.ok(first.nextStateRaw, "a code-change plan stages a snapshot");

  // The exact digits, straight off the binding — not via JSON.parse, which rounds
  // anything past 2^53 and would silently corrupt every fingerprint.
  const exact = first.responseText.match(/"stg_orders":(\d+)/)[1];
  assert.ok(BigInt(exact) > BigInt(Number.MAX_SAFE_INTEGER), "the fixture exercises the unsafe range");
  assert.ok(first.nextStateRaw.includes(exact), "the stored state keeps the exact digits");

  // Feed it back: nothing changed, so nothing should replan. This is the assertion
  // that fails loudly if state is ever routed through a JS number — a corrupted
  // fingerprint makes every model look edited.
  const second = runPlan(request, first.nextStateRaw);
  assert.deepEqual(second.plan.models, [], "an unchanged project replans to nothing");
});

test("editing one model after a commit replans only it and its dependents", async () => {
  const { plan: runPlan } = await import("../assets/planner.js");
  const request = JSON.parse(REQUEST);
  const committed = runPlan(request).nextStateRaw;

  const edited = structuredClone(request);
  edited.models[0].sql = "select order_id, customer_id, amount, order_day, discount from raw_orders";
  edited.sources.raw_orders.push("discount");

  const after = runPlan(edited, committed);
  const names = after.plan.models.map((m) => m.name);
  assert.deepEqual(names, ["stg_orders", "customer_ltv", "daily_revenue"]);
  assert.equal(after.plan.models[0].reason, "directly_changed");
  assert.equal(after.plan.models[1].reason.downstream.because_of, "stg_orders");
});

// ── raw JSON field surgery ────────────────────────────────────────────────────

test("rawField reads a top-level value verbatim", async () => {
  const { rawField } = await import("../assets/planner.js");
  assert.equal(rawField('{"a":1,"b":{"c":2}}', "b"), '{"c":2}');
  assert.equal(rawField('{"a": 18446744073709551615 }', "a"), "18446744073709551615");
  assert.equal(rawField('{"a":"x"}', "a"), '"x"');
  assert.equal(rawField('{"a":[1,2]}', "a"), "[1,2]");
  assert.equal(rawField('{"a":null}', "a"), "null");
  assert.equal(rawField("{}", "a"), null);
});

test("rawField ignores a same-named key that is nested or inside a string", async () => {
  const { rawField } = await import("../assets/planner.js");
  assert.equal(rawField('{"outer":{"k":"deep"},"k":"top"}', "k"), '"top"');
  assert.equal(rawField('{"note":"look at \\"k\\": here","k":1}', "k"), "1");
  assert.equal(rawField('{"a":{"k":9}}', "k"), null, "depth-2 only is not a match");
});

test("rawField survives braces and escapes inside string values", async () => {
  const { rawField } = await import("../assets/planner.js");
  assert.equal(rawField('{"sql":"select \\"}{\\" from t","k":7}', "k"), "7");
});

test("withRawField replaces an existing field and appends a missing one", async () => {
  const { withRawField, rawField } = await import("../assets/planner.js");
  const big = "16899773155021735464";
  assert.equal(rawField(withRawField('{"a":1}', "s", big), "s"), big);
  assert.equal(rawField(withRawField('{"s":2,"a":1}', "s", big), "s"), big);
  assert.equal(rawField(withRawField("{}", "s", big), "s"), big);
  // and the result is still valid JSON
  assert.equal(JSON.parse(withRawField('{"a":1}', "s", "2")).s, 2);
});
