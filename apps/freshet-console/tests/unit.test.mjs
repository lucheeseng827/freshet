// The pure modules, with no browser: the shared core (escaping, exact JSON, the
// state file, a commit's words) and the API client over a fake transport and over
// the demo. `node --test tests/unit.test.mjs`, from apps/freshet-console.

import { test } from "node:test";
import assert from "node:assert/strict";
import { html, esc, raw, Safe } from "../assets/js/core/ui.js";
import { parseExact, stringifyExact } from "../assets/js/core/json.js";
import { stateFileText, commitMoves, contextLine, moveSentence, snapshotRows } from "../assets/js/core/state.js";
import { createApi, ApiError, hex, hexSnapshot } from "../assets/js/api.js";
import { createDemoFetch } from "../assets/js/demo.js";
import { snippet } from "../assets/js/views/overview.js";

const BIG = 16899773155021735464n;

test("every interpolation is escaped unless it is already Safe", () => {
  const name = `<img src=x onerror="alert(1)">'&`;
  assert.equal(esc(name), "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;&#39;&amp;");
  const out = html`<p title="${name}">${name}${raw("<b>ok</b>")}</p>`;
  assert.ok(out instanceof Safe);
  assert.ok(!String(out).includes("<img"), String(out));
  assert.ok(String(out).includes("<b>ok</b>"));
  assert.equal(String(html`${[1, null, false, "<"]}`), "1&lt;");
});

test("a fingerprint past 2^53 is hex, exactly, from the server's digits", () => {
  const parsed = parseExact(`{"m":${BIG},"n":42}`);
  assert.equal(typeof parsed.m, "bigint");
  assert.equal(hex(parsed.m), BIG.toString(16));
  assert.equal(hex(parsed.n), "000000000000002a", "a small one is padded to 16 digits");
  assert.equal(hex(2n ** 64n - 1n), "ffffffffffffffff");
  assert.deepEqual(hexSnapshot({ fingerprints: { m: parsed.m }, watermarks: { m: "x" } }), {
    fingerprints: { m: BIG.toString(16) },
    watermarks: { m: "x" },
  });
  assert.deepEqual(hexSnapshot(undefined), { fingerprints: {}, watermarks: {} });
});

test("the downloaded state.json is the snapshot, digit for digit", () => {
  const ledger = JSON.stringify({ v: 2, grain: "Day", built: [["2026-07-01", "2026-10-27"]] });
  const text = stateFileText({ fingerprints: { stg_orders: BIG.toString(16), a: "0000000000000001" }, watermarks: { stg_orders: ledger } });
  assert.ok(text.includes(`"stg_orders": ${BIG}`), text);
  const back = parseExact(text);
  assert.equal(back.fingerprints.stg_orders, BIG);
  assert.equal(back.fingerprints.a, 1);
  assert.equal(back.watermarks.stg_orders, ledger, "the ledger stays the string it was");
});

test("a commit's words come from the server's own summary", () => {
  const m = commitMoves({ changed: ["a", "b"], removed: ["c"], watermarks_changed: ["a"], context: { command: "plan --commit", client: "freshet/0.1.0", models_rebuilt: 3 } });
  assert.equal(moveSentence(m), "2 models rebuilt, 1 partition ledger moved, 1 model removed");
  assert.equal(contextLine(m.context), "plan --commit · freshet/0.1.0 · 3 rebuilt");
  assert.equal(moveSentence(commitMoves(null)), "Nothing moved");
  assert.equal(contextLine("not an object"), "");
  const rows = snapshotRows({ fingerprints: { b: "2", a: "1" }, watermarks: { a: "w" } }, new Set(["b"]));
  assert.deepEqual(rows.map((r) => [r.name, r.hit, r.wm ?? null]), [["a", false, "w"], ["b", true, null]]);
});

test("the client sends the token, reads the server's words, and says when it can't reach it", async () => {
  const seen = [];
  const answer = (status, body) => async (url, init) => {
    seen.push({ url, init });
    return new Response(body, { status, headers: { "content-type": "application/json" } });
  };
  let api = createApi({ base: "http://s", token: () => "t0k", fetch: answer(200, `{"states":[],"access":"read"}`) });
  assert.deepEqual(await api.states(), { states: [], access: "read" });
  assert.equal(seen[0].url, "http://s/v1/");
  assert.equal(seen[0].init.headers.authorization, "Bearer t0k");
  assert.equal(seen[0].init.credentials, "omit", "never cookies");

  api = createApi({ fetch: answer(200, `{"commits":[],"limit":5,"next_before":null}`) });
  await api.history("a b", { limit: 5, before: 9 });
  assert.equal(seen[1].url, "/v1/a%20b/history?limit=5&before=9", "names are encoded, empty params dropped");
  assert.equal(seen[1].init.headers.authorization, undefined, "no token, no header");

  api = createApi({ fetch: answer(401, `{"error":"unauthorized: send Authorization: Bearer …"}`) });
  await assert.rejects(api.states(), (e) => e instanceof ApiError && e.status === 401 && e.message.startsWith("unauthorized"));
  api = createApi({ fetch: answer(502, "bad gateway") });
  await assert.rejects(api.states(), (e) => e.status === 502 && e.message === "bad gateway");
  api = createApi({ fetch: async () => { throw new TypeError("network"); } });
  await assert.rejects(api.states(), (e) => e.status === 0 && /Can't reach the state server/.test(e.message));

  const tried = createApi({ token: () => null, fetch: answer(200, "{}") }).withToken("other");
  await tried.states();
  assert.equal(seen.at(-1).init.headers.authorization, "Bearer other", "a token is tried before it is kept");
});

test("current and a version come back with hex fingerprints, exactly", async () => {
  const body = `{"version":3,"committed_at":"2026-10-28T00:00:00Z","summary":{"changed":["m"]},"snapshot":{"fingerprints":{"m":${BIG}},"watermarks":{}}}`;
  const api = createApi({ fetch: async () => new Response(body, { status: 200 }) });
  const head = await api.current("p");
  assert.deepEqual(head, { version: 3, snapshot: { fingerprints: { m: BIG.toString(16) }, watermarks: {} } });
  const v = await api.version("p", 3);
  assert.equal(v.snapshot.fingerprints.m, BIG.toString(16));
  assert.deepEqual(v.summary, { changed: ["m"] });
  const never = createApi({ fetch: async () => new Response(`{"version":null,"snapshot":{"fingerprints":{},"watermarks":{}}}`) });
  assert.deepEqual(await never.current("new"), { version: null, snapshot: { fingerprints: {}, watermarks: {} } });
});

test("the demo answers as the server does, through the same client", async () => {
  const api = createApi({ fetch: createDemoFetch({ now: Date.parse("2026-10-28T12:00:00Z") }) });
  const { states, access } = await api.states();
  assert.equal(access, "open");
  assert.deepEqual(states.map((s) => s.name), ["analytics", "marketing", "scratch"]);
  const top = states[0];
  const head = await api.current("analytics");
  assert.equal(head.version, top.version);
  assert.equal(Object.keys(head.snapshot.fingerprints).length, top.models);
  assert.ok(Object.values(head.snapshot.fingerprints).every((f) => /^[0-9a-f]{16}$/.test(f)));

  // Paging walks back to version 1 with no gaps and no repeats.
  const seen = [];
  let before;
  for (;;) {
    const page = await api.history("analytics", { limit: 10, before });
    seen.push(...page.commits.map((c) => c.version));
    if (page.next_before == null) break;
    before = page.next_before;
  }
  assert.deepEqual(seen, Array.from({ length: top.version }, (_, i) => top.version - i));

  // A version's snapshot agrees with what its commit says it moved.
  const v2 = await api.version("analytics", 2);
  const v1 = await api.version("analytics", 1);
  for (const m of v2.summary.changed) assert.notEqual(v2.snapshot.fingerprints[m], v1.snapshot.fingerprints[m], m);
  await assert.rejects(api.version("analytics", 999), (e) => e.status === 404 && /keeps no version/.test(e.message));
  await assert.rejects(api.current("x/y"), (e) => e.status === 404);
});

test("the demo writes its u64s as digits, and stringifyExact reads them back", async () => {
  const fetchDemo = createDemoFetch({ now: 0 });
  const text = await (await fetchDemo("/v1/scratch/current")).text();
  const parsed = parseExact(text);
  const fp = Object.values(parsed.snapshot.fingerprints)[0];
  assert.ok(typeof fp === "bigint" || Number.isSafeInteger(fp));
  assert.equal(stringifyExact(parsed), text, "what the demo sends is what a parse-and-write gives back");
  assert.equal((await fetchDemo("/v1/scratch/commit", { method: "POST" })).status, 405, "the demo, like the page, never commits");
});

test("the connect commands carry the state's address and version", () => {
  const ci = snippet("engineer", { url: "http://s/v1/p", version: 7 });
  const typed = ci.lines.filter((l) => l.t).map((l) => l.t).join("\n");
  assert.ok(typed.includes("--state http://s/v1/p --commit --expect-version 7"), typed);
  assert.ok(snippet("engineer", { url: "u", version: null }).lines.some((l) => l.t?.includes("--expect-version none")));
  assert.ok(snippet("scientist", { url: "http://s/v1/p" }).lines.some((l) => l.k?.includes('"http://s/v1/p/current"')));
});
