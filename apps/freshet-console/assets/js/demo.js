// The demo: a freshet-state-server held in memory, behind a `fetch`. It answers the
// same routes with the same bodies, u64 fingerprints written as the digits they are,
// so `?demo` runs through exactly the parsing a real server's answers do (api.js).
//
// Three states, the same on every load: a busy one, a small one, and one that has
// only ever had its first commit.

import { stringifyExact } from "./core/json.js";

const DAY = 86400000;

// A small deterministic PRNG, so the demo looks the same on every load and in every
// screenshot, while still reading as real data.
function rng(seed) {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/// A full-range u64: almost every one is past 2^53, which is the point.
const u64 = (r) => (BigInt(Math.floor(r() * 2 ** 32)) << 32n) | BigInt(Math.floor(r() * 2 ** 32));
const pick = (r, xs) => xs[Math.floor(r() * xs.length)];
const stamp = (ms) => new Date(ms).toISOString().replace(/\.\d+Z$/, "Z");

const MODELS = [
  "stg_orders", "stg_customers", "stg_payments", "stg_events", "stg_sessions", "stg_products",
  "stg_refunds", "stg_invoices", "stg_web_visits", "int_orders_enriched", "int_sessions_stitched",
  "int_payments_matched", "int_customer_identity", "fct_orders", "fct_revenue_daily", "fct_sessions",
  "fct_events", "fct_refunds", "dim_customers", "dim_products", "dim_dates", "dim_channels",
  "agg_cohort_retention", "agg_funnel_weekly", "agg_revenue_by_region", "rpt_finance_monthly",
  "rpt_ops_weekly", "rpt_exec_dashboard",
];
const INCREMENTAL = new Set(["stg_events", "stg_web_visits", "fct_events", "fct_sessions", "int_sessions_stitched", "fct_revenue_daily"]);

function ledger(r, now) {
  const end = new Date(now - Math.floor(r() * 2) * DAY);
  const start = new Date(end.getTime() - 120 * DAY);
  const d = (x) => x.toISOString().slice(0, 10);
  return JSON.stringify({ v: 2, grain: "Day", built: [[d(start), d(end)]], pending: [], pending_full: false });
}

/// `count` commits over `spanDays`, each with the summary the server itself writes:
/// what changed, what left, which ledgers moved, and the committer's words.
function history(r, models, count, spanDays, now) {
  const snap = { fingerprints: {}, watermarks: {} };
  const commits = [];
  const start = now - spanDays * DAY;
  const step = (spanDays * DAY - 40 * 60000) / count;
  for (let v = 1; v <= count; v++) {
    const at = start + v * step - r() * step * 0.4;
    const before = { fingerprints: { ...snap.fingerprints }, watermarks: { ...snap.watermarks } };
    let context;
    if (v === 1) {
      for (const m of models) snap.fingerprints[m] = u64(r);
      for (const m of models) if (INCREMENTAL.has(m)) snap.watermarks[m] = ledger(r, now);
      context = { command: "import" };
    } else if (r() < 0.3) {
      const m = pick(r, models.filter((x) => INCREMENTAL.has(x)));
      if (m) snap.watermarks[m] = ledger(r, now);
      context = { client: "freshet/0.1.0", command: "restate --commit", model: m, partitions: 1 + Math.floor(r() * 4) };
    } else {
      const n = 1 + Math.floor(r() * 5);
      for (let i = 0; i < n; i++) snap.fingerprints[pick(r, models)] = u64(r);
      context = { client: "freshet/0.1.0", command: "plan --commit", models_rebuilt: n + Math.floor(r() * 3) };
    }
    if (v === Math.floor(count * 0.6) && snap.fingerprints.rpt_ops_weekly != null) {
      delete snap.fingerprints.rpt_ops_weekly;
      delete snap.watermarks.rpt_ops_weekly;
    }
    const changed = Object.keys(snap.fingerprints).filter((m) => before.fingerprints[m] !== snap.fingerprints[m]).sort();
    const removed = Object.keys(before.fingerprints).filter((m) => !(m in snap.fingerprints)).sort();
    const moved = new Set([
      ...Object.keys(snap.watermarks).filter((m) => before.watermarks[m] !== snap.watermarks[m]),
      ...Object.keys(before.watermarks).filter((m) => !(m in snap.watermarks)),
    ]);
    commits.push({
      version: v,
      committed_at: stamp(at),
      summary: { changed, removed, watermarks_changed: [...moved].sort(), context },
      snapshot: { fingerprints: { ...snap.fingerprints }, watermarks: { ...snap.watermarks } },
    });
  }
  return commits;
}

function seed(now = Date.now()) {
  const r = rng(20261028);
  return {
    analytics: history(r, MODELS, 34, 21, now),
    marketing: history(r, MODELS.filter((m) => /orders|sessions|web_visits|channels|funnel|cohort/.test(m)), 4, 9, now),
    scratch: history(r, ["stg_events", "fct_events"], 1, 1, now),
  };
}

const json = (status, body) =>
  new Response(stringifyExact(body), { status, headers: { "content-type": "application/json" } });

/// A `fetch` that answers freshet-state-server's read routes from memory. It needs no
/// token, so the console sees `access: "open"`.
export function createDemoFetch({ now } = {}) {
  const states = seed(now);
  return async (url, init = {}) => {
    if ((init.method || "GET") !== "GET") return json(405, { error: "the demo is read-only, as the console is" });
    const u = new URL(url, "http://demo.invalid");
    const path = u.pathname.replace(/\/+$/, "") || "/";
    if (path === "/v1") {
      const listed = Object.entries(states).map(([name, commits]) => {
        const head = commits[commits.length - 1];
        return { name, version: head.version, committed_at: head.committed_at, models: Object.keys(head.snapshot.fingerprints).length };
      });
      return json(200, { states: listed, access: "open" });
    }
    const m = path.match(/^\/v1\/([^/]+)\/(current|history|versions\/([^/]+))$/);
    if (!m) return json(404, { error: "not found" });
    const name = decodeURIComponent(m[1]);
    // The server's own rule for a state's name: one safe path component.
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(name)) {
      return json(404, { error: `\`${name}\` is not a state name: letters, digits, \`.\`, \`_\` and \`-\`, starting with a letter or digit` });
    }
    const commits = states[name] || [];
    if (m[2] === "current") {
      const head = commits[commits.length - 1];
      return json(200, head ? { version: head.version, snapshot: head.snapshot } : { version: null, snapshot: { fingerprints: {}, watermarks: {} } });
    }
    if (m[2] === "history") {
      const limit = Math.min(Number(u.searchParams.get("limit") || 20), 100);
      const before = Number(u.searchParams.get("before") || Infinity);
      const page = commits.filter((c) => c.version < before).reverse().slice(0, limit);
      const last = page[page.length - 1];
      const older = last && commits.some((c) => c.version < last.version);
      return json(200, {
        commits: page.map(({ version, committed_at, summary }) => ({ version, committed_at, summary })),
        limit,
        next_before: older ? last.version : null,
      });
    }
    const v = Number(m[3]);
    const c = commits.find((x) => x.version === v);
    if (!c) return json(404, { error: `state \`${name}\` keeps no version ${m[3]}: it never landed, or retention (\`--keep\`) has dropped it` });
    return json(200, c);
  };
}
