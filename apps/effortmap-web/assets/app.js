/* Wiring: files in, plan out, rows on the page.
 *
 * Everything with arithmetic in it lives in `effortmap.js` (pure, tested under node)
 * and everything touching the planner lives in `planner.js` (which owns the reason
 * state is handled as text). This file is the DOM.
 */

import * as em from "./effortmap.js";
import { plan as runPlan, ready } from "./planner.js";
import { barChart } from "./charts.js";

const $ = (id) => document.getElementById(id);
const fmtCu = (n) => (n == null ? "—" : `${n.toFixed(n >= 10 ? 1 : 2)}`);

const state = {
  models: {},          // name -> sql
  sources: {},
  materializations: {},
  plan: null,          // the parsed PlanResponse
  priorState: null,    // RAW TEXT — never parsed; see planner.js
  mode: "code",
};

// ── persistence ───────────────────────────────────────────────────────────────
// localStorage, not IndexedDB: the payload is one string of a few tens of KB (a
// fingerprint is ~40 bytes and a big project has a few thousand models), and a
// synchronous get on load keeps the first render single-pass. It is stored as TEXT for
// the reason planner.js documents — parsing it would round every u64 fingerprint.

const STATE_KEY = "effortmap.priorState";

function loadStoredState() {
  try {
    return localStorage.getItem(STATE_KEY);
  } catch {
    return null; // private mode, or site data blocked: plan cold rather than fail
  }
}

function storeState(raw) {
  state.priorState = raw;
  try {
    if (raw) localStorage.setItem(STATE_KEY, raw);
    else localStorage.removeItem(STATE_KEY);
  } catch {
    /* the plan still works; only the memory of it is lost */
  }
  renderStateStatus();
}

function renderStateStatus() {
  const raw = state.priorState;
  if (!raw) {
    $("state-status").textContent =
      "No state yet — the first plan will be a cold one that rebuilds everything.";
  } else {
    const n = countFingerprints(raw);
    $("state-status").textContent = n == null
      ? "State remembered in this browser — the fingerprints could not be counted, but they are stored verbatim and plan normally."
      : `${n} model fingerprint(s) remembered in this browser.`;
  }
  $("export-state").disabled = !raw;
}

/** How many fingerprints the stored state holds, or null when that cannot be told.
 *
 *  [`rawFingerprintNames`] cuts the map out of the text with a regex rather than
 *  parsing it, so a model name containing a brace hands back a truncated fragment and
 *  the parse throws. That is a cosmetic count of an otherwise perfectly usable state,
 *  and this runs at module evaluation — an escaping throw here would abort the rest of
 *  the boot and leave every button on the page unwired over a number in a caption. */
function countFingerprints(raw) {
  try {
    return Object.keys(JSON.parse(rawFingerprintNames(raw))).length;
  } catch {
    return null;
  }
}

/** The fingerprint map with every value replaced by 0, purely to count the keys.
 *  Counting must not parse the real numbers. */
function rawFingerprintNames(raw) {
  return raw.replace(/:\s*\d+/g, ": 0").match(/"fingerprints"\s*:\s*(\{.*?\})/s)?.[1] ?? "{}";
}

// ── loading a project ─────────────────────────────────────────────────────────

const SQL = /\.sql$/i;

async function loadFromFileList(fileList) {
  const files = [...fileList];
  const models = {};
  let sources = null;
  let materializations = null;

  for (const file of files) {
    // `webkitRelativePath` is "folder/sub/file.sql". Only the top level is taken, to
    // match `freshet-serve --project`: flattening nested dbt-shaped folders would
    // silently collide two models both called orders.sql.
    const rel = file.webkitRelativePath || file.name;
    const parts = rel.split("/");
    const depth = parts.length - (file.webkitRelativePath ? 1 : 0);
    const name = parts[parts.length - 1];

    if (SQL.test(name)) {
      if (depth > 1) continue;
      models[name.replace(SQL, "")] = await file.text();
    } else if (name === "sources.json") {
      sources = await file.text();
    } else if (name === "materializations.json" || name === "mat.json") {
      materializations = await file.text();
    }
  }
  applyProject(models, sources, materializations, files.length);
}

async function loadFromDirectoryHandle(handle) {
  const models = {};
  let sources = null;
  let materializations = null;
  let seen = 0;
  for await (const [name, entry] of handle.entries()) {
    if (entry.kind !== "file") continue;
    seen++;
    if (SQL.test(name)) models[name.replace(SQL, "")] = await (await entry.getFile()).text();
    else if (name === "sources.json") sources = await (await entry.getFile()).text();
    else if (name === "materializations.json" || name === "mat.json")
      materializations = await (await entry.getFile()).text();
  }
  applyProject(models, sources, materializations, seen);
}

function applyProject(models, sourcesText, materializationsText, seen) {
  const names = Object.keys(models);
  if (!names.length) {
    note("project-status", `no .sql files among ${seen} file(s)`, "bad");
    return;
  }
  state.models = models;
  if (sourcesText) $("sources").value = sourcesText.trim();
  if (materializationsText) $("materializations").value = materializationsText.trim();

  $("project-status").textContent =
    `${names.length} model(s): ${names.slice(0, 4).join(", ")}${names.length > 4 ? "…" : ""}`;
  $("run").disabled = false;
  refreshRestateModels();
}

function refreshRestateModels() {
  const select = $("restate-model");
  const previous = select.value;
  select.textContent = "";
  for (const name of Object.keys(state.models).sort()) {
    const option = document.createElement("option");
    option.value = name;
    option.textContent = name;
    select.appendChild(option);
  }
  if (previous) select.value = previous;
}

// ── planning ──────────────────────────────────────────────────────────────────

function buildRequest() {
  const request = {
    include_cost: true,
    include_queue: true,
    models: Object.keys(state.models)
      .sort() // deterministic: the project's model order reaches the plan's output
      .map((name) => {
        const model = { name, sql: state.models[name] };
        const hint = state.materializations[name];
        if (hint) model.materialization = hint;
        return model;
      }),
    sources: state.sources,
  };
  if (state.mode === "restate") {
    const partitions = $("restate-partitions").value.split("\n").map((s) => s.trim()).filter(Boolean);
    if (!partitions.length) throw new Error("a restatement needs at least one partition");
    request.restate = { model: $("restate-model").value, partitions };
  }
  return request;
}

function doPlan() {
  try {
    state.sources = em.sourcesFromJson($("sources").value);
    state.materializations = em.sourcesFromJson($("materializations").value);
  } catch (e) {
    note("plan-status", `sources/materializations: ${e.message}`, "bad");
    return;
  }

  let request;
  try {
    request = buildRequest();
  } catch (e) {
    note("plan-status", e.message, "bad");
    return;
  }

  const started = performance.now();
  // A restatement must not read committed fingerprints: it rebuilds data, and the
  // planner's own contract is that it neither reads nor advances code state.
  const prior = state.mode === "restate" ? null : state.priorState;

  let result;
  try {
    result = runPlan(request, prior);
  } catch (e) {
    // The planner reports refusals in band, as `plan.error` below; this is the other
    // kind — a wasm trap, or output that is not JSON, which `plan()` throws out of.
    // SHORTFALLS.md §4 is the reason it has to be caught here: a panic across the wasm
    // boundary leaves nothing useful in the console, so an uncaught throw would leave
    // the previous plan on screen looking like the answer to this request.
    note("plan-status", `the planner failed: ${e.message}`, "bad");
    $("result").hidden = true;
    return;
  }
  const elapsed = performance.now() - started;

  if (result.plan.error) {
    note("plan-status", `${result.plan.error.code}: ${result.plan.error.message}`, "bad");
    $("result").hidden = true;
    return;
  }

  state.plan = result.plan;
  state.nextStateRaw = result.nextStateRaw;
  $("plan-status").textContent = `planned in ${elapsed.toFixed(0)} ms`;
  render();
}

// ── rendering ─────────────────────────────────────────────────────────────────

const colors = () => {
  const css = getComputedStyle(document.documentElement);
  return {
    changed: { color: css.getPropertyValue("--series-direct").trim(), label: "changed" },
    downstream: { color: css.getPropertyValue("--series-downstream").trim(), label: "downstream" },
  };
};

function render() {
  const plan = state.plan;
  $("result").hidden = false;

  if (!plan.models.length) {
    $("stats").innerHTML = "";
    $("origin-line").innerHTML =
      `<span class="note good" style="display:block">Nothing to rebuild. Every model's fingerprint
       matches the last commit, so no code has changed since then.</span>`;
    for (const id of ["panel-effort", "panel-breakdown", "panel-queue"]) $(id).hidden = true;
    return;
  }

  const s = em.summary(plan);
  $("stats").innerHTML = [
    ["Models rebuilding", s.models],
    ["Predicted cost", `${fmtCu(s.totalCu)} <small>CU</small>`],
    ["Avoided by pruning", s.avoidedCu === null ? "—" : `${fmtCu(s.avoidedCu)} <small>CU</small>`],
    ["Work items", s.workItems ?? "—"],
    ["Critical path", s.depth ?? "—"],
    ["Widest wave", s.width ?? "—"],
  ]
    .map(([k, v]) => `<div class="stat"><div class="k">${k}</div><div class="v">${v}</div></div>`)
    .join("");

  const verb = state.mode === "restate" ? "restated" : "changed directly";
  $("origin-line").innerHTML = s.downstream
    ? `<strong>${s.directlyChanged}</strong> ${verb} · <strong>${s.downstream}</strong> pulled in downstream.`
    : "";

  renderEffort(plan);
  renderBreakdown(plan);
  renderQueue(plan);
  selectTab("effort");
}

function renderEffort(plan) {
  const rows = em.effortRows(plan);
  const series = colors();

  barChart($("effort-chart"), {
    axisLabel: "compute units (CU)",
    series,
    rows: rows.map((r) => ({
      label: r.model,
      value: r.cu ?? 0,
      display: r.cu == null ? "" : fmtCu(r.cu),
      series: r.origin,
      title: `${r.model} — ${fmtCu(r.cu)} CU (${r.millicu} mCU), ${r.basisLabel}` +
        (r.becauseOf ? `, pulled in by ${r.becauseOf} via ${r.viaColumns}` : ", changed itself"),
    })),
  });

  table($("effort-table"),
    ["Model", "Why", "CU", "mCU (exact)", "Charged as", "Rebuilds", "Because of", "Via columns"],
    rows.map((r) => [
      r.model,
      // The one cell on the page that is really markup: origin is a closed set
      // (`changed` / `downstream`) that doubles as the pill's colour class.
      { html: `<span class="pill ${escapeHtml(r.origin)}">${escapeHtml(r.origin)}</span>` },
      { num: fmtCu(r.cu) },
      { num: r.millicu ?? "—" },
      r.basisLabel,
      r.unit,
      r.becauseOf,
      r.viaColumns,
    ]));
}

function renderBreakdown(plan) {
  const breakdown = em.breakdownRows(plan);
  const host = $("breakdown");
  host.textContent = "";

  if (!breakdown.length) {
    host.innerHTML =
      `<div class="note info">Nothing in this plan is partition-scoped: every selected
       model rebuilds whole. Only <code>incremental</code> models can be scoped — mark
       them in the materializations box, then restate specific partitions.</div>`;
  } else {
    const t = document.createElement("table");
    table(t, ["Model", "Partitions", "First", "Last"],
      breakdown.map((b) => [b.model, { num: b.partitions }, b.first, b.last]));
    const scroll = document.createElement("div");
    scroll.className = "scroll";
    scroll.appendChild(t);
    host.appendChild(scroll);

    for (const b of breakdown) {
      const d = document.createElement("details");
      d.innerHTML =
        `<summary>${escapeHtml(b.model)} — ${b.partitions} partitions</summary><pre></pre>`;
      d.querySelector("pre").textContent = b.keys.join("\n");
      host.appendChild(d);
    }
  }

  const waves = em.waveRows(plan);
  barChart($("wave-chart"), {
    axisLabel: "compute units (CU)",
    series: { wave: { color: colors().changed.color, label: "wave" } },
    rows: waves.map((w) => ({
      label: `wave ${w.wave}`,
      value: w.cu,
      display: fmtCu(w.cu),
      series: "wave",
      title: `wave ${w.wave} — ${w.items} item(s), ${fmtCu(w.cu)} CU: ${w.models.join(", ")}`,
    })),
  });
  table($("wave-table"), ["Wave", "Items", "CU", "Models"],
    waves.map((w) => [{ num: w.wave }, { num: w.items }, { num: fmtCu(w.cu) }, w.models.join(", ")]));
}

function renderQueue(plan) {
  const rows = em.queueRows(plan);
  table($("queue-table"), ["#", "Work item", "Wave", "Waits on", "CU", "mCU (exact)"],
    rows.map((r) => [
      { num: r.seq }, r.target, { num: r.wave }, r.dependsOn, { num: fmtCu(r.cu) }, { num: r.millicu },
    ]));

  const area = $("commit-area");
  area.textContent = "";
  if (state.mode === "restate" || !state.nextStateRaw) {
    area.innerHTML =
      `<div class="note info">A restatement produces no commit token: it rebuilds data,
       and advancing code fingerprints from it would claim work that never happened.</div>`;
    return;
  }
  area.innerHTML =
    `<div class="note warn">
       <strong>After the run.</strong> Marking this built advances the fingerprints that
       record which code has been built. Do it <em>only once this plan has actually
       run</em> — mark it without running and the next plan sees nothing to do, so the
       rebuild is skipped silently.
     </div>
     <button id="commit">Mark this plan as built</button>`;
  $("commit").addEventListener("click", () => {
    storeState(state.nextStateRaw);
    note("plan-status", "state advanced — re-plan to confirm there is nothing left", "good");
    doPlan();
  });
}

// ── small DOM helpers ─────────────────────────────────────────────────────────

const ENTITIES = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };

/** Escape a value for interpolation into markup. Nullish becomes the empty string.
 *
 *  Every string this page renders is attacker-shaped: a model name is a `.sql`
 *  filename off someone's disk, a partition key is typed into a textarea, a source and
 *  its columns come out of a pasted JSON box. Interpolated raw, a model called
 *  `<img src=x onerror=…>` runs script in this origin — and this origin is where the
 *  remembered fingerprints live, so a folder picked out of curiosity could read the
 *  state or rewrite it into one that says everything is already built. So escaping is
 *  what a cell gets by default here, and markup is the opt-in: `{ html }` below. */
export function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ENTITIES[c]);
}

const isNum = (cell) => cell != null && typeof cell === "object" && "num" in cell;

/** Render rows into `node`. A cell is a plain value (escaped), `{ num }` for a
 *  right-aligned figure (escaped), or `{ html }` for markup the caller vouches for.
 *
 *  Exported for the escaping test only — nothing imports this file but the page. */
export function table(node, headers, rows) {
  const head = `<thead><tr>${headers
    .map((h, i) => `<th class="${isNum(rows[0]?.[i]) ? "num" : ""}">${escapeHtml(h)}</th>`)
    .join("")}</tr></thead>`;
  const body = rows
    .map((cells) =>
      `<tr>${cells
        .map((c) => {
          if (isNum(c)) return `<td class="num">${escapeHtml(c.num)}</td>`;
          if (c != null && typeof c === "object" && "html" in c) return `<td>${c.html}</td>`;
          return `<td>${escapeHtml(c)}</td>`;
        })
        .join("")}</tr>`)
    .join("");
  node.innerHTML = head + `<tbody>${body}</tbody>`;
}

function note(id, message, kind) {
  const node = $(id);
  node.textContent = message;
  node.style.color = kind === "bad" ? "var(--bad-ink)" : kind === "good" ? "var(--good-ink)" : "var(--ink-muted)";
}

function download(name, text, mime) {
  const url = URL.createObjectURL(new Blob([text], { type: mime }));
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

function selectTab(which) {
  for (const key of ["effort", "breakdown", "queue"]) {
    $(`tab-${key}`).setAttribute("aria-selected", String(key === which));
    $(`panel-${key}`).hidden = key !== which;
  }
}

// ── the example project ───────────────────────────────────────────────────────
// Inlined rather than fetched: the page must work opened from a static host with no
// extra round trips, and it makes the whole thing demonstrable in one click.

const EXAMPLE = {
  models: {
    stg_orders: "select order_id, customer_id, amount, order_day from raw_orders",
    customer_ltv: "select customer_id, amount from stg_orders",
    daily_revenue: "select order_day, amount from stg_orders",
  },
  sources: { raw_orders: ["order_id", "customer_id", "amount", "order_day"] },
  materializations: { stg_orders: "incremental", daily_revenue: "incremental" },
};

// ── boot ──────────────────────────────────────────────────────────────────────

$("load-example").addEventListener("click", () => {
  $("sources").value = JSON.stringify(EXAMPLE.sources, null, 2);
  $("materializations").value = JSON.stringify(EXAMPLE.materializations, null, 2);
  applyProject({ ...EXAMPLE.models }, null, null, 3);
  $("advanced").open = true;
});

$("pick-files").addEventListener("change", (e) => loadFromFileList(e.target.files));

// The File System Access API keeps a handle, so a folder can be re-read without
// re-picking. Directory handles are Chromium-only today; everywhere else the
// `webkitdirectory` input above is the path, which works but re-picks each time.
if ("showDirectoryPicker" in window) {
  $("pick-files-label").hidden = true;
  const button = $("pick-dir");
  button.hidden = false;
  button.addEventListener("click", async () => {
    try {
      await loadFromDirectoryHandle(await window.showDirectoryPicker({ mode: "read" }));
    } catch (e) {
      if (e.name !== "AbortError") note("project-status", e.message, "bad");
    }
  });
}

for (const radio of document.querySelectorAll('input[name="mode"]')) {
  radio.addEventListener("change", (e) => {
    state.mode = e.target.value;
    $("restate-controls").hidden = state.mode !== "restate";
  });
}

$("run").addEventListener("click", doPlan);
$("download-queue").addEventListener("click", () =>
  download("queue.ndjson", em.queueNdjson(state.plan), "application/x-ndjson"));
$("download-plan").addEventListener("click", () =>
  download("plan.json", JSON.stringify(state.plan, null, 2), "application/json"));

$("export-state").addEventListener("click", () =>
  download("state.json", state.priorState, "application/json"));
$("import-state").addEventListener("change", async (e) => {
  const file = e.target.files[0];
  if (!file) return;
  const text = (await file.text()).trim();
  try {
    JSON.parse(text); // validity check only — the TEXT is what gets stored
  } catch {
    note("state-status", "that file is not JSON", "bad");
    return;
  }
  storeState(text);
});
$("clear-state").addEventListener("click", () => storeState(null));

for (const key of ["effort", "breakdown", "queue"]) {
  $(`tab-${key}`).addEventListener("click", () => selectTab(key));
}

state.priorState = loadStoredState();
renderStateStatus();

ready()
  .then(() => {
    $("engine-dot").className = "dot ok";
    $("engine-status").textContent =
      "planner loaded — running locally in this page, nothing is uploaded";
  })
  .catch((e) => {
    $("engine-dot").className = "dot bad";
    $("engine-status").textContent = `could not load the planner: ${e.message}`;
  });
