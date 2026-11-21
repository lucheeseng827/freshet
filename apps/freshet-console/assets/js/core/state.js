// Hosted state, as a console draws it: every commit is a row on one glaze line
// running down the ledger, and each names the models it moved with the landing page's
// three densities — fused where a fingerprint changed (the model was rebuilt whole), a
// thin run where only a partition ledger moved, bare where a model left the project.
//
// Snapshots reach these functions with fingerprints as the 16-digit lowercase hex
// `freshet fingerprint` prints. A fingerprint is a full-range u64 and a JavaScript
// number is exact only to 2^53, so hex is the one form no step here can round.

import { html, dialog, toast, fmtInt, when } from "./ui.js";

const SHOW = 8;

export const stamp = ({ text, density }) =>
  html`<span class="stamp${density === "run" ? " stamp--run" : density === "fuse" ? " stamp--fuse" : ""}">${text}</span>`;

/// What a commit moved, from the server's summary.
export function commitMoves(summary) {
  const s = summary || {};
  const list = (k) => (Array.isArray(s[k]) ? s[k].map(String) : []);
  return { changed: list("changed"), removed: list("removed"), watermarks: list("watermarks_changed"), context: s.context ?? null };
}

/// A plain sentence for a commit's client note (`{client, command, models_rebuilt}`).
export function contextLine(ctx) {
  if (!ctx || typeof ctx !== "object") return "";
  const parts = [];
  if (ctx.command) parts.push(String(ctx.command));
  if (ctx.client) parts.push(String(ctx.client));
  if (ctx.models_rebuilt != null) parts.push(`${ctx.models_rebuilt} rebuilt`);
  if (ctx.model) parts.push(`model ${ctx.model}`);
  if (ctx.partitions != null) parts.push(`${ctx.partitions} partition(s)`);
  return parts.join(" · ");
}

export function moveSentence(m) {
  const parts = [];
  if (m.changed.length) parts.push(`${m.changed.length} model${m.changed.length === 1 ? "" : "s"} rebuilt`);
  if (m.watermarks.length) parts.push(`${m.watermarks.length} partition ledger${m.watermarks.length === 1 ? "" : "s"} moved`);
  if (m.removed.length) parts.push(`${m.removed.length} model${m.removed.length === 1 ? "" : "s"} removed`);
  return parts.length ? parts.join(", ") : "Nothing moved";
}

function modelStamps(m) {
  const items = [
    ...m.changed.map((n) => ({ text: n, density: "fuse" })),
    ...m.watermarks.filter((n) => !m.changed.includes(n)).map((n) => ({ text: n, density: "run" })),
    ...m.removed.map((n) => ({ text: `${n} (removed)`, density: "bare" })),
  ];
  const shown = items.slice(0, SHOW);
  const more = items.length - shown.length;
  return html`${shown.map((i) => html`<li>${stamp(i)}</li>`)}${more > 0 ? html`<li><span class="stamp">+${more} more</span></li>` : ""}`;
}

/// The pour record's rows: a whole history page, or the overview's latest few.
///
/// `more(commit)`: what an edition adds under a commit's line, such as where it came
/// from or a way to bring it back; nothing by default.
export function pourRows(commits, { open = true, more = null } = {}) {
  return commits.map((c) => {
    const m = commitMoves(c.summary);
    const ctxLine = contextLine(m.context);
    return html`<li class="pour__row" data-version="${c.version}" tabindex="-1">
      <span class="pour__bead" aria-hidden="true"></span>
      <p class="pour__v"><span class="vh">Version </span>${c.version}<small>${when(c.committed_at)}</small></p>
      <div class="pour__what">
        <p class="pour__line">${moveSentence(m)}</p>
        ${m.changed.length + m.watermarks.length + m.removed.length ? html`<ul class="pour__models" aria-label="Models in version ${c.version}">${modelStamps(m)}</ul>` : ""}
        ${ctxLine ? html`<p class="pour__ctx">${ctxLine}</p>` : ""}
        ${more ? more(c) : ""}
      </div>
      ${open ? html`<button class="btn btn--quiet btn--sm" type="button" data-act="open" data-v="${c.version}" aria-label="Open version ${c.version}">Open</button>` : html`<span></span>`}
    </li>`;
  });
}

export const legend = html`<p class="legend">
  <span>${stamp({ text: "model", density: "fuse" })} rebuilt whole</span>
  <span>${stamp({ text: "model", density: "run" })} partitions moved</span>
  <span>${stamp({ text: "model", density: "bare" })} left the project</span>
</p>`;

/// One line for a model's partition ledger: its grain, what is built, what is owed.
export function ledgerSummary(raw) {
  if (raw == null) return "";
  try {
    const w = JSON.parse(raw);
    const grain = typeof w.grain === "string" ? w.grain : w.grain?.Integer ? `every ${w.grain.Integer.step}` : "—";
    const built = Array.isArray(w.built) && w.built.length ? `${w.built[0][0]} → ${w.built[w.built.length - 1][1]}` : "nothing built";
    const pending = Array.isArray(w.pending) && w.pending.length ? ` · ${w.pending.length} pending` : "";
    return `${grain} · ${built}${pending}${w.pending_full ? " · full refresh owed" : ""}`;
  } catch {
    return String(raw).slice(0, 80);
  }
}

/// A snapshot as the exact `state.json` a `freshet --state` file holds: fingerprints
/// written back as the unsigned 64-bit integers they are. Built as text, because
/// `JSON.stringify` of a JavaScript number cannot hold one.
export function stateFileText(snapshot) {
  const fps = Object.entries(snapshot.fingerprints || {})
    .map(([k, h]) => `    ${JSON.stringify(k)}: ${BigInt(`0x${h}`).toString()}`)
    .join(",\n");
  return `{\n  "fingerprints": {\n${fps}\n  },\n  "watermarks": ${JSON.stringify(snapshot.watermarks || {}, null, 2).replace(/\n/g, "\n  ")}\n}\n`;
}

/// Save `text` as a file the browser downloads.
export function download(name, text) {
  const blob = new Blob([text], { type: "application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = name;
  document.body.append(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

/// Every model in a snapshot, by name, with `hit` on the ones in `moved`.
export function snapshotRows(snapshot, moved = new Set()) {
  const { fingerprints = {}, watermarks = {} } = snapshot || {};
  return Object.keys(fingerprints).sort().map((name) => ({
    name, fp: fingerprints[name], wm: watermarks[name], hit: moved.has(name),
  }));
}

/// The snapshot's table with its filter box: `filterId` names the box, `tableId` the
/// table. The box carries `data-filter`, so `/` reaches it from anywhere on the page.
export function snapshotTable(rows, { filterId, tableId }) {
  return html`<div class="field"><label for="${filterId}">Find a model</label>
      <input class="input input--mono" id="${filterId}" type="search" autocomplete="off" spellcheck="false" placeholder="fct_orders" data-filter></div>
    <div class="table-scroll table-scroll--tall">
      <table class="rec" id="${tableId}">
        <caption>Fingerprints are the 16-digit hex that <code>freshet fingerprint</code> prints.</caption>
        <thead><tr><th scope="col">Model</th><th scope="col">Fingerprint</th><th scope="col">Partition ledger</th></tr></thead>
        <tbody>${rows.map((r) => html`<tr class="${r.hit ? "hl" : ""}" data-name="${r.name}"><th scope="row">${r.name}</th><td>${r.fp}</td><td class="wrap">${ledgerSummary(r.wm)}</td></tr>`)}</tbody>
      </table>
    </div>`;
}

/// Hide the rows of `table` whose model does not contain what `input` holds.
export function wireFilter(input, table) {
  const trs = [...table.querySelectorAll("tbody tr")];
  const apply = () => {
    const q = input.value.trim().toLowerCase();
    for (const tr of trs) tr.hidden = !!q && !tr.dataset.name.toLowerCase().includes(q);
  };
  input.addEventListener("input", apply);
  return () => input.removeEventListener("input", apply);
}

/// One version's snapshot in a dialog: every model, filterable, the ones the commit
/// moved washed, and a download of the exact state file. `commit` is
/// `{version, committed_at, summary, snapshot}`; the file is named `<stem>-state-v<n>.json`.
export async function openSnapshot(commit, { stem }) {
  const m = commitMoves(commit.summary);
  const rows = snapshotRows(commit.snapshot, new Set([...m.changed, ...m.watermarks]));
  const file = `${stem}-state-v${commit.version}.json`;
  const r = await dialog({
    title: `Version ${commit.version}`,
    wide: true,
    body: html`<p>${moveSentence(m)}${commit.committed_at ? html`, <time datetime="${commit.committed_at}">${new Date(commit.committed_at).toLocaleString()}</time>` : ""}.
      ${fmtInt(rows.length)} models in state; the ones this commit moved are washed.</p>
      ${snapshotTable(rows, { filterId: "snap-filter", tableId: "snap-table" })}`,
    actions: [
      { label: "Download as state.json", value: "download" },
      { label: "Close", value: "cancel", kind: "btn" },
    ],
    initial: "#snap-filter",
    onMount: (d) => wireFilter(d.querySelector("#snap-filter"), d.querySelector("#snap-table")),
  });
  if (r.value === "download") {
    download(file, stateFileText(commit.snapshot));
    toast(`Downloaded version ${commit.version} as a state file. Plan against it with --state ${file}.`);
  }
}
