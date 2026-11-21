// Keyboard: the command palette (⌘K / Ctrl+K), two-key "go to" sequences, and the
// shortcuts sheet. Single-character shortcuts can be switched off from the sheet
// (WCAG 2.1.4) — a speech-input or switch user can trigger them by accident — and
// none of them fire while focus is in a field.

import { html, mount, esc, dialog } from "./ui.js";

const PREF = "freshet.singleKeys";
export function singleKeysOn() {
  try { return localStorage.getItem(PREF) !== "off"; } catch { return true; }
}
function setSingleKeys(on) {
  try { localStorage.setItem(PREF, on ? "on" : "off"); } catch { /* per-visit only */ }
}

const typing = (el) =>
  el && (el.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName));

/// `commands()` returns the palette's current list: `{id, label, hint, run}`.
/// `goKeys` maps a letter to a route for the `g` sequences; `titles` names each
/// letter's page on the shortcuts sheet. `hint` is the palette's empty-list advice.
export function installKeys({ commands, goKeys, titles = {}, onFocusMode, hint }) {
  let pendingG = 0;

  document.addEventListener("keydown", (e) => {
    if (e.defaultPrevented) return;
    const mod = e.metaKey || e.ctrlKey;
    if (mod && e.key.toLowerCase() === "k") {
      e.preventDefault();
      openPalette(commands(), { hint });
      return;
    }
    if (mod || e.altKey || typing(e.target) || document.querySelector("dialog[open]")) return;
    if (!singleKeysOn()) return;

    if (pendingG && Date.now() - pendingG < 1200) {
      pendingG = 0;
      const to = goKeys[e.key.toLowerCase()];
      if (to) { e.preventDefault(); location.hash = `#/${to}`; }
      return;
    }
    if (e.key === "g") { pendingG = Date.now(); return; }
    if (e.key === "?") { e.preventDefault(); openShortcuts(goKeys, titles); return; }
    if (e.key === "/") {
      const f = document.querySelector("[data-filter]");
      if (f) { e.preventDefault(); f.focus(); }
      return;
    }
    if (e.key === ".") { e.preventDefault(); onFocusMode(); }
  });
}

// ── the palette: a combobox over a listbox, arrow keys and Enter ────────────
/// `hint` is what the empty list suggests trying, in this console's words.
export function openPalette(all, { hint = "Try a page name." } = {}) {
  const opener = document.activeElement;
  const d = document.createElement("dialog");
  d.className = "dlg palette";
  d.setAttribute("aria-label", "Command palette");
  mount(d, html`<div class="dlg__in">
    <input type="text" role="combobox" aria-expanded="true" aria-controls="pal-list" aria-autocomplete="list"
      aria-label="Go to a page or run a command" placeholder="Go to a page or run a command…" autocomplete="off" spellcheck="false">
    <ul id="pal-list" role="listbox" aria-label="Matches"></ul>
    <p class="palette__empty" hidden>Nothing matches. ${hint}</p>
    <div class="palette__foot"><span><kbd>↑</kbd> <kbd>↓</kbd> to move</span><span><kbd>Enter</kbd> to run</span><span><kbd>Esc</kbd> to close</span></div>
  </div>`);
  document.body.append(d);
  const input = d.querySelector("input");
  const list = d.querySelector("ul");
  const empty = d.querySelector(".palette__empty");
  let shown = all;
  let sel = 0;
  let ran = false;

  const score = (c, q) => {
    const hay = `${c.label} ${c.hint || ""}`.toLowerCase();
    if (!q) return 1;
    if (hay.startsWith(q)) return 3;
    if (hay.includes(` ${q}`)) return 2;
    return hay.includes(q) ? 1 : 0;
  };
  function paint() {
    const q = input.value.trim().toLowerCase();
    shown = all.map((c) => [score(c, q), c]).filter(([s]) => s > 0).sort((a, b) => b[0] - a[0]).map(([, c]) => c);
    sel = Math.min(sel, Math.max(0, shown.length - 1));
    list.innerHTML = shown.map((c, i) =>
      `<li role="option" id="pal-${i}" aria-selected="${i === sel}" data-i="${i}"><b>${esc(c.label)}</b>${c.hint ? `<span>${esc(c.hint)}</span>` : ""}</li>`).join("");
    empty.hidden = shown.length > 0;
    if (shown.length) {
      input.setAttribute("aria-activedescendant", `pal-${sel}`);
      list.querySelector(`#pal-${sel}`)?.scrollIntoView({ block: "nearest" });
    } else {
      input.removeAttribute("aria-activedescendant");
    }
  }
  function run(i) {
    const c = shown[i];
    if (!c) return;
    ran = true;
    d.close();
    // Run after close so the command can move focus itself (a route change focuses
    // the new page's heading) without the dialog's focus return undoing it.
    setTimeout(() => c.run(), 0);
  }
  input.addEventListener("input", () => { sel = 0; paint(); });
  input.addEventListener("keydown", (e) => {
    if (e.key === "ArrowDown") { e.preventDefault(); sel = (sel + 1) % Math.max(1, shown.length); paint(); }
    else if (e.key === "ArrowUp") { e.preventDefault(); sel = (sel - 1 + shown.length) % Math.max(1, shown.length); paint(); }
    else if (e.key === "Enter") { e.preventDefault(); run(sel); }
  });
  list.addEventListener("click", (e) => {
    const li = e.target.closest("li[data-i]");
    if (li) run(+li.dataset.i);
  });
  d.addEventListener("close", () => {
    d.remove();
    if (!ran && opener && document.contains(opener)) opener.focus();
  });
  d.showModal();
  paint();
  input.focus();
}

export async function openShortcuts(goKeys, titles = {}) {
  const names = titles;
  const on = singleKeysOn();
  const r = await dialog({
    title: "Keyboard shortcuts",
    wide: true,
    body: html`<dl class="keys">
        <dt><kbd>⌘</kbd> <kbd>K</kbd> or <kbd>Ctrl</kbd> <kbd>K</kbd></dt><dd>Go to a page or run a command</dd>
        ${Object.keys(goKeys).map((k) => html`<dt><kbd>g</kbd> then <kbd>${k}</kbd></dt><dd>${names[k] || goKeys[k]}</dd>`)}
        <dt><kbd>/</kbd></dt><dd>Filter the list on this page</dd>
        <dt><kbd>.</kbd></dt><dd>Focus mode: hide the sidebar and follow your place with a reading ruler</dd>
        <dt><kbd>?</kbd></dt><dd>This sheet</dd>
      </dl>
      <p>Single-key shortcuts are <strong>${on ? "on" : "off"}</strong>. Turn them off if you use speech
        input or a switch device; <kbd>⌘</kbd>/<kbd>Ctrl</kbd> <kbd>K</kbd> always works.</p>`,
    actions: [
      { label: on ? "Turn single-key shortcuts off" : "Turn single-key shortcuts on", value: "toggle" },
      { label: "Done", value: "cancel", kind: "btn" },
    ],
  });
  if (r.value === "toggle") setSingleKeys(!on);
}
