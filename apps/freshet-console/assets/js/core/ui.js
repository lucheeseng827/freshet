// The console's small DOM kit: escaped templates, dialogs that trap and return focus,
// announced toasts, copy, and time. No framework — the page is a handful of views over
// one API, and every string someone else wrote (a state's name, a commit's note, a
// model's name) reaches the DOM through `html`, which escapes it.

export class Safe {
  constructor(s) { this.s = s; }
  toString() { return this.s; }
}

const ESC = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
export function esc(v) {
  return String(v).replace(/[&<>"']/g, (c) => ESC[c]);
}

function part(v) {
  if (v == null || v === false || v === true) return "";
  if (v instanceof Safe) return v.s;
  if (Array.isArray(v)) return v.map(part).join("");
  return esc(v);
}

/// Tagged template: every interpolation is escaped unless it is already `Safe`.
export function html(strings, ...vals) {
  let out = strings[0];
  for (let i = 0; i < vals.length; i++) out += part(vals[i]) + strings[i + 1];
  return new Safe(out);
}

/// Trusted markup only — literals written in this codebase, never data.
export const raw = (s) => new Safe(s);

/// Only http(s) and same-origin paths may become an href; anything else (a
/// `javascript:` URL a tenant typed into a field) renders inert.
export function safeUrl(u) {
  try {
    const url = new URL(u, location.href);
    return url.protocol === "https:" || url.protocol === "http:" ? url.href : "#";
  } catch {
    return "#";
  }
}

export function mount(el, safe) {
  el.innerHTML = safe instanceof Safe ? safe.s : esc(safe);
}

export const $ = (sel, root = document) => root.querySelector(sel);
export const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

/// Route every click on `[data-act]` inside `root` to `handlers[name](el, event)`.
/// One listener per view, rebound on each render, so a re-render never leaves a
/// handler pointing at a node that is gone.
export function actions(root, handlers) {
  const onClick = (e) => {
    const el = e.target.closest("[data-act]");
    if (!el || !root.contains(el)) return;
    const fn = handlers[el.dataset.act];
    if (fn) { e.preventDefault(); fn(el, e); }
  };
  root.addEventListener("click", onClick);
  return () => root.removeEventListener("click", onClick);
}

// ── time ────────────────────────────────────────────────────────────────
const RTF = new Intl.RelativeTimeFormat("en", { numeric: "auto" });
export function timeAgo(iso) {
  if (!iso) return "never";
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return String(iso);
  const s = Math.round((t - Date.now()) / 1000);
  const abs = Math.abs(s);
  if (abs < 45) return s <= 0 ? "just now" : "in a moment";
  const steps = [[60, "second"], [3600, "minute"], [86400, "hour"], [2592000, "day"], [31536000, "month"], [Infinity, "year"]];
  let prev = 1;
  for (const [limit, unit] of steps) {
    if (abs < limit) return RTF.format(Math.round(s / prev), unit);
    prev = limit;
  }
  return RTF.format(Math.round(s / 31536000), "year");
}
export function absTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toISOString().replace("T", " ").replace(/:\d\d(\.\d+)?Z$/, " UTC");
}
/// A `<time>` whose text is relative and whose title and datetime are exact.
export function when(iso, fallback = "never") {
  if (!iso) return html`<span>${fallback}</span>`;
  return html`<time datetime="${iso}" title="${absTime(iso)}">${timeAgo(iso)}</time>`;
}

export const fmtInt = (n) => (n == null ? "—" : Number(n).toLocaleString("en"));

// ── announcements and toasts ────────────────────────────────────────────
export function announce(msg) {
  const live = document.getElementById("live");
  if (!live) return;
  live.textContent = "";
  // A fresh node each time, so a screen reader reads a repeat of the same words.
  requestAnimationFrame(() => { live.textContent = msg; });
}

export function toast(msg, { alert = false, ms = 6000 } = {}) {
  const box = document.getElementById("toasts");
  if (!box) return;
  const t = document.createElement("div");
  t.className = alert ? "toast toast--alert" : "toast";
  t.setAttribute("role", alert ? "alert" : "status");
  t.textContent = msg;
  box.append(t);
  setTimeout(() => t.remove(), alert ? ms + 4000 : ms);
}

// ── copy ────────────────────────────────────────────────────────────────
export async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    // An insecure origin or a denied permission. Fall back to a selection copy,
    // which older and locked-down browsers still allow from a click.
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.cssText = "position:fixed;left:-9999px;top:0";
    document.body.append(ta);
    ta.select();
    let ok = false;
    try { ok = document.execCommand("copy"); } catch { ok = false; }
    ta.remove();
    return ok;
  }
}

/// A sapphire transcript. `lines` are `{t: "typed command"}`, `{k: "code, no prompt"}`,
/// `{o: "output"}` or `{c: "# comment"}`; Copy takes only what the reader would
/// type or paste — commands and code — one per line.
export function term(title, lines, caption) {
  const body = lines.map((l) => {
    if (l.t != null) return html`<span class="p">$ </span>${l.t}\n`;
    if (l.k != null) return html`${l.k}\n`;
    if (l.c != null) return html`<span class="c">${l.c}</span>\n`;
    return html`<span class="o">${l.o}</span>\n`;
  });
  const typed = lines.filter((l) => l.t != null || l.k != null).map((l) => l.t ?? l.k).join("\n");
  return html`<figure class="term">
    <div class="term__bar">${title}</div>
    <button class="copy" type="button" data-copy="${typed}" aria-label="Copy the commands in ${title}">Copy</button>
    <pre><code>${body}</code></pre>
    ${caption ? html`<figcaption>${caption}</figcaption>` : ""}
  </figure>`;
}

/// One delegated handler for every transcript's Copy button on the page.
export function wireCopyButtons(root) {
  root.addEventListener("click", async (e) => {
    const b = e.target.closest("button[data-copy]");
    if (!b) return;
    const ok = await copyText(b.dataset.copy);
    if (ok) {
      b.textContent = "Copied";
      b.dataset.done = "";
      announce("Copied to the clipboard");
      setTimeout(() => { b.textContent = "Copy"; delete b.dataset.done; }, 1600);
    } else {
      toast("Couldn't reach the clipboard. Select the text and copy it by hand.", { alert: true });
    }
  });
}

// ── dialogs ─────────────────────────────────────────────────────────────
// A native <dialog> opened modal: the browser makes the rest of the page inert and
// traps Tab inside it; this adds the two things it does not do — returning focus to
// whatever opened it, and resolving with the button that closed it.

export function dialog({ title, body, actions: acts = [], wide = false, initial = null, onMount = null }) {
  return new Promise((resolve) => {
    const opener = document.activeElement;
    const d = document.createElement("dialog");
    d.className = wide ? "dlg dlg--wide" : "dlg";
    const id = `dlg-${Math.random().toString(36).slice(2, 8)}`;
    d.setAttribute("aria-labelledby", `${id}-h`);
    mount(d, html`<form method="dialog" class="dlg__in">
      <h2 id="${id}-h">${title}</h2>
      ${body}
      <div class="dlg__foot">
        ${acts.map((a) => html`<button class="${a.kind || "btn btn--ghost"}" value="${a.value}" ${a.disabled ? raw("disabled") : ""} ${a.value === "cancel" ? raw("formnovalidate") : ""}>${a.label}</button>`)}
      </div>
    </form>`);
    document.body.append(d);
    let result = null;
    d.addEventListener("close", () => {
      resolve(result ?? { value: d.returnValue || "cancel", form: d.querySelector("form") });
      d.remove();
      if (opener && typeof opener.focus === "function" && document.contains(opener)) opener.focus();
    });
    d.addEventListener("submit", (e) => {
      // The browser has already validated the form for every button but Cancel
      // (which is `formnovalidate`), so the button is all that is left to read.
      result = { value: e.submitter?.value || "ok", form: d.querySelector("form") };
    });
    // Enter in a field means the dialog's main action. Left to the browser it would
    // be the form's first button, which is Cancel. A click still validates the form,
    // and a disabled main action (a name not yet typed) leaves Enter doing nothing.
    d.addEventListener("keydown", (e) => {
      if (e.key !== "Enter" || e.isComposing || e.target.tagName !== "INPUT") return;
      e.preventDefault();
      const main = d.querySelector(".dlg__foot button:last-child");
      if (main && !main.disabled) main.click();
    });
    d.showModal();
    if (onMount) onMount(d);
    const first = initial ? d.querySelector(initial) : d.querySelector("input, select, textarea, button");
    first?.focus();
  });
}

/// A destructive action asks for the thing's own name, typed. A click can be a slip;
/// typing a name cannot. The confirm button stays disabled until it matches.
export async function confirmTyped({ title, what, phrase, confirm }) {
  const r = await dialog({
    title,
    body: html`<div class="form">${what}
      <div class="field">
        <label for="confirm-phrase">Type <code>${phrase}</code> to confirm</label>
        <input class="input input--mono" id="confirm-phrase" name="phrase" autocomplete="off" spellcheck="false" required>
      </div></div>`,
    actions: [
      { label: "Keep it", value: "cancel" },
      { label: confirm, value: "ok", kind: "btn btn--pool", disabled: true },
    ],
    initial: "#confirm-phrase",
    onMount: (d) => {
      const input = d.querySelector("#confirm-phrase");
      const go = d.querySelector('button[value="ok"]');
      input.addEventListener("input", () => { go.disabled = input.value.trim() !== phrase; });
    },
  });
  return r.value === "ok";
}

export async function confirmPlain({ title, what, confirm, kind = "btn" }) {
  const r = await dialog({
    title,
    body: what,
    actions: [
      { label: "Cancel", value: "cancel" },
      { label: confirm, value: "ok", kind },
    ],
    initial: 'button[value="ok"]',
  });
  return r.value === "ok";
}
