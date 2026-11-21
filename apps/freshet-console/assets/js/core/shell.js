// The parts of a console's shell that do not depend on what it shows: the theme,
// focus mode, and the hash router. Routes are hash routes (`#/history`), so the page
// works from any static host and a deep link is just a URL. Every route change moves
// focus to the new page's heading, so a keyboard or screen-reader user lands where the
// content starts instead of back at the top of the sidebar.

import { html, mount, $, $$, announce } from "./ui.js";

const store = {
  get(k) { try { return localStorage.getItem(k); } catch { return null; } },
  set(k, v) { try { localStorage.setItem(k, v); } catch { /* per-visit only */ } },
  del(k) { try { localStorage.removeItem(k); } catch { /* nothing to clear */ } },
};
export { store };

// ── theme and focus mode ─────────────────────────────────────────────────────
// theme.js applied both before first paint; these keep them in step afterwards.

export const THEMES = ["system", "light", "dark"];
export function themeChoice() { const t = store.get("freshet.theme"); return THEMES.includes(t) ? t : "system"; }
export function applyTheme() {
  const choice = themeChoice();
  const dark = choice === "dark" || (choice === "system" && matchMedia("(prefers-color-scheme: dark)").matches);
  document.documentElement.dataset.theme = dark ? "dark" : "light";
  const label = { system: "System", light: "Light", dark: "Dark" }[choice];
  $("#theme-label").textContent = label;
  $("#theme-toggle").setAttribute("aria-label", `Theme: ${label}${choice === "system" ? ` (${dark ? "dark" : "light"} now)` : ""}. Change theme`);
}
export function setTheme(choice) { store.set("freshet.theme", choice); applyTheme(); announce(`Theme: ${choice}`); }
export function cycleTheme() { setTheme(THEMES[(THEMES.indexOf(themeChoice()) + 1) % THEMES.length]); }

export function focusOn() { return document.documentElement.dataset.focus === "on"; }
export function setFocusMode(on) {
  document.documentElement.dataset.focus = on ? "on" : "off";
  store.set("freshet.focus", on ? "on" : "off");
  $("#focus-toggle").setAttribute("aria-pressed", String(on));
  announce(on ? "Focus mode on: sidebar hidden, reading ruler on" : "Focus mode off");
}

/// Paint the theme and focus controls and wire them up.
export function installPrefs() {
  applyTheme();
  $("#focus-toggle").setAttribute("aria-pressed", String(focusOn()));
  matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => { if (themeChoice() === "system") applyTheme(); });
  $("#theme-toggle").addEventListener("click", cycleTheme);
  $("#focus-toggle").addEventListener("click", () => setFocusMode(!focusOn()));
}

/// The palette's commands every console has: focus mode and the three themes.
export function prefCommands() {
  return [
    { id: "focus", label: focusOn() ? "Turn focus mode off" : "Turn focus mode on", hint: ".", run: () => setFocusMode(!focusOn()) },
    ...THEMES.map((t) => ({ id: `theme-${t}`, label: `Theme: ${t}`, hint: t === themeChoice() ? "current" : "", run: () => setTheme(t) })),
  ];
}

// ── router ───────────────────────────────────────────────────────────────────

/// `routes` maps a name to a view `{title, render(ctx, el, params)}`; an unknown name
/// is `fallback`. `ready()` says whether there is anything to render yet (signed in).
/// `title(view)` is the document title for a view. `quiet(e)` says an error is already
/// handled elsewhere (a session that ended), so the page should not show it.
export function createRouter({ routes, fallback, page, ctx, ready, title, quiet = () => false }) {
  let seq = 0;
  let lastRoute = "";
  let cleanups = [];

  function current() {
    const raw = location.hash.replace(/^#\/?/, "");
    const [name, ...params] = raw.split("/").filter(Boolean);
    return { name: routes[name] ? name : fallback, params, raw };
  }

  async function render({ keepFocus = false, initial = false } = {}) {
    if (!ready()) return;
    const { name, params, raw } = current();
    const view = routes[name];
    const mine = ++seq;
    for (const off of cleanups) off();
    cleanups = [];
    for (const a of $$(".side a[data-route]")) {
      if (a.dataset.route === name) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current");
    }
    document.title = title(view);
    const stale = () => mine !== seq;
    ctx.stale = stale;
    // The view gets the shared ctx with its own `stale`: one that stays true once this
    // render is superseded, however long its async work runs. Reads and writes go
    // through to the shared ctx, so a view that updates the ctx still does.
    const viewCtx = new Proxy(ctx, { get: (t, k) => (k === "stale" ? stale : Reflect.get(t, k, t)) });
    page.setAttribute("aria-busy", "true");
    try {
      await view.render(viewCtx, page, params);
    } catch (e) {
      if (mine !== seq) return;
      if (quiet(e)) return;
      mount(page, html`<div class="page__head"><div><h1 tabindex="-1">${view.title}</h1></div></div>
        <div class="note note--alert" role="alert"><p><strong>This page couldn't load.</strong> ${e.message || String(e)}</p>
        <div class="actions"><button class="btn btn--ghost btn--sm" type="button" id="retry">Try again</button></div></div>`);
      $("#retry", page)?.addEventListener("click", () => render());
    } finally {
      if (mine === seq) page.removeAttribute("aria-busy");
    }
    if (mine !== seq) return;
    const moved = raw !== lastRoute;
    lastRoute = raw;
    // On a route change, not on the first paint: arriving at the console, focus is
    // already at the top of the document, where the skip link is.
    if (moved && !keepFocus && !initial) $("h1", page)?.focus({ preventScroll: false });
  }

  window.addEventListener("hashchange", () => render());

  return {
    render,
    current,
    /// Whatever page is still working is stale from here (signing out), so it cannot
    /// paint over what replaces it.
    invalidate() { seq++; },
    /// The next render starts from no route, so its first paint is not a "change".
    reset() { lastRoute = ""; },
    /// Run `off` when the page changes.
    track(off) { cleanups.push(off); },
  };
}
