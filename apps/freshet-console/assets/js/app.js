// Freshet console — boot, the token, the state picker, pages and the palette.
//
// One page over a freshet-state-server's `/v1` (api.js), or over the in-memory demo
// (demo.js) with `?demo`. The theme, focus mode and the hash router are the shared
// core's (core/shell.js). The page only reads: commits come from `freshet` itself, so
// a state keeps one write path whether or not anyone has this open.

import { html, mount, $, toast, announce, wireCopyButtons, dialog, copyText, actions } from "./core/ui.js";
import { installPrefs, prefCommands, setFocusMode, focusOn, createRouter, store } from "./core/shell.js";
import { installKeys, openPalette, openShortcuts } from "./core/keys.js";
import { createApi, ApiError } from "./api.js";
import { createDemoFetch } from "./demo.js";
import signin from "./views/signin.js";
import states from "./views/states.js";
import overview from "./views/overview.js";
import historyPage from "./views/history.js";
import snapshot from "./views/snapshot.js";

const ROUTES = { states, overview, history: historyPage, snapshot };
const GO_KEYS = { s: "states", o: "overview", h: "history", n: "snapshot" };
const TITLES = Object.fromEntries(Object.entries(GO_KEYS).map(([k, r]) => [k, ROUTES[r].title]));

const cfg = window.FRESHET_CONSOLE || {};
const DEMO = cfg.demo === true || new URL(location.href).searchParams.has("demo");
const TOKEN = "freshet.state.token";
const base = (cfg.apiBase || "").replace(/\/$/, "");

// ── the token: per tab unless the person asked to stay signed in ─────────────
let memoryToken = null;
function loadToken() {
  try { return sessionStorage.getItem(TOKEN) || localStorage.getItem(TOKEN); } catch { return null; }
}
function saveToken(token, remember) {
  memoryToken = token;
  try {
    sessionStorage.setItem(TOKEN, token);
    if (remember) localStorage.setItem(TOKEN, token); else localStorage.removeItem(TOKEN);
  } catch { /* the token lives in memory for this visit */ }
}
function clearToken() {
  memoryToken = null;
  try { sessionStorage.removeItem(TOKEN); localStorage.removeItem(TOKEN); } catch { /* nothing stored */ }
}
const currentToken = () => memoryToken || loadToken();

const api = createApi({ base, token: currentToken, fetch: DEMO ? createDemoFetch() : undefined });
// The server's own address, for every command the page prints.
const server = DEMO ? "https://state.example.com" : new URL(`${base}/`, location.href).href.replace(/\/$/, "");

const page = $("#page");
const ctx = {
  api, cfg, demo: DEMO, server,
  ready: false,
  states: [],
  access: null,
  state: null,
  stateUrl: (name) => `${server}/v1/${name}`,
  go(path) { if (location.hash === `#/${path}`) router.render(); else location.hash = `#/${path}`; },
  refresh: () => router.render({ keepFocus: true }),
  /// Route `[data-act]` clicks inside `el` to `handlers` until the page changes.
  bind(el, handlers) { router.track(actions(el, handlers)); },
  /// Run `off` when the page changes.
  track(off) { router.track(off); },
  /// Run an API call; a 401 ends the session, anything else becomes a toast. Either
  /// way the error is thrown on, so the caller stops where the call failed.
  async attempt(fn, { quiet = false } = {}) {
    try {
      return await fn();
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) { signOut("The server no longer accepts that token. Sign in again."); throw e; }
      if (!quiet) toast(e.message || String(e), { alert: true });
      throw e;
    }
  },
  /// Take a fresh listing: the picker follows it, and the chosen state stays chosen
  /// while it is still on the server.
  setStates(listing) {
    ctx.states = listing.states || [];
    ctx.access = listing.access || null;
    if (!ctx.states.some((s) => s.name === ctx.state)) {
      const saved = store.get("freshet.state");
      ctx.state = (ctx.states.find((s) => s.name === saved) || ctx.states[0])?.name || null;
    }
    paintScope();
  },
  setState(name) {
    ctx.state = name;
    store.set("freshet.state", name);
    paintScope();
  },
};

const router = createRouter({
  routes: ROUTES,
  fallback: "states",
  page,
  ctx,
  ready: () => ctx.ready,
  title: (view) => `${view.title} · ${ctx.state && view !== states ? ctx.state : "Freshet"} · Freshet`,
  quiet: (e) => e instanceof ApiError && e.status === 401,
});

// ── shell ────────────────────────────────────────────────────────────────────
function showShell(on) {
  $("#side").hidden = !on;
  $("#scope").hidden = !on || !ctx.states.length;
  $("#account").hidden = !on;
  $("#frame").classList.toggle("frame--bare", !on);
}

function paintScope() {
  const sel = $("#state-select");
  mount(sel, html`${ctx.states.map((s) => html`<option value="${s.name}" ${s.name === ctx.state ? html`selected` : ""}>${s.name}</option>`)}`);
  $("#scope").hidden = !ctx.ready || !ctx.states.length;
  // A stamp only when there is something to say: the demo, or a token that can commit.
  const stamp = $("#access");
  stamp.hidden = !DEMO && ctx.access !== "commit";
  stamp.className = ctx.access === "commit" && !DEMO ? "stamp stamp--run" : "stamp";
  stamp.textContent = DEMO ? "Demo · sample data" : "Commit token";
  $("#account").textContent = DEMO ? "Demo" : new URL(server).host;
}

$("#state-select").addEventListener("change", (e) => {
  ctx.setState(e.target.value);
  announce(`State: ${ctx.state}`);
  // The list of states names no state; any other page shows the one just picked.
  if (router.current().name === "states") ctx.go("overview"); else router.render({ keepFocus: true });
});
$("#open-palette").addEventListener("click", () => openPalette(commands()));
$("#account").addEventListener("click", accountDialog);

const ACCESS = {
  open: "No token: this server answers anyone who can reach it",
  read: "The read-only token",
  commit: "The token that commits (this page still only reads)",
};

async function accountDialog() {
  const r = await dialog({
    title: "This server",
    body: html`<dl class="keys">
      <dt>Server</dt><dd class="mono">${server}</dd>
      <dt>Signed in with</dt><dd>${DEMO ? "Nothing: the demo, on sample data" : ACCESS[ctx.access] || "—"}</dd>
      <dt>States</dt><dd>${ctx.states.length}</dd>
    </dl>`,
    actions: [
      { label: "Close", value: "cancel" },
      ...(DEMO ? [{ label: "Leave the demo", value: "signout", kind: "btn" }] : currentToken() ? [{ label: "Sign out", value: "signout", kind: "btn" }] : []),
    ],
  });
  if (r.value === "signout") signOut(DEMO ? null : "Signed out.");
}

function signOut(msg) {
  // Once: requests made together all come back 401, and each would sign out.
  if (!ctx.ready && !DEMO) return;
  clearToken();
  if (DEMO) { location.href = location.pathname; return; }
  // Whatever page is still working has lost its token: stale from here, so it cannot
  // paint over the sign-in page.
  router.invalidate();
  ctx.ready = false;
  showShell(false);
  showSignIn(msg);
}

// ── the palette's commands ───────────────────────────────────────────────────
function commands() {
  const pages = Object.entries(ROUTES).map(([id, v]) => ({ id: `go-${id}`, label: v.title, hint: `g ${Object.keys(GO_KEYS).find((k) => GO_KEYS[k] === id)}`, run: () => ctx.go(id) }));
  const acts = [
    ...(ctx.state ? [{ id: "copy-state", label: "Copy this state's address", hint: ctx.stateUrl(ctx.state), run: async () => { if (await copyText(ctx.stateUrl(ctx.state))) toast("Copied the state's address."); } }] : []),
    ...prefCommands(),
    { id: "keys", label: "Keyboard shortcuts", hint: "?", run: () => openShortcuts(GO_KEYS, TITLES) },
    ...(DEMO ? [{ id: "signout", label: "Leave the demo", hint: "", run: () => signOut(null) }] : currentToken() ? [{ id: "signout", label: "Sign out", hint: new URL(server).host, run: () => signOut("Signed out.") }] : []),
  ];
  const others = ctx.states.filter((s) => s.name !== ctx.state).map((s) => ({ id: `state-${s.name}`, label: `Switch to ${s.name}`, hint: "state", run: () => { ctx.setState(s.name); router.render({ keepFocus: true }); } }));
  return [...pages, ...acts, ...others];
}

// ── sign-in ──────────────────────────────────────────────────────────────────
function showSignIn(message) {
  document.title = "Sign in · Freshet";
  signin.render({
    api, message, server,
    onSignedIn: async (token, remember, listing) => { saveToken(token, remember); await start(listing); },
  }, page);
}

/// Open the console: the server's listing says whether it needs a token, and which.
async function start(listing = null) {
  try {
    listing = listing || (await api.states());
  } catch (e) {
    if (e instanceof ApiError && e.status === 401) {
      const had = !!currentToken();
      clearToken();
      showShell(false);
      showSignIn(had ? "The server no longer accepts that token. Sign in again." : null);
      return;
    }
    mount(page, html`<div class="gate"><div class="gate__card"><h1 tabindex="-1">Can't open the console</h1>
      <p class="note note--alert" role="alert">${e.message || String(e)}</p>
      <div class="actions"><button class="btn" type="button" id="retry">Try again</button></div></div></div>`);
    $("#retry", page).addEventListener("click", () => start());
    return;
  }
  ctx.ready = true;
  ctx.setStates(listing);
  showShell(true);
  paintScope();
  if (!location.hash || location.hash === "#") {
    history.replaceState(null, "", `${location.pathname}${location.search}#/${ctx.states.length ? "overview" : "states"}`);
  }
  router.reset();
  await router.render({ initial: true });
}

// ── boot ─────────────────────────────────────────────────────────────────────
function boot() {
  installPrefs();
  wireCopyButtons(document.body);
  installKeys({ commands, goKeys: GO_KEYS, titles: TITLES, onFocusMode: () => setFocusMode(!focusOn()) });
  start();
}

boot();
