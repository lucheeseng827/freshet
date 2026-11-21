# Freshet console

A web page for the states a `freshet-state-server` keeps. It lists every state, every
commit to one with what that commit moved, and the snapshot each version left, which
you can download as the exact `state.json` to plan against offline.

**It only reads.** Commits come from `freshet` itself (`freshet plan --commit`), so a
state keeps one write path whether or not anyone has the page open.

```
freshet-console/
  index.html        the shell: header, sidebar, one <main>, no inline script or style
  config.js         the one file an install replaces (API base, demo)
  assets/
    console.css     the whole stylesheet, both themes
    theme.js        sets light/dark and focus mode before first paint (blocking, tiny)
    fonts/          Antonio · Figtree · JetBrains Mono (OFL; licences alongside)
    js/core/        the shared core: ui.js (escaped templates, dialogs, toasts),
                    keys.js (⌘K palette, g-sequences), shell.js (theme, focus mode,
                    the router), json.js (exact u64s), page.js, state.js (the commit
                    ledger and the snapshot table)
    js/app.js       boot, the token, the state picker, the palette's commands
    js/api.js       one function per route of the state API
    js/demo.js      the same API answered in memory from sample data (?demo)
    js/views/*.js   one module per page
  tests/
    unit.test.mjs   node --test: the core, the client and the demo
    e2e.mjs         every page in Chromium, served by a real freshet-state-server
```

There is no build step, no bundler and no `package.json`. The browser loads plain ES
modules as they are.

## Quickstart

**Look at it with no server at all.** The demo answers every call from sample data:

```sh
cd apps/freshet-console
python3 -m http.server 8620        # then open http://127.0.0.1:8620/?demo
```

Serve it over HTTP, not `file://`: ES modules and font preloads are CORS requests.

**Serve it from the state server.** `--console` names this directory, and the server
serves it at `/console/` on the API's own origin, so there is no CORS and the token
never crosses an origin:

```sh
export FRESHET_STATE_SERVER_TOKEN="$(openssl rand -hex 32)"
export FRESHET_STATE_SERVER_READ_TOKEN="$(openssl rand -hex 32)"
freshet-state-server --dir /var/lib/freshet-state --console apps/freshet-console
```

```text
serving states from /var/lib/freshet-state on http://127.0.0.1:8790/v1/<name>
console at http://127.0.0.1:8790/console/ (from …/apps/freshet-console)
```

Open `http://127.0.0.1:8790/` (it sends you to `/console/`) and paste the read-only
token. In the image, the console is on already: `FRESHET_STATE_SERVER_CONSOLE` points
at its copy, and setting that variable to empty turns it off.

A `--console` directory that does not exist, or has no `index.html`, stops the server
at start with the reason. Otherwise the failure would be a `/console/` that 404s for no
visible cause.

## Pages

| Page | What it shows |
|---|---|
| Every state | every state the server keeps: its version, last commit and models |
| Overview | one state's version, commits this week, models and partition ledgers; the latest commits; the commands to reach it for each kind of person |
| History | every commit kept, newest first, as a glaze line: what each moved; open any version's snapshot; download it as an exact `state.json` |
| Snapshot | what plans read now: every model's fingerprint and partition ledger, filterable, and the download |

**For each kind of person.** The Overview's Connect panel carries this state's real
address and version in every command:
- **Data engineer:** plan → run → commit in CI, with `--expect-version`.
- **Analyst:** what my branch would rebuild, plus `freshet explain` for a dbt project.
- **Data scientist:** the state in a notebook, with stdlib `urllib`. Python's integers
  hold a u64 fingerprint exactly; JavaScript's do not.

**Ease of focus.**
- Every route change moves focus to the new page's heading.
- Every control has a visible 2px focus ring.
- `⌘K` / `Ctrl K` opens the palette. `g` then a letter jumps to a page. `/` jumps to the
  page's filter. `.` toggles focus mode. `?` shows every shortcut.
- Single-key shortcuts can be switched off (WCAG 2.1.4).
- **Focus mode** hides the sidebar and grain and narrows the page to a reading measure.
- **Dark mode** follows the system until you pick one, and remembers your choice.

## Tokens

The server takes bearer tokens, not accounts, so signing in is pasting one:

| Token | Here |
|---|---|
| `FRESHET_STATE_SERVER_READ_TOKEN` | Reads everything the console shows. The right one for a browser. |
| `FRESHET_STATE_SERVER_TOKEN` | Works too, and the page says it is more than the page needs. |
| none (a server with no tokens) | The page opens with no sign-in; the server answers anyone who can reach it. |

The token is kept per tab in `sessionStorage`, and moves to `localStorage` only if you
tick *Stay signed in on this device*. A `401` ends it, and signing out forgets it.

## Configuration

`config.js` is the only file an install replaces:

| Key | Default | What it does |
|---|---|---|
| `apiBase` | `""` | Where the state API answers. `""` is the page's own origin, which is right when the server serves the console. The server answers no cross-origin requests, so anything else means a proxy that puts both under one origin. |
| `demo` | `false` | `true` runs on sample data with no server. `?demo` in the URL does the same for one visit. |

## API the console calls

Every route is `GET`, with `Authorization: Bearer <token>` when the server has tokens.
Errors are `{"error": "…"}`, and `api.js` shows the server's own words.

| Route | Answer |
|---|---|
| `/v1/` | `{"states": [{"name", "version", "committed_at", "models"}], "access"}`; `access` is `open`, `read` or `commit` |
| `/v1/{name}/current` | `{"version", "snapshot"}` |
| `/v1/{name}/history?limit=&before=` | `{"commits": [{"version", "committed_at", "summary"}], "limit", "next_before"}` |
| `/v1/{name}/versions/{v}` | `{"version", "committed_at", "summary", "snapshot"}`, or `404` once retention (`--keep`) dropped it |

**Fingerprints become hex in the browser, on purpose.** They are full-range u64s, and
a JavaScript number is exact only to 2^53, so `JSON.parse` would round them and every
model would look changed. The client parses with `core/json.js`, which keeps a big
integer as a `BigInt`, and shows each fingerprint as the 16-digit hex
`freshet fingerprint` prints. *Download as state.json* writes them back as the exact
integers they are.

## Security posture

- **Same origin, strict CSP.** The server sends `default-src 'self'` and allows no inline
  script or style. The e2e test runs under that policy and fails on any violation.
- **Every string from the server is escaped.** That covers state names, model names and
  a commit's note. `html` in `core/ui.js` escapes every interpolation that is not a
  literal of this codebase.
- **Read-only.** There is no write route in `api.js`, and the demo refuses one.
- The page itself needs no token: its files are public, and what they read is not.

## The shared core

`assets/js/core/`, the stylesheet, the theme and the fonts hold nothing about state
servers in particular: another console can be built on them, adding pages and an API
client of its own, the way this one adds `app.js`, `api.js`, `demo.js` and `views/`.
A change to the core is then a change to every console built on it.

## Tests

```sh
cargo build -p freshet-state-server      # from the workspace root, for the e2e
cd apps/freshet-console
node --test tests/unit.test.mjs          # no dependencies
node tests/e2e.mjs                       # Playwright + Chromium
SHOTS=/tmp/shots node tests/e2e.mjs      # …and a screenshot of every page
```

`e2e.mjs` starts the server on a port it picks itself, seeds states through the commit
route the CLI uses, and fails on:
- a console error, an exception or a CSP violation
- a control with no accessible name
- focus not reaching a page's heading
- sideways scroll at 390px
- any of its flows misbehaving: the token sign-in, wrong and right; a u64 past 2^53
  shown as its exact hex and downloaded digit for digit; the retention note; the
  filter; the palette, `g`-sequences, theme and focus mode; switching states; the
  commit-token note; signing out

## Licence

Apache-2.0, like the rest of Freshet. The vendored fonts are SIL OFL 1.1
(`assets/fonts/LICENSES.md`).
