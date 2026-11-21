// End-to-end: the console in a real browser, served by a real freshet-state-server
// (`--console`), under the policy that server sends, against states seeded through
// the same commit route the CLI uses. Then the demo, which needs no server.
//
//   cargo build -p freshet-state-server              # from the workspace root
//   node tests/e2e.mjs                               # from apps/freshet-console
//   SHOTS=/tmp/shots node tests/e2e.mjs              # also save a screenshot of every page
//
// FRESHET_STATE_SERVER names the binary; unset, the workspace's target/debug one.
// Needs Playwright (any recent version) and a Chromium: a global install is used if
// there is one, and PLAYWRIGHT_BROWSERS_PATH's Chromium when set. No npm install into
// this directory — the console ships with no package.json on purpose.
//
// What fails the run: a console error, an uncaught exception, a CSP violation, a
// control with no accessible name, focus not landing on a page's heading after a
// route change, sideways scroll on a phone, or any flow below not doing what it says.

import { spawn, execSync } from "node:child_process";
import { mkdtemp, mkdir, readFile, rm } from "node:fs/promises";
import { existsSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { stringifyExact } from "../assets/js/core/json.js";

const APP = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const WORKSPACE = resolve(APP, "../..");
const READ = "r-e2e-read-token";
const COMMIT = "c-e2e-commit-token";
const BIG = 16899773155021735464n; // past 2^53: a JSON.parse of it would round
const BIG_HEX = BIG.toString(16).padStart(16, "0");

function loadPlaywright() {
  const require = createRequire(import.meta.url);
  let globalRoot = "";
  try { globalRoot = execSync("npm root -g", { encoding: "utf8" }).trim(); } catch { /* no npm on PATH */ }
  const tries = ["playwright", globalRoot && join(globalRoot, "playwright"), "/opt/node22/lib/node_modules/playwright"].filter(Boolean);
  for (const t of tries) {
    try { return require(t); } catch { /* next */ }
  }
  throw new Error("Playwright not found. Install it globally (npm i -g playwright) or run with NODE_PATH pointing at it.");
}

function chromiumPath() {
  const base = process.env.PLAYWRIGHT_BROWSERS_PATH;
  if (!base || !existsSync(base)) return undefined;
  const dir = readdirSync(base).filter((d) => /^chromium-\d+$/.test(d)).sort().pop();
  const exe = dir && join(base, dir, "chrome-linux", "chrome");
  return exe && existsSync(exe) ? exe : undefined;
}

/// The server, on a port it picks itself (`--port 0`), and the address it says it bound.
async function startServer(dir) {
  const bin = process.env.FRESHET_STATE_SERVER || join(WORKSPACE, "target/debug/freshet-state-server");
  if (!existsSync(bin)) throw new Error(`no freshet-state-server at ${bin}: cargo build -p freshet-state-server, or set FRESHET_STATE_SERVER`);
  const child = spawn(bin, ["--dir", dir, "--port", "0", "--keep", "3", "--console", APP], {
    env: { ...process.env, FRESHET_STATE_SERVER_TOKEN: COMMIT, FRESHET_STATE_SERVER_READ_TOKEN: READ },
    stdio: ["ignore", "ignore", "pipe"],
  });
  const base = await new Promise((ok, fail) => {
    let err = "";
    const timer = setTimeout(() => fail(new Error(`the server said nothing usable in 10s:\n${err}`)), 10000);
    child.stderr.on("data", (b) => {
      err += b;
      const m = err.match(/on (http:\/\/[\d.]+:\d+)\/v1/);
      if (m) { clearTimeout(timer); ok(m[1]); }
    });
    child.on("exit", (code) => fail(new Error(`the server exited (${code}):\n${err}`)));
  });
  return { child, base };
}

const ledger = (end) => JSON.stringify({ v: 2, grain: "Day", built: [["2026-07-01", end]], pending: [], pending_full: false });

/// Commit through the route the CLI commits through, with the commit token.
async function commit(base, name, expected, fingerprints, watermarks, summary) {
  const body = stringifyExact({ expected_version: expected, snapshot: { fingerprints, watermarks }, summary });
  const r = await fetch(`${base}/v1/${name}/commit`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${COMMIT}` },
    body,
  });
  if (!r.ok) throw new Error(`seeding ${name}: ${r.status} ${await r.text()}`);
}

async function seed(base) {
  // Five commits on a server that keeps three: versions 3–5 stay, 1–2 are gone.
  let fps = { stg_orders: BIG, fct_orders: 1n, dim_customers: 2n };
  let wms = { fct_orders: ledger("2026-10-20") };
  await commit(base, "analytics", null, fps, wms, { command: "import" });
  for (let v = 2; v <= 5; v++) {
    fps = { ...fps, fct_orders: BigInt(v) * 1000003n };
    if (v === 4) wms = { fct_orders: ledger("2026-10-27") };
    await commit(base, "analytics", v - 1, fps, wms, { client: "freshet/0.1.0", command: "plan --commit", models_rebuilt: 1 });
  }
  await commit(base, "marketing", null, { stg_web_visits: 7n }, {}, { command: "import" });
}

let failures = 0;
const check = (cond, what) => {
  if (cond) { console.log(`  ok   ${what}`); return; }
  failures += 1;
  console.log(`  FAIL ${what}`);
};

/// The page, watched: console errors, exceptions and CSP violations land in `problems`.
async function watched(browser, scheme, opts = {}) {
  const context = await browser.newContext({ colorScheme: scheme, viewport: { width: 1360, height: 900 }, reducedMotion: "reduce", acceptDownloads: true, ...opts });
  const page = await context.newPage();
  const problems = [];
  page.on("console", (m) => {
    // A 401 is how the server says "sign in"; the browser logs the response it got.
    if (m.type() === "error" && !/status of 401/.test(m.text())) problems.push(`console: ${m.text()}`);
  });
  page.on("pageerror", (e) => problems.push(`page error: ${e.message}`));
  await page.addInitScript(() => {
    document.addEventListener("securitypolicyviolation", (e) => console.error(`CSP: ${e.violatedDirective} ${e.blockedURI}`));
  });
  return { context, page, problems };
}

async function settled(page, route) {
  await page.waitForFunction((r) => location.hash === `#/${r}` && document.querySelector(`.side a[data-route="${r}"]`)?.getAttribute("aria-current") === "page" && !document.querySelector("#page[aria-busy]"), route);
  await page.waitForTimeout(120);
}

async function unnamed(page) {
  return page.evaluate(() => {
    const bad = [];
    for (const el of document.querySelectorAll("button, a[href], input, select, textarea")) {
      if (el.type === "hidden" || el.closest("[hidden]")) continue;
      const labelled = el.getAttribute("aria-label") || el.labels?.length || el.textContent.trim() || el.getAttribute("title") || el.getAttribute("aria-labelledby");
      if (!labelled) bad.push(el.outerHTML.slice(0, 80));
    }
    return bad;
  });
}

async function onServer(browser, base, shots) {
  for (const scheme of ["light", "dark"]) {
    console.log(`\nserver · ${scheme}`);
    const { context, page, problems } = await watched(browser, scheme);

    await page.goto(`${base}/`);
    await page.waitForSelector("#signin");
    check(new URL(page.url()).pathname === "/console/", "the server's root sends the browser to /console/");
    check(await page.evaluate(() => document.documentElement.dataset.theme) === scheme, `the ${scheme} firing follows the system`);
    check(await page.evaluate(() => document.activeElement === document.querySelector("#page h1")), "sign-in: the heading takes focus");

    await page.click('#signin button[type="submit"]');
    check((await page.textContent("#signin-err")).includes("Paste the server's token"), "an empty token is refused in words");
    await page.fill("#token", "not-the-token");
    await page.click('#signin button[type="submit"]');
    await page.waitForFunction(() => document.querySelector("#signin-err")?.textContent.includes("not a token this server accepts"));
    check(true, "a wrong token is refused by the server, and the page says so");

    await page.fill("#token", READ);
    await page.click('#signin button[type="submit"]');
    await settled(page, "overview");
    check((await page.textContent("#page h1")) === "analytics", "the read-only token opens the first state, by name");
    check(!(await page.isVisible("#access")), "a read-only sign-in carries no warning stamp");
    const log = await page.textContent(".log");
    check(/State version\s*5/.test(log), "the overview reads version 5");
    check((await page.textContent("#connect-term")).includes(`${base}/v1/analytics`), "the connect commands carry this state's own address");
    check((await page.textContent("#connect-term")).includes("--expect-version 5"), "…and its version");

    for (const route of ["overview", "history", "snapshot", "states"]) {
      await page.evaluate((r) => { location.hash = `#/${r}`; }, route);
      await settled(page, route);
      const h1 = await page.evaluate(() => ({ text: document.querySelector("#page h1")?.textContent, focused: document.activeElement === document.querySelector("#page h1") }));
      check(!!h1.text && (route === "overview" || h1.focused), `${route}: heading "${h1.text}"${route === "overview" ? "" : " takes focus"}`);
      const bad = await unnamed(page);
      check(bad.length === 0, `${route}: every control has a name${bad.length ? ` — ${bad.join(" | ")}` : ""}`);
      if (shots) await page.screenshot({ path: join(shots, `server-${scheme}-${route}.png`), fullPage: true });
    }

    if (scheme === "light") {
      // History: the kept commits, the retention note, a version's snapshot.
      await page.evaluate(() => { location.hash = "#/history"; });
      await settled(page, "history");
      const versions = await page.$$eval(".pour__row", (rows) => rows.map((r) => r.dataset.version));
      check(versions.join(",") === "5,4,3", `history lists what the server keeps, newest first (${versions})`);
      check((await page.textContent("#page")).includes("Commits before version 3 are no longer kept"), "and says the older ones were dropped, and why");
      await page.click('button[data-act="open"][data-v="4"]');
      await page.waitForSelector("dialog.dlg[open] #snap-table");
      const row = await page.textContent('#snap-table tr[data-name="stg_orders"] td');
      check(row === BIG_HEX, `a u64 past 2^53 shows as its exact hex (${row})`);
      check(await page.$eval('#snap-table tr[data-name="fct_orders"]', (tr) => tr.classList.contains("hl")), "the models a commit moved are washed");
      await page.fill("#snap-filter", "dim");
      check(await page.$$eval("#snap-table tbody tr:not([hidden])", (r) => r.length) === 1, "the filter narrows to the model typed");
      const [download] = await Promise.all([page.waitForEvent("download"), page.click('dialog.dlg[open] button[value="download"]')]);
      const text = await readFile(await download.path(), "utf8");
      check(download.suggestedFilename() === "analytics-state-v4.json", `the download is named for its version (${download.suggestedFilename()})`);
      check(text.includes(`"stg_orders": ${BIG}`), "the downloaded state.json holds the u64 digit for digit");
      check(JSON.parse(text).watermarks.fct_orders === ledger("2026-10-27"), "and the ledger string as stored");

      // Snapshot: `/` reaches the filter.
      await page.evaluate(() => { location.hash = "#/snapshot"; });
      await settled(page, "snapshot");
      await page.keyboard.press("/");
      check(await page.evaluate(() => document.activeElement?.id === "model-filter"), "/ moves to the snapshot's filter");
      await page.keyboard.type("orders");
      check(await page.$$eval("#model-table tbody tr:not([hidden])", (r) => r.length) === 2, "and typing narrows the models");

      // Every state, and switching.
      await page.keyboard.press("Escape");
      await page.evaluate(() => document.activeElement?.blur());
      await page.keyboard.press("g");
      await page.keyboard.press("s");
      await settled(page, "states");
      check(await page.$$eval(".ledger__row", (r) => r.length) === 2, "g then s lists both states");
      await page.click('button[data-act="open"][data-name="marketing"]');
      await settled(page, "overview");
      check((await page.textContent("#page h1")) === "marketing", "opening a state shows it");
      check(await page.$eval("#state-select", (s) => s.value) === "marketing", "and the picker follows");

      // The palette.
      await page.keyboard.press("Control+k");
      await page.waitForSelector("dialog.palette[open]");
      await page.keyboard.type("history");
      await page.keyboard.press("Enter");
      await settled(page, "history");
      check(true, "Ctrl+K → “history” → Enter opens History");

      // Theme and focus mode.
      await page.click("#theme-toggle");
      check(await page.evaluate(() => localStorage.getItem("freshet.theme")) === "light", "the theme toggle remembers a choice");
      await page.keyboard.press(".");
      check(await page.evaluate(() => document.documentElement.dataset.focus) === "on", ". turns focus mode on");
      await page.keyboard.press(".");

      // Who am I, then out.
      await page.click("#account");
      await page.waitForSelector("dialog.dlg[open]");
      check((await page.textContent("dialog.dlg[open]")).includes("The read-only token"), "the server dialog says which token this is");
      await page.click('dialog.dlg[open] button[value="signout"]');
      await page.waitForSelector("#signin");
      check(await page.evaluate(() => !sessionStorage.getItem("freshet.state.token") && !localStorage.getItem("freshet.state.token")), "signing out forgets the token");

      // The commit token works, and the page says a browser should hold the other one.
      await page.fill("#token", COMMIT);
      await page.click('#signin button[type="submit"]');
      await settled(page, "history");
      check(true, "signing in again returns to the page the link names");
      await page.evaluate(() => { location.hash = "#/overview"; });
      await settled(page, "overview");
      check((await page.textContent("#page")).includes("You signed in with the token that commits"), "a commit-token sign-in is told the read-only one is enough");
      check(await page.isVisible("#access"), "and carries the stamp that says so");

      // On a phone.
      await page.setViewportSize({ width: 390, height: 844 });
      for (const route of ["overview", "history", "snapshot", "states"]) {
        await page.evaluate((r) => { location.hash = `#/${r}`; }, route);
        await page.waitForFunction(() => !document.querySelector("#page[aria-busy]"));
        await page.waitForTimeout(80);
        const wide = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 1);
        check(!wide, `${route}: no sideways scroll at 390px`);
      }
    }

    check(problems.length === 0, `no console errors, exceptions or CSP violations${problems.length ? `:\n      ${problems.join("\n      ")}` : ""}`);
    await context.close();
  }
}

async function demo(browser, base, shots) {
  console.log("\ndemo");
  const { context, page, problems } = await watched(browser, "light");
  await page.goto(`${base}/console/?demo#/states`);
  await settled(page, "states");
  check(await page.$$eval(".ledger__row", (r) => r.length) === 3, "the demo keeps three states");
  check((await page.textContent("#access")).includes("Demo"), "and says it is the demo");
  await page.click('button[data-act="open"][data-name="analytics"]');
  await settled(page, "overview");
  await page.evaluate(() => { location.hash = "#/history"; });
  await settled(page, "history");
  check(await page.$$eval(".pour__row", (r) => r.length) === 25, "history pages 25 at a time");
  await page.click('button[data-act="older"]');
  await page.waitForFunction(() => document.querySelectorAll(".pour__row").length > 25);
  check(await page.evaluate(() => document.activeElement?.classList.contains("pour__row")), "loading older commits lands on the first new one");
  check((await page.textContent("#page")).includes("every commit, back to the first"), "and the end says it is the end");
  await page.click('button[data-act="open"][data-v="1"]');
  await page.waitForSelector("dialog.dlg[open] #snap-table");
  const fps = await page.$$eval("#snap-table td:nth-child(2)", (tds) => tds.map((t) => t.textContent));
  check(fps.length > 20 && fps.every((f) => /^[0-9a-f]{16}$/.test(f)), "every demo fingerprint is 16-digit hex");
  await page.keyboard.press("Escape");
  if (shots) await page.screenshot({ path: join(shots, "demo-history.png"), fullPage: true });
  check(problems.length === 0, `no console errors, exceptions or CSP violations${problems.length ? `:\n      ${problems.join("\n      ")}` : ""}`);
  await context.close();
}

async function main() {
  const { chromium } = loadPlaywright();
  const dir = await mkdtemp(join(tmpdir(), "freshet-console-e2e-"));
  const { child, base } = await startServer(dir);
  const shots = process.env.SHOTS;
  if (shots) await mkdir(shots, { recursive: true });
  const browser = await chromium.launch({ executablePath: chromiumPath() });
  try {
    await seed(base);
    await onServer(browser, base, shots);
    await demo(browser, base, shots);
  } finally {
    await browser.close();
    child.kill("SIGTERM");
    await rm(dir, { recursive: true, force: true });
  }
  console.log(failures ? `\n${failures} check(s) failed` : "\nevery check passed");
  process.exit(failures ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
