// Overview: one state at a glance — where it stands, what the latest commits moved,
// and how each kind of person reaches it. Everything links on to the page that holds
// the full story.

import { html, mount, $, when, fmtInt, term, announce } from "../core/ui.js";
import { pageHead, section, empty } from "../core/page.js";
import { pourRows, legend } from "../core/state.js";
import { noState } from "./common.js";

export const PERSONAS = [
  { id: "engineer", label: "Data engineer · CI" },
  { id: "analyst", label: "Analyst · dbt project" },
  { id: "scientist", label: "Scientist · Python" },
];

/// `{title, lines, caption}` for a transcript, with this state's address in it.
export function snippet(persona, { url, version }) {
  const tok = "export FRESHET_STATE_TOKEN=…   # this server's token";
  if (persona === "analyst") {
    return {
      title: "what your branch would rebuild",
      lines: [
        { t: tok },
        { t: `freshet plan --project models --state ${url}` },
        { c: "# against the team's committed state, not your laptop's" },
        { t: "freshet explain --against target/" },
        { c: "# a dbt project: which models dbt would pick, and why" },
      ],
      caption: "Planning reads state and runs nothing. Your warehouse is never queried.",
    };
  }
  if (persona === "scientist") {
    return {
      title: "this state in a notebook",
      lines: [
        { k: "import json, os, urllib.request" },
        { k: `req = urllib.request.Request("${url}/current",` },
        { k: '    headers={"authorization": "Bearer " + os.environ["FRESHET_STATE_TOKEN"]})' },
        { k: "state = json.load(urllib.request.urlopen(req))" },
        { k: 'print(state["version"], len(state["snapshot"]["fingerprints"]), "models")' },
      ],
      caption: "Python's integers are exact, so fingerprints survive json.load. In JavaScript they would not: this page parses them as BigInt.",
    };
  }
  return {
    title: "plan → run → commit in CI",
    lines: [
      { t: tok },
      { t: `freshet plan --project models --state ${url} --json > plan.json` },
      { c: "# … run plan.json with your orchestrator …" },
      { t: `freshet plan --project models --state ${url} --commit --expect-version ${version ?? "none"}` },
    ],
    caption: "If another job committed first, nothing is written and freshet exits 3: plan again.",
  };
}

const PERSONA_KEY = "freshet.persona";
const persona = () => { try { return localStorage.getItem(PERSONA_KEY) || "engineer"; } catch { return "engineer"; } };

function connect(ctx, head, which) {
  const s = snippet(which, { url: ctx.stateUrl(ctx.state), version: head.version });
  return html`<div class="toggles" role="group" aria-label="Show the commands for">
      ${PERSONAS.map((p) => html`<button type="button" data-act="persona" data-p="${p.id}" aria-pressed="${p.id === which}">${p.label}</button>`)}
    </div>
    <div class="connect__term" id="connect-term">${term(s.title, s.lines, s.caption)}</div>`;
}

export default {
  title: "Overview",
  async render(ctx, el) {
    if (!ctx.state) { mount(el, html`${pageHead({ title: "Overview" })}${noState(ctx)}`); return; }
    const name = ctx.state;
    const [head, hist] = await Promise.all([
      ctx.attempt(() => ctx.api.current(name)),
      ctx.attempt(() => ctx.api.history(name, { limit: 50 })),
    ]);
    if (ctx.stale()) return;
    const commits = hist.commits || [];
    const week = commits.filter((c) => Date.now() - Date.parse(c.committed_at) < 7 * 86400000).length;
    const models = Object.keys(head.snapshot.fingerprints).length;
    const ledgers = Object.keys(head.snapshot.watermarks).length;

    mount(el, html`
      ${pageHead({
        title: name,
        lede: html`One project's state on this server: what plans read, and every commit that moved it.`,
        actions: html`<a class="btn" href="#/history">Every commit</a><a class="btn btn--ghost" href="#/snapshot">The snapshot</a>`,
      })}
      ${ctx.access === "commit" ? html`<div class="note" role="status"><p><strong>You signed in with the token that commits.</strong>
        This page only reads, and the read-only token (<code>FRESHET_STATE_SERVER_READ_TOKEN</code>) reads everything here. A browser is a better home for that one.</p></div>` : ""}
      <div class="log" aria-label="This state">
        <p class="log__title">${name} · <span class="mono">${ctx.stateUrl(name)}</span></p>
        <dl class="log__grid">
          <div><dt>State version</dt><dd>${head.version ?? "none"}</dd></div>
          <div><dt>Commits this week</dt><dd>${commits.length >= 50 && week >= 50 ? "50+" : week}</dd></div>
          <div><dt>Models tracked</dt><dd>${fmtInt(models)}</dd></div>
          <div><dt>Partition ledgers</dt><dd>${fmtInt(ledgers)}</dd></div>
        </dl>
        <p class="log__note">${commits[0] ? html`Last commit ${when(commits[0].committed_at)}. ` : "Nothing committed yet. "}Plans read this state; CI commits it with compare-and-swap, so two jobs never overwrite each other.</p>
      </div>
      <div class="cols">
        ${section({
          id: "recent-h",
          title: "Latest commits",
          actions: commits.length ? html`<a class="btn btn--quiet btn--sm" href="#/history">Every commit</a>` : "",
          body: commits.length
            ? html`<ol class="pour" aria-label="The latest commits, newest first">${pourRows(commits.slice(0, 5), { open: false })}</ol>${legend}`
            : empty({ title: "Nothing committed yet", text: "The first plan with --commit makes version 1, and the pour starts here." }),
        })}
        ${section({
          id: "connect-h",
          title: "Connect",
          note: "The same state, from wherever you work. The address and version are this state's.",
          body: html`<div id="connect">${connect(ctx, head, persona())}</div>`,
        })}
      </div>`);

    ctx.bind(el, {
      persona: (b) => {
        const which = b.dataset.p;
        try { localStorage.setItem(PERSONA_KEY, which); } catch { /* per-visit */ }
        mount($("#connect", el), connect(ctx, head, which));
        $(`#connect button[data-p="${which}"]`, el)?.focus();
        announce(`Showing the commands for ${PERSONAS.find((p) => p.id === which).label}`);
      },
    });
  },
};
