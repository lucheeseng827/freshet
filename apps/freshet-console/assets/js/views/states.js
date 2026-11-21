// Every state: the server's whole shelf, one row a state, with its version, its last
// commit and the models it tracks. Opening one makes it the state the other pages show.

import { html, mount, when, fmtInt, term } from "../core/ui.js";
import { pageHead, section, empty } from "../core/page.js";

export default {
  title: "Every state",
  async render(ctx, el) {
    const listing = await ctx.attempt(() => ctx.api.states());
    if (ctx.stale()) return;
    ctx.setStates(listing);
    const states = listing.states || [];
    const first = term("create a state with its first commit", [
      { t: `freshet plan --project models --state ${ctx.stateUrl("analytics")} --commit` },
      { c: "# the first commit makes version 1" },
    ], "A state is one project's. Its first commit creates it; a name is letters, digits, ., _ and -.");

    mount(el, html`
      ${pageHead({
        title: "Every state",
        lede: html`The states <span class="mono">${ctx.server}</span> keeps, one a project. Open one to see its commits and its snapshot.`,
      })}
      ${section({
        id: "states-h",
        title: `${states.length} state${states.length === 1 ? "" : "s"}`,
        body: states.length
          ? html`<ul class="ledger" aria-label="States, by name">${states.map((s) => html`<li class="ledger__row">
              <p class="ledger__name">${s.name}<span class="ledger__sub mono">${ctx.stateUrl(s.name)}</span></p>
              <div class="ledger__body">
                <p class="ledger__meta"><span>version <b>${s.version}</b></span><span>last commit <b>${when(s.committed_at)}</b></span><span><b>${fmtInt(s.models)}</b> model${s.models === 1 ? "" : "s"}</span></p>
              </div>
              <div class="ledger__act"><button class="btn btn--quiet btn--sm" type="button" data-act="open" data-name="${s.name}" aria-label="Open ${s.name}">Open</button></div>
            </li>`)}</ul>`
          : html`${empty({ title: "No state yet", text: "A state appears here with its first commit. Point a plan at this server and commit." })}${first}`,
      })}`);

    ctx.bind(el, {
      open: (b) => { ctx.setState(b.dataset.name); ctx.go("overview"); },
    });
  },
};
