// History: every commit to the state, newest first, drawn as one glaze line running
// down the ledger, a page at a time. Any version still kept opens to its snapshot.

import { html, mount, term, esc } from "../core/ui.js";
import { pageHead, section, empty } from "../core/page.js";
import { pourRows, legend, openSnapshot } from "../core/state.js";
import { noState } from "./common.js";

const PAGE = 25;

export default {
  title: "History",
  async render(ctx, el) {
    if (!ctx.state) { mount(el, html`${pageHead({ title: "History" })}${noState(ctx)}`); return; }
    const name = ctx.state;
    const first = await ctx.attempt(() => ctx.api.history(name, { limit: PAGE }));
    if (ctx.stale()) return;
    const commits = [...first.commits];
    let nextBefore = first.next_before;

    const tail = () => {
      if (nextBefore != null) return html`<div class="actions"><button class="btn btn--ghost btn--sm" type="button" data-act="older">Load older commits</button></div>`;
      const oldest = commits[commits.length - 1];
      return oldest && oldest.version > 1
        ? html`<p class="section__note">Commits before version ${oldest.version} are no longer kept: the server keeps the newest ones (<code>--keep</code>).</p>`
        : html`<p class="section__note">That is every commit, back to the first.</p>`;
    };
    const body = () => commits.length
      ? html`<ol class="pour" id="pour" aria-label="Commits, newest first">${pourRows(commits)}</ol>${legend}${tail()}`
      : html`${empty({ title: "Nothing committed yet", text: "The first commit creates version 1." })}
        ${term("the first commit", [{ t: `freshet plan --project models --state ${ctx.stateUrl(name)} --commit` }])}`;

    const paint = () => mount(el, html`
      ${pageHead({
        title: "History",
        lede: html`Every commit to <strong>${name}</strong>, newest first, with what each one moved. Plans read the latest; CI adds one with compare-and-swap.`,
      })}
      ${section({ id: "commits-h", title: "Commits", body: body() })}`);
    paint();

    ctx.bind(el, {
      open: async (b) => {
        const c = await ctx.attempt(() => ctx.api.version(name, b.dataset.v));
        await openSnapshot(c, { stem: name });
      },
      older: async (b) => {
        b.disabled = true;
        b.textContent = "Loading…";
        const page = await ctx.attempt(() => ctx.api.history(name, { limit: PAGE, before: nextBefore }));
        if (ctx.stale()) return;
        const firstNew = page.commits[0]?.version;
        commits.push(...page.commits);
        nextBefore = page.next_before;
        paint();
        // Land on the first row that was just added, not back at the top.
        el.querySelector(`.pour__row[data-version="${esc(String(firstNew))}"]`)?.focus();
      },
    });
  },
};
