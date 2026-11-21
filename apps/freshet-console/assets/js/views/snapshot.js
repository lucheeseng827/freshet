// Snapshot: what plans read right now — every model the state fingerprints, with its
// partition ledger, and the exact state.json to plan against offline.

import { html, mount, $, fmtInt, toast } from "../core/ui.js";
import { pageHead, section, empty } from "../core/page.js";
import { snapshotRows, snapshotTable, wireFilter, stateFileText, download } from "../core/state.js";
import { noState } from "./common.js";

export default {
  title: "Snapshot",
  async render(ctx, el) {
    if (!ctx.state) { mount(el, html`${pageHead({ title: "Snapshot" })}${noState(ctx)}`); return; }
    const name = ctx.state;
    const head = await ctx.attempt(() => ctx.api.current(name));
    if (ctx.stale()) return;
    const rows = snapshotRows(head.snapshot);
    const file = `${name}-state-v${head.version}.json`;

    mount(el, html`
      ${pageHead({
        title: "Snapshot",
        lede: html`What every plan against <strong>${name}</strong> reads now: version ${head.version ?? "none"}, ${fmtInt(rows.length)} models.`,
        actions: rows.length ? html`<button class="btn btn--ghost" type="button" data-act="download">Download as state.json</button>` : "",
      })}
      ${section({
        id: "models-h",
        title: "Models",
        note: "Press / to find a model.",
        body: rows.length
          ? snapshotTable(rows, { filterId: "model-filter", tableId: "model-table" })
          : empty({ title: "Nothing fingerprinted yet", text: "The first commit's snapshot appears here." }),
      })}`);

    const input = $("#model-filter", el);
    if (input) ctx.track(wireFilter(input, $("#model-table", el)));
    ctx.bind(el, {
      download: () => {
        download(file, stateFileText(head.snapshot));
        toast(`Downloaded version ${head.version} as a state file. Plan against it with --state ${file}.`);
      },
    });
  },
};
