// What a state page shows before the server keeps any state.

import { html } from "../core/ui.js";
import { empty } from "../core/page.js";

export function noState(ctx) {
  return empty({
    title: "No state yet",
    text: `${ctx.server} keeps no state yet. A state appears with its first commit, one project to a state.`,
    action: html`<a class="btn btn--sm btn--ghost" href="#/states">Every state</a>`,
  });
}
