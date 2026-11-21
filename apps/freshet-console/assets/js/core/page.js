// Pieces every page is built from: the page head, a section and the empty state.

import { html } from "./ui.js";

export function pageHead({ title, lede, actions }) {
  return html`<div class="page__head">
    <div>
      <h1 tabindex="-1">${title}</h1>
      ${lede ? html`<p class="lede">${lede}</p>` : ""}
    </div>
    ${actions ? html`<div class="actions">${actions}</div>` : ""}
  </div>`;
}

export function section({ id, title, note, actions, body }) {
  return html`<section class="section" ${id ? html`aria-labelledby="${id}"` : ""}>
    <div class="section__head">
      <div>
        <h2 ${id ? html`id="${id}"` : ""}>${title}</h2>
        ${note ? html`<p class="section__note">${note}</p>` : ""}
      </div>
      ${actions ? html`<div class="actions">${actions}</div>` : ""}
    </div>
    ${body}
  </section>`;
}

/// An empty list is an invitation: what would be here, and the one thing to do.
/// `art` is an image beside the words, for a console that ships one.
export function empty({ art, title, text, action }) {
  return html`<div class="empty${art ? "" : " empty--plain"}">
    ${art ? html`<img src="${art}" alt="" width="360" height="446" loading="lazy">` : ""}
    <div><h3>${title}</h3><p>${text}</p>${action || ""}</div>
  </div>`;
}
