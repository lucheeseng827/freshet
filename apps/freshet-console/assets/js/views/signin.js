// Sign in: the server takes a bearer token, not accounts, so the page asks for one. The
// read-only token is the right one for a browser; the token that commits works too,
// and the console still never commits.

import { html, mount, $ } from "../core/ui.js";

export default {
  title: "Sign in",
  render({ api, message, server, onSignedIn }, el) {
    mount(el, html`<div class="gate">
      <div class="gate__card">
        <h1 tabindex="-1">Sign in to this state server</h1>
        <p class="lede">Every state <span class="mono">${server}</span> keeps, every commit to them, and the snapshot each one left.</p>
        ${message ? html`<p class="note" role="status">${message}</p>` : ""}
        <form class="form" id="signin" novalidate>
          <div class="field">
            <label for="token">Token</label>
            <input class="input input--mono" id="token" name="token" type="password" autocomplete="off" spellcheck="false" required aria-describedby="token-hint">
            <p class="field__hint" id="token-hint">The server's <code>FRESHET_STATE_SERVER_READ_TOKEN</code> is enough to read everything here. Its <code>FRESHET_STATE_SERVER_TOKEN</code> works too.</p>
          </div>
          <ul class="choices"><li class="choice"><label>
            <input type="checkbox" name="remember">
            <span class="choice__name">Stay signed in on this device</span>
            <span></span>
            <span class="choice__what">Otherwise the token is forgotten when this tab closes. Leave it off on a shared machine.</span>
          </label></li></ul>
          <p class="form__err" id="signin-err" role="alert" hidden></p>
          <div class="form__foot">
            <button class="btn" type="submit">Sign in</button>
          </div>
        </form>
        <p class="gate__alt">Just looking? <a href="?demo#/states">Open the demo</a>, on sample data, with no server.</p>
      </div>
    </div>`);
    const form = $("#signin", el);
    const err = $("#signin-err", el);
    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      const token = form.token.value.trim();
      if (!token) {
        err.hidden = false;
        err.textContent = "Paste the server's token.";
        form.token.focus();
        return;
      }
      const btn = form.querySelector('button[type="submit"]');
      btn.disabled = true;
      btn.textContent = "Signing in…";
      try {
        // Checked before it is kept: a wrong token is said here, not on the next page.
        const listing = await api.withToken(token).states();
        await onSignedIn(token, form.remember.checked, listing);
      } catch (e2) {
        err.hidden = false;
        err.textContent = e2.status === 401 ? "That is not a token this server accepts." : e2.message;
        btn.disabled = false;
        btn.textContent = "Sign in";
        form.token.select();
        form.token.focus();
      }
    });
    $("h1", el).focus();
  },
};
