// The state API, as the console calls it: freshet-state-server's `/v1`. One function per
// route; every one resolves to the parsed body or throws an ApiError whose message is
// the server's own words (it answers `{"error": "…"}`).
//
// Bodies are parsed with core/json.js, not JSON.parse: a fingerprint is a full-range
// u64 and JSON.parse would round it. Each snapshot then leaves this module with its
// fingerprints as the 16-digit hex `freshet fingerprint` prints, the form the shared
// views take and the one no later step can round.

import { parseExact } from "./core/json.js";

export class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

/// A fingerprint as 16-digit lowercase hex. It arrives exact: a Number below 2^53, a
/// BigInt above.
export const hex = (fp) => BigInt(fp).toString(16).padStart(16, "0");

/// `snapshot` with every fingerprint as hex.
export function hexSnapshot(snapshot) {
  const fingerprints = {};
  for (const [model, fp] of Object.entries(snapshot?.fingerprints || {})) fingerprints[model] = hex(fp);
  return { fingerprints, watermarks: snapshot?.watermarks || {} };
}

const qs = (params) => {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(params || {})) {
    if (v !== undefined && v !== null && v !== "") p.set(k, String(v));
  }
  const s = p.toString();
  return s ? `?${s}` : "";
};

const enc = encodeURIComponent;

/// `base` is the server's origin and path prefix ("" for same-origin); `token()`
/// returns the bearer token, or null. `fetch` is the transport: the browser's, or the
/// demo's in-memory server (demo.js), so both go through the same parsing.
export function createApi({ base = "", token = () => null, fetch: send = globalThis.fetch.bind(globalThis) } = {}) {
  async function get(path) {
    const headers = { accept: "application/json" };
    const t = token();
    if (t) headers.authorization = `Bearer ${t}`;
    let resp;
    try {
      resp = await send(`${base}${path}`, { method: "GET", headers, credentials: "omit" });
    } catch {
      throw new ApiError(0, "Can't reach the state server. Check that it is running and that this page came from it, then try again.");
    }
    const text = await resp.text();
    let data = null;
    if (text) {
      try { data = parseExact(text); } catch { data = null; }
    }
    if (!resp.ok) {
      const msg = (data && data.error) || text.trim() || `${resp.status} ${resp.statusText}`;
      throw new ApiError(resp.status, msg);
    }
    return data ?? {};
  }

  return {
    /// The same API with `t` as its token, to try one before keeping it.
    withToken: (t) => createApi({ base, token: () => t, fetch: send }),
    /// `{states: [{name, version, committed_at, models}], access}`. `access` is
    /// `open` (the server needs no token), `read` or `commit`.
    states: () => get("/v1/"),
    /// `{version, snapshot}`; `version` is null before a state's first commit.
    async current(name) {
      const r = await get(`/v1/${enc(name)}/current`);
      return { version: r.version ?? null, snapshot: hexSnapshot(r.snapshot) };
    },
    /// `{commits, limit, next_before}`, newest first.
    history: (name, { limit, before } = {}) => get(`/v1/${enc(name)}/history${qs({ limit, before })}`),
    /// `{version, committed_at, summary, snapshot}` while the server keeps it.
    async version(name, v) {
      const r = await get(`/v1/${enc(name)}/versions/${enc(v)}`);
      return { ...r, snapshot: hexSnapshot(r.snapshot) };
    },
  };
}
