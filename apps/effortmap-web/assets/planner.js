/* The planner, in the browser.
 *
 * One export does the work: `plan(request, priorStateRaw)` — the same JSON funnel
 * every other binding calls, running as WebAssembly in the page. There is no server,
 * no HTTP and no Python; `freshet_wasm_bg.wasm` is about 1.1 MB (359 KB gzipped)
 * because the planner *parses* SQL for lineage and never executes it, so no query
 * engine is shipped.
 *
 * ## Why state is handled as text, and never as an object
 *
 * `StateSnapshot.fingerprints` maps a model name to a **u64**. JavaScript numbers are
 * IEEE-754 doubles with 53 bits of integer precision, and a fingerprint routinely
 * exceeds that:
 *
 *     wasm emits    16899773155021735464     (exact)
 *     JSON.parse -> 16899773155021736000     (rounded, and silently)
 *
 * The fingerprints ARE the state: they are what a later plan compares against to
 * decide which models were edited. Parse them into JS numbers and every stored
 * fingerprint differs from the freshly computed one, so every model looks changed and
 * the incremental planner quietly degrades into "rebuild the world, every time" —
 * which looks like conservatism rather than like corruption, and would never be
 * noticed.
 *
 * So the state never becomes a JS number. It is extracted from the response as raw
 * text by [`rawField`] and spliced back into the next request as raw text by
 * [`withRawField`]. This is the same instinct `dagron-state`'s wire.rs records — "the
 * snapshot is carried opaquely as raw JSON; dagron has no business interpreting
 * fingerprints" — except that in JavaScript "opaque" has to mean *text*, because
 * parsing is itself the lossy step.
 *
 * The rest of the response is parsed normally: cost is milliCU in the low thousands
 * and every other field is a small integer or a string.
 */

import initWasm, { plan as wasmPlan } from "./freshet_wasm.js";

let started = null;

/** Load the WebAssembly module. Idempotent; the page awaits it once at startup. */
export function ready(url = new URL("./freshet_wasm_bg.wasm", import.meta.url)) {
  if (!started) started = initWasm({ module_or_path: url });
  return started;
}

/** Run one plan.
 *
 * `request` is a plain object (models, sources, materializations, include_cost,
 * include_queue, and optionally `restate`). `priorStateRaw` is the *text* of a
 * previous response's `next_state`, or null for a cold plan.
 *
 * Returns `{ plan, nextStateRaw, responseText }`: `plan` is parsed and safe to render,
 * `nextStateRaw` is exact text to persist, `responseText` is the untouched output.
 */
export function plan(request, priorStateRaw = null) {
  let requestText = JSON.stringify(request);
  if (priorStateRaw) requestText = withRawField(requestText, "prior_state", priorStateRaw);
  const responseText = wasmPlan(requestText);
  return {
    plan: JSON.parse(responseText),
    nextStateRaw: rawField(responseText, "next_state"),
    responseText,
  };
}

// ── raw JSON field surgery ────────────────────────────────────────────────────
// Small on purpose. These two functions are the entire reason the planner's state
// survives a trip through a browser, so they are scanned rather than regexed and are
// tested against nesting, escapes, and keys that appear inside string values.

/** The raw text of a **top-level** field's value, or null if absent.
 *
 *  Scans rather than parses: the value is returned exactly as the producer wrote it,
 *  digits and all. Only depth-1 keys match, so a `"next_state"` nested inside some
 *  other object is not mistaken for the real one. */
export function rawField(json, key) {
  const span = topLevelSpan(json, key);
  return span ? json.slice(span[0], span[1]) : null;
}

/** Return `json` with `key` set to `rawValue`, inserted as text.
 *
 *  Replaces the field if it is already there, appends it if not. `rawValue` is spliced
 *  verbatim — it must be valid JSON, which it is when it came from [`rawField`]. */
export function withRawField(json, key, rawValue) {
  const span = topLevelSpan(json, key);
  if (span) return json.slice(0, span[0]) + rawValue + json.slice(span[1]);

  const close = json.lastIndexOf("}");
  if (close < 0) throw new Error("not a JSON object");
  const body = json.slice(0, close).trim();
  const sep = body.endsWith("{") ? "" : ",";
  return `${body}${sep}"${key}":${rawValue}}`;
}

/** `[start, end)` of the value for a depth-1 `key`, or null.
 *
 *  One pass, tracking string state and nesting depth so that neither a brace inside a
 *  string nor a same-named key one level down can fool it. */
function topLevelSpan(json, key) {
  const needle = `"${key}"`;
  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let i = 0; i < json.length; i++) {
    const c = json[i];

    if (inString) {
      if (escaped) escaped = false;
      else if (c === "\\") escaped = true;
      else if (c === '"') inString = false;
      continue;
    }

    if (c === '"') {
      // A key at depth 1 is the only thing that can match. Confirm it is a key by
      // looking for the colon that must follow the closing quote.
      if (depth === 1 && json.startsWith(needle, i)) {
        const afterKey = i + needle.length;
        const colon = skipSpace(json, afterKey);
        if (json[colon] === ":") {
          const start = skipSpace(json, colon + 1);
          return [start, valueEnd(json, start)];
        }
      }
      inString = true;
      continue;
    }

    if (c === "{" || c === "[") depth++;
    else if (c === "}" || c === "]") depth--;
  }
  return null;
}

/** The index just past the JSON value beginning at `start`. */
function valueEnd(json, start) {
  const first = json[start];

  if (first === '"') {
    let escaped = false;
    for (let i = start + 1; i < json.length; i++) {
      const c = json[i];
      if (escaped) escaped = false;
      else if (c === "\\") escaped = true;
      else if (c === '"') return i + 1;
    }
    throw new Error("unterminated string");
  }

  if (first === "{" || first === "[") {
    let depth = 0;
    let inString = false;
    let escaped = false;
    for (let i = start; i < json.length; i++) {
      const c = json[i];
      if (inString) {
        if (escaped) escaped = false;
        else if (c === "\\") escaped = true;
        else if (c === '"') inString = false;
        continue;
      }
      if (c === '"') inString = true;
      else if (c === "{" || c === "[") depth++;
      else if (c === "}" || c === "]") {
        depth--;
        if (depth === 0) return i + 1;
      }
    }
    throw new Error("unterminated object or array");
  }

  // A number, or one of true / false / null: runs until a structural character.
  let i = start;
  while (i < json.length && !",}]".includes(json[i]) && !/\s/.test(json[i])) i++;
  return i;
}

const skipSpace = (json, i) => {
  while (i < json.length && /\s/.test(json[i])) i++;
  return i;
};
