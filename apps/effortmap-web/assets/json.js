/* JSON that keeps every integer exact.
 *
 * `JSON.parse` turns every number into an IEEE-754 double, which holds an integer
 * exactly only up to 2^53 (`Number.MAX_SAFE_INTEGER`). The planner's wire carries u64s
 * — fingerprints today, and whatever u64 or i64 the contract adds next — and a double
 * rounds those silently:
 *
 *     wasm emits    16899773155021735464     (exact)
 *     JSON.parse -> 16899773155021736000     (rounded, and no error)
 *
 * [`parseExact`] is `JSON.parse` with one difference: an integer literal outside the
 * safe range becomes a `BigInt` holding its exact value. Strings, fractions, exponents
 * and safe integers come back exactly as `JSON.parse` returns them. A large integer is
 * therefore never wrong, only of another type, and code that forgets and does
 * arithmetic with one throws ("Cannot mix BigInt and other types") rather than
 * rounding. That is what makes the fix structural instead of per-field: the next u64
 * added to the wire arrives exact, with nothing to remember.
 *
 * [`stringifyExact`] is the way back: `JSON.stringify`, except that a BigInt is written
 * as its digits (`JSON.stringify` throws on one). On plain data without a BigInt — what
 * a request is built from, and what `parseExact` returns — the output is byte-identical
 * to `JSON.stringify`'s, and a test holds it to that.
 *
 * The contract is unchanged: integers stay integers on the wire, so every `state.json`
 * written before this reads as it always did. Serializing u64s as strings would have
 * fixed the browser by breaking every file and every other binding.
 */

const NUMBER = /-?(?:0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?/y;

/** `JSON.parse(text)`, except that an integer beyond 2^53 - 1 in magnitude is a BigInt. */
export function parseExact(text) {
  let i = 0;

  const fail = (what) => {
    throw new SyntaxError(`${what} at position ${i} of the JSON`);
  };

  const space = () => {
    for (; i < text.length; i++) {
      const c = text.charCodeAt(i);
      if (c !== 0x20 && c !== 0x0a && c !== 0x0d && c !== 0x09) return;
    }
  };

  const value = () => {
    space();
    switch (text[i]) {
      case "{": return object();
      case "[": return array();
      case '"': return string();
      case "t": return word("true", true);
      case "f": return word("false", false);
      case "n": return word("null", null);
      default: return number();
    }
  };

  const object = () => {
    const out = {};
    i++;
    space();
    if (text[i] === "}") {
      i++;
      return out;
    }
    for (;;) {
      space();
      if (text[i] !== '"') fail("expected a key");
      const key = string();
      space();
      if (text[i] !== ":") fail("expected ':'");
      i++;
      const v = value();
      // A model can be called `__proto__`. Assigning that key would set the object's
      // prototype instead of adding a field, so it is defined, as JSON.parse does.
      if (key === "__proto__") {
        Object.defineProperty(out, key, { value: v, writable: true, enumerable: true, configurable: true });
      } else {
        out[key] = v;
      }
      space();
      if (text[i] === "}") {
        i++;
        return out;
      }
      if (text[i] !== ",") fail("expected ',' or '}'");
      i++;
    }
  };

  const array = () => {
    const out = [];
    i++;
    space();
    if (text[i] === "]") {
      i++;
      return out;
    }
    for (;;) {
      out.push(value());
      space();
      if (text[i] === "]") {
        i++;
        return out;
      }
      if (text[i] !== ",") fail("expected ',' or ']'");
      i++;
    }
  };

  // Found by scanning, decoded by JSON.parse: escapes and the control characters a
  // string may not hold are JSON.parse's to judge, so they are judged identically.
  const string = () => {
    const start = i++;
    let plain = true;
    for (;;) {
      if (i >= text.length) fail("unterminated string");
      const c = text.charCodeAt(i);
      if (c === 0x22) break;
      if (c === 0x5c) {
        plain = false;
        i += 2;
      } else {
        if (c < 0x20) plain = false;
        i++;
      }
    }
    i++;
    return plain ? text.slice(start + 1, i - 1) : JSON.parse(text.slice(start, i));
  };

  const word = (literal, v) => {
    if (!text.startsWith(literal, i)) fail("unexpected character");
    i += literal.length;
    return v;
  };

  const number = () => {
    NUMBER.lastIndex = i;
    const m = NUMBER.exec(text);
    if (!m) fail(i < text.length ? "unexpected character" : "unexpected end");
    i = NUMBER.lastIndex;
    const n = Number(m[0]);
    // An integer literal is an integer on the Rust side too. A fraction or an exponent
    // is an f64 there, which a double holds exactly, so only integers can be lost.
    if (m[1] === undefined && m[2] === undefined && !Number.isSafeInteger(n)) return BigInt(m[0]);
    return n;
  };

  const out = value();
  space();
  if (i !== text.length) fail("unexpected text after the JSON");
  return out;
}

/** `JSON.stringify(value, null, indent)` for plain data, writing a BigInt as its digits. */
export function stringifyExact(value, indent = "") {
  const step = typeof indent === "number" ? " ".repeat(Math.max(0, Math.min(10, indent))) : String(indent).slice(0, 10);

  const write = (v, inner) => {
    if (typeof v === "bigint") return v.toString();
    if (v === null || typeof v !== "object") return JSON.stringify(v); // undefined for undefined and functions
    const next = inner + step;
    const open = step ? `\n${next}` : "";
    const close = step ? `\n${inner}` : "";
    const sep = step ? `,\n${next}` : ",";

    if (Array.isArray(v)) {
      if (!v.length) return "[]";
      return `[${open}${v.map((x) => write(x, next) ?? "null").join(sep)}${close}]`;
    }
    const fields = [];
    for (const key of Object.keys(v)) {
      const s = write(v[key], next);
      if (s !== undefined) fields.push(`${JSON.stringify(key)}:${step ? " " : ""}${s}`);
    }
    if (!fields.length) return "{}";
    return `{${open}${fields.join(sep)}${close}}`;
  };

  return write(value, "");
}
