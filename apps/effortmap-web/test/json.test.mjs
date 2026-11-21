/* `json.js`: JSON.parse and JSON.stringify, except that no integer is ever rounded.
 *
 * Pinned two ways. Where a document holds no integer past 2^53 the exact functions must
 * be indistinguishable from the built-ins: the same values, the same bytes, the same
 * refusals. Where it does, the integer must come back as the exact BigInt, and go out
 * again as the same digits.
 *
 * `node --test test/*.test.mjs` — pure, no wasm needed.
 */

import { test } from "node:test";
import assert from "node:assert/strict";

import { parseExact, stringifyExact } from "../assets/json.js";

test("an integer past 2^53 is an exact BigInt, and one within it is a number", () => {
  const v = parseExact(
    '{"u64max":18446744073709551615,"i64min":-9223372036854775808,"fp":16899773155021735464,' +
      '"safe":9007199254740991,"first":9007199254740992,"minus":-9007199254740992,"small":3}',
  );
  assert.equal(v.u64max, 18446744073709551615n);
  assert.equal(v.i64min, -9223372036854775808n);
  assert.equal(v.fp, 16899773155021735464n, "JSON.parse would have said 16899773155021736000");
  assert.equal(v.safe, Number.MAX_SAFE_INTEGER);
  assert.equal(v.first, 9007199254740992n, "the first unsafe integer, which a double does hold, is still a BigInt");
  assert.equal(v.minus, -9007199254740992n);
  assert.equal(v.small, 3);
});

test("a fraction or an exponent stays a number however large", () => {
  // These are f64s on the Rust side; a double loses nothing a float had.
  const v = parseExact("[1e300,12345678901234567890.5,-0,2.5E-3,1E20]");
  assert.deepEqual(v, [1e300, 12345678901234567890.5, -0, 2.5e-3, 1e20]);
  assert.ok(Object.is(v[2], -0));
});

test("arithmetic on a large integer throws rather than rounding", () => {
  const { n } = parseExact('{"n":18446744073709551615}');
  assert.throws(() => n + 1, TypeError);
  assert.throws(() => n / 1000, TypeError);
});

const ORDINARY = [
  "{}",
  "[]",
  " \t\r\n{ \"a\" : [ 1 , 2.5 , -3e2 , true , false , null ] }\n",
  '{"s":"plain","e":"q\\" b\\\\ s\\/ \\b\\f\\n\\r\\t \\u00e9 \\ud83d\\ude00","u":"é😀"}',
  '{"nested":{"deep":{"deeper":[[[]],{}]}},"after":1}',
  '{"dup":1,"other":2,"dup":3}',
  '{"__proto__":{"polluted":true},"x":1}',
  '"a bare string"',
  "12",
  "null",
  '{"lone":"\\ud800"}',
  '{"sql":"select \\"}{\\" from t where a = \'[\'"}',
];

test("a document without a large integer parses as JSON.parse parses it", () => {
  for (const text of ORDINARY) {
    assert.deepEqual(parseExact(text), JSON.parse(text), text);
  }
});

test("a key called __proto__ is a field, not the object's prototype", () => {
  const v = parseExact('{"__proto__":{"polluted":true}}');
  assert.equal(Object.getPrototypeOf(v), Object.prototype);
  assert.ok(Object.hasOwn(v, "__proto__"));
  assert.equal({}.polluted, undefined);
});

test("what JSON.parse refuses, parseExact refuses", () => {
  const bad = [
    "", " ", "{", "}", "[1,]", '{"a":1,}', "01", "1.", ".5", "+1", "-", "1e", "tru", "nul",
    '"\\x"', '"a\u0001b"', '"open', "1 2", "{'a':1}", "NaN", "[1 2]", '{"a" 1}', '{"a":1 "b":2}',
    "[", "[1", '{"a"', '{"a":', "\"\\", "{1:2}", "[,]", "1}", "true false",
  ];
  for (const text of bad) {
    assert.throws(() => JSON.parse(text), SyntaxError, `JSON.parse accepted ${JSON.stringify(text)}`);
    assert.throws(() => parseExact(text), SyntaxError, `parseExact accepted ${JSON.stringify(text)}`);
  }
});

test("without a BigInt, stringifyExact writes JSON.stringify's bytes", () => {
  const values = [
    ...ORDINARY.map((t) => JSON.parse(t)),
    { a: undefined, b: () => 1, c: [undefined, () => 1, NaN, Infinity], d: "x" },
    { models: [{ name: "m", sql: "select 1" }], sources: { raw: [] }, strict_refs: true },
    [],
    {},
  ];
  for (const v of values) {
    for (const indent of [undefined, 2, "\t", 0]) {
      assert.equal(stringifyExact(v, indent), JSON.stringify(v, null, indent), `${JSON.stringify(v)} at ${indent}`);
    }
  }
});

test("a BigInt goes out as the digits it came in as", () => {
  const text = '{"fingerprints":{"a":16899773155021735464,"b":3},"n":[-9223372036854775808]}';
  assert.equal(stringifyExact(parseExact(text)), text);
  assert.equal(
    stringifyExact({ fp: 18446744073709551615n }, 2),
    '{\n  "fp": 18446744073709551615\n}',
  );
});
