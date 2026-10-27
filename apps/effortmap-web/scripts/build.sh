#!/usr/bin/env bash
# Build the planner into the WebAssembly module this page runs.
#
# There is no bundler, no npm and no build step for the *page* — index.html, the CSS
# and the JS are served exactly as they sit in the tree, the same discipline every
# `*-site/` in this repository follows. This script exists for one artifact only: the
# `.wasm` the page loads, which cannot be authored by hand.
#
#   ./scripts/build.sh            debug-ish, fast
#   ./scripts/build.sh --release  what gets deployed
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
module="$(cd "$here/../.." && pwd)"          # the Freshet workspace root
out="$here/assets"

profile="release"
[[ "${1:-}" == "--debug" ]] && profile="debug"

# wasm-bindgen's CLI and the `wasm-bindgen` crate must be the SAME version — a
# mismatch fails at generation time with a version banner, not at run time. Read the
# version the lockfile actually resolved rather than assuming.
want="$(grep -A1 '^name = "wasm-bindgen"$' "$module/Cargo.lock" | grep '^version' | head -1 | cut -d'"' -f2)"
if ! command -v wasm-bindgen >/dev/null 2>&1; then
  echo "wasm-bindgen CLI not found. Install the matching version:" >&2
  echo "  cargo install wasm-bindgen-cli --version $want" >&2
  exit 1
fi
have="$(wasm-bindgen --version | awk '{print $2}')"
if [[ "$have" != "$want" ]]; then
  echo "wasm-bindgen CLI is $have but the crate is $want; they must match:" >&2
  echo "  cargo install wasm-bindgen-cli --version $want --force" >&2
  exit 1
fi

echo "==> cargo build ($profile) for wasm32-unknown-unknown"
if [[ "$profile" == "release" ]]; then
  cargo build --manifest-path "$module/Cargo.toml" -p freshet-wasm \
    --features wasm --target wasm32-unknown-unknown --release
else
  cargo build --manifest-path "$module/Cargo.toml" -p freshet-wasm \
    --features wasm --target wasm32-unknown-unknown
fi

wasm="$module/target/wasm32-unknown-unknown/$profile/freshet_wasm.wasm"

echo "==> wasm-bindgen --target web"
# `--target web` emits an ES module with an `init()` that fetches the .wasm, so the
# page loads it with a plain <script type="module"> and no bundler.
wasm-bindgen --target web --out-dir "$out" "$wasm"

# wasm-opt is optional: it typically takes another 10-20% off, and its absence must
# not break a build on a machine that only has cargo.
if command -v wasm-opt >/dev/null 2>&1; then
  echo "==> wasm-opt -Oz"
  wasm-opt -Oz "$out/freshet_wasm_bg.wasm" -o "$out/freshet_wasm_bg.wasm"
else
  echo "==> wasm-opt not found, skipping (install binaryen for a smaller artifact)"
fi

# `numfmt` is GNU coreutils and is not on a stock macOS — where the README sends people
# with `open index.html`, so it is an expected host. What happens there without this
# helper is worth stating precisely, because the obvious guess is wrong: `set -e` does
# NOT fire, since a command substitution that fails inside a `printf` argument is not a
# failed command. The build exits 0 and prints `raw` and `gzipped` with EMPTY values,
# plus two "command not found" lines on stderr — a successful build that reports nothing
# and looks broken. Degrade to raw bytes instead. The `$(( ))` normalises `wc`'s output
# because BSD `wc` pads its count with leading spaces and GNU `wc` does not.
human() {
  if command -v numfmt >/dev/null 2>&1; then
    numfmt --to=iec-i --suffix=B "$1"
  else
    printf '%s bytes' "$1"
  fi
}

raw=$(( $(wc -c < "$out/freshet_wasm_bg.wasm") ))
gz=$(( $(gzip -9 -c "$out/freshet_wasm_bg.wasm" | wc -c) ))
printf '\n%s\n' "built $out/freshet_wasm_bg.wasm"
printf '  raw     %s\n' "$(human "$raw")"
printf '  gzipped %s   <- what a CDN serves\n' "$(human "$gz")"
