#!/usr/bin/env bash
# Build the C ABI, compile tests/c/harness.c against include/planner.h, and run it:
# the golden request must produce the golden response, and the memory contract must
# hold under the sanitizers.
#
#   crates/freshet-ffi/tests/c/run.sh              AddressSanitizer + LeakSanitizer + UBSan
#   crates/freshet-ffi/tests/c/run.sh --valgrind   the same program under Valgrind
#
# Needs a C compiler (cc; clang or gcc) and python3 for the JSON comparison.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
crate="$(cd "$here/../.." && pwd)"
module="$(cd "$crate/../.." && pwd)"
golden="$module/apps/effortmap-web/test/golden"
mode="${1:-asan}"

cargo build --manifest-path "$module/Cargo.toml" -p freshet-ffi --quiet
target_dir="$(cargo metadata --manifest-path "$module/Cargo.toml" --format-version 1 --no-deps \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
lib_dir="$target_dir/debug"

out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT

cflags=(-std=c11 -Wall -Wextra -Werror -g -I "$crate/include")
case "$mode" in
  --valgrind) run=(valgrind --error-exitcode=99 --leak-check=full --errors-for-leak-kinds=definite,indirect) ;;
  asan)       cflags+=(-fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=all); run=() ;;
  *)          echo "usage: $0 [--valgrind]" >&2; exit 2 ;;
esac

${CC:-cc} "${cflags[@]}" "$here/harness.c" -o "$out/harness" \
  -L "$lib_dir" -lfreshet_ffi -Wl,-rpath,"$lib_dir"

export ASAN_OPTIONS="detect_leaks=1:abort_on_error=1"
"${run[@]}" "$out/harness" "$golden/request.json" > "$out/response.json"

python3 - "$out/response.json" "$golden/response.json" <<'PY'
import json, sys
got, want = (json.load(open(p)) for p in sys.argv[1:3])
if got != want:
    print("the C ABI drifted from the golden fixture:\n" + json.dumps(got, indent=2), file=sys.stderr)
    sys.exit(1)
PY
echo "C ABI: golden response reproduced; memory contract clean (${mode#--})"
