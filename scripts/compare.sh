#!/usr/bin/env bash
# scripts/compare.sh — time Freshet, dbt-core, dbt v2 (dbt-oss) and SQLMesh on the SAME
# generated topology, out-of-band, and write the result as BenchManifest JSON.
#
#   usage: scripts/compare.sh [--sizes 16x64,80x128] [--cols 8] [--seed 0x5151_5151]
#                             [--edit m_0_0] [--reps 5] [--warmup 1] [--quiet-wait 60]
#                             [--work DIR] [--out FILE]
#                             [--only freshet,dbt-core,dbt-oss,sqlmesh]
#
# Tools come from PATH (activate the venv holding dbt-core, dbt-duckdb and sqlmesh
# first), or from DBT=, DBT_OSS=, SQLMESH=, CURL= overrides. A tool that is not
# installed is recorded as skipped, never silently left out. The defaults reproduce
# results/compare-*.json and take on the order of two hours on a 4-core machine,
# almost all of it dbt-core and SQLMesh at 10,240 models.
#
# ---------------------------------------------------------------------------------
# What is timed, and why these commands
#
# Each number is the wall time of one whole process, as bash's `time` sees it, run
# from the project directory: interpreter start-up, imports and output included,
# because that is what a user waits for. User+system CPU time is recorded beside it
# (a tool that spreads over cores shows CPU > wall). Every operation gets WARMUP
# untimed runs (warm OS page cache and bytecode caches) and then REPS timed runs,
# sequentially; the median and MAD are computed by freshet-bench-manifest, not here.
# "cold" and "warm" below refer only to each TOOL'S OWN project cache.
#
# On a shared machine other processes compete for the same cores, so every
# repetition also records how much CPU the REST of the machine used while it ran
# (system-wide busy time from /proc/stat minus the repetition's own CPU time) and
# how much the hypervisor stole. Before each repetition the script waits up to
# --quiet-wait seconds for a one-second window in which the rest of the machine used
# under half a core. Neither makes a busy machine quiet; both make it visible.
#
# The tools do not all do the same work, so every row names its operation and the
# rows answer four different questions. Compare within a question, never across:
#
#   LOAD     parse every model and build the DAG; plan nothing.
#            freshet graph | dbt parse | dbt-oss parse | sqlmesh info --skip-connection
#   WARM     the same with the tool's own cache warm and nothing changed.
#            (freshet keeps no parse cache: its CLI is always cold)
#            dbt parse (partial parse, on by default) | dbt-oss parse --partial-parse
#            (opt-in) | sqlmesh info (.cache/) | freshet freshet-serve replan (the
#            daemon holds the parsed project in memory — a different architecture,
#            said so in its note)
#   PLAN0    what must rebuild when nothing has ever been built.
#            freshet plan (no state file) | sqlmesh plan --explain (empty state).
#            dbt has no such step.
#   DIFF     what must rebuild versus the last known state: nothing changed (0) and
#            one model edited (k = the edited model + its dependents, predicted by
#            the exporter). freshet plan vs a committed state | dbt ls -s
#            state:modified+ vs a base manifest (slim CI) | sqlmesh plan --explain vs
#            an applied state. Each tool's answer is checked against k.
#
# `sqlmesh plan` without --auto-apply still prompts, and aborts on a non-tty, so the
# plan-without-execution command is `plan --explain`: it computes the plan and prints
# what it would do, executing nothing. The applied state it diffs against is created
# once per size by an untimed `sqlmesh plan --auto-apply` over an empty bench.src.
# ---------------------------------------------------------------------------------
set -Eeuo pipefail
# `set -e` exits silently; say where, so an aborted two-hour run is diagnosable.
trap 'printf "compare.sh: line %s: \`%s\` failed (exit %s)\n" "$LINENO" "$BASH_COMMAND" "$?" >&2' ERR
export LC_ALL=C
# Usage telemetry at process exit would put a network round trip inside a timed run.
export DBT_SEND_ANONYMOUS_USAGE_STATS=false SQLMESH__DISABLE_ANONYMIZED_ANALYTICS=1

die() { printf 'compare.sh: %s\n' "$*" >&2; exit 1; }
log() { printf '[compare] %s\n' "$*" >&2; }
((BASH_VERSINFO[0] >= 4)) || die "bash >= 4 is required"

HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
MODULE=$(dirname -- "$HERE")
TARGET=${CARGO_TARGET_DIR:-$MODULE/target}

SIZES=16x64,80x128 COLS=8 SEED=0x5151_5151 EDIT=m_0_0 REPS=5 WARMUP=1 QUIET_WAIT=60 WORK='' OUT='' ONLY=''
while (($#)); do
    case $1 in
        -h | --help) sed -n '2,14p' "${BASH_SOURCE[0]}"; exit 0 ;;
        --sizes | --cols | --seed | --edit | --reps | --warmup | --quiet-wait | --work | --out | --only)
            (($# >= 2)) || die "$1 needs a value" ;;
        *) die "unknown argument: $1 (see --help)" ;;
    esac
    case $1 in
        --sizes) SIZES=$2 ;;
        --cols) COLS=$2 ;;
        --seed) SEED=$2 ;;
        --edit) EDIT=$2 ;;
        --reps) REPS=$2 ;;
        --warmup) WARMUP=$2 ;;
        --quiet-wait) QUIET_WAIT=$2 ;;
        --work) WORK=$2 ;;
        --out) OUT=$2 ;;
        --only) ONLY=$2 ;;
    esac
    shift 2
done
[[ $REPS =~ ^[1-9][0-9]*$ ]] || die "--reps must be a positive integer"
[[ $WARMUP =~ ^[0-9]+$ ]] || die "--warmup must be a non-negative integer"
[[ $QUIET_WAIT =~ ^[0-9]+$ ]] || die "--quiet-wait must be a non-negative number of seconds"
WORK=${WORK:-$(mktemp -d "${TMPDIR:-/tmp}/freshet-compare.XXXXXX")}
mkdir -p -- "$WORK"
WORK=$(cd -- "$WORK" && pwd)
OUT=${OUT:-$WORK/compare.json}
wanted() { [[ -z $ONLY || ,$ONLY, == *,$1,* ]]; }

# --- build -------------------------------------------------------------------------
log "building freshet, freshet-serve (http) and the freshet-bench tools (release)"
(
    cd -- "$MODULE"
    cargo build --quiet --release -p freshet-cli --bin freshet
    cargo build --quiet --release -p freshet-bench --bins
    cargo build --quiet --release -p freshet-serve --features http --bin freshet-serve
)
FRESHET=$TARGET/release/freshet
SERVE=$TARGET/release/freshet-serve
EXPORT=$TARGET/release/freshet-bench-export
MANIFEST=$TARGET/release/freshet-bench-manifest

# --- tools -------------------------------------------------------------------------
DBT=${DBT:-$(command -v dbt || true)}
DBT_OSS=${DBT_OSS:-$(command -v dbt-core-experimental-parser || true)}
SQLMESH=${SQLMESH:-$(command -v sqlmesh || true)}
CURL=${CURL:-$(command -v curl || true)}

# The interpreter behind a pip console script: the venv's python beside it, else the
# shebang. Used to read package versions offline (`dbt --version` asks PyPI).
python_of() {
    local dir line
    dir=$(dirname -- "$1")
    if [[ -x $dir/python ]]; then printf '%s' "$dir/python"; return; fi
    IFS= read -r line < "$1" || true
    line=${line#\#!}
    [[ -x ${line%% *} ]] && printf '%s' "${line%% *}" || printf 'python3'
}
pkg_version() { "$1" -c 'import sys, importlib.metadata as m; print(m.version(sys.argv[1]))' "$2" 2>/dev/null || echo "not installed"; }

DBT_PY='' SQLMESH_PY=''
[[ -n $DBT ]] && DBT_PY=$(python_of "$DBT")
[[ -n $SQLMESH ]] && SQLMESH_PY=$(python_of "$SQLMESH")
DBT_SKIP='' DBT_OSS_SKIP='' SQLMESH_SKIP='' SERVE_SKIP=''
if [[ -z $DBT ]]; then DBT_SKIP="dbt not found on PATH (pip install dbt-core dbt-duckdb)"
elif [[ $(pkg_version "$DBT_PY" dbt-duckdb) == "not installed" ]]; then DBT_SKIP="dbt-duckdb is not installed beside dbt-core"; fi
[[ -z $DBT_OSS ]] && DBT_OSS_SKIP="dbt-core-experimental-parser not found (it ships with dbt-core >= 1.12)"
[[ -z $SQLMESH ]] && SQLMESH_SKIP="sqlmesh not found on PATH (pip install sqlmesh)"
[[ -z $CURL ]] && SERVE_SKIP="curl not found; the daemon is timed through it"

FRESHET_VERSION=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version *= *"\(.*\)"/\1/p' "$MODULE/Cargo.toml")
DBT_V='' DBT_DUCKDB_V='' DBT_OSS_V='' SQLMESH_V=''
[[ -z $DBT_SKIP ]] && DBT_V=$(pkg_version "$DBT_PY" dbt-core) && DBT_DUCKDB_V=$(pkg_version "$DBT_PY" dbt-duckdb)
[[ -z $DBT_OSS_SKIP ]] && DBT_OSS_V=$("$DBT_OSS" --version 2>/dev/null | awk '/^dbt-oss / && !v { v = $2 } END { print v }')
[[ -z $SQLMESH_SKIP ]] && SQLMESH_V=$(pkg_version "$SQLMESH_PY" sqlmesh)

T_FRESHET="freshet $FRESHET_VERSION"
T_SERVE="freshet $FRESHET_VERSION (freshet-serve daemon)"
T_DBT="dbt-core $DBT_V (dbt-duckdb $DBT_DUCKDB_V)"
T_OSS="dbt-oss $DBT_OSS_V (dbt v2, beta)"
T_SQLMESH="sqlmesh $SQLMESH_V"

provenance() {
    local dirty=false cpu v1
    # Dirty = uncommitted changes in the code that is measured: this module and the
    # planner-core crates it path-depends on.
    # The core sits at ./core in the public repo; elsewhere, point FRESHET_CORE_DIR at it.
    v1=$(cd -- "${FRESHET_CORE_DIR:-$MODULE/core}" 2> /dev/null && pwd || true)
    [[ -n $(git -C "$MODULE" status --porcelain -- "$MODULE" ${v1:+"$v1"}) ]] && dirty=true
    # No `| head -1` anywhere in here: under pipefail, a reader that exits early can
    # SIGPIPE the writer and abort the run over a provenance string.
    cpu=$(awk -F': ' '/^model name/ && !seen { print $2; seen = 1 }' /proc/cpuinfo 2> /dev/null || true)
    [[ -z $cpu ]] && cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)
    printf 'commit\t%s\n' "$(git -C "$MODULE" rev-parse HEAD)"
    printf 'dirty\t%s\n' "$dirty"
    printf 'rustc\t%s\n' "$(rustc -V)"
    printf 'cpu\t%s\n' "$cpu"
    printf 'cores\t%s\n' "$(getconf _NPROCESSORS_ONLN)"
    printf 'os\t%s\n' "$(uname -srm)"
    printf 'build\tcargo build --release: %s\n' \
        "$(sed -n '/^\[profile.release\]/,/^\[/{/^[a-z]/p}' "$MODULE/Cargo.toml" | paste -sd, - | sed 's/,/, /g')"
    printf 'tool:freshet\t%s\n' "$FRESHET_VERSION"
    if [[ -z $DBT_SKIP ]]; then
        printf 'tool:dbt-core\t%s\n' "$DBT_V"
        printf 'tool:dbt-duckdb\t%s\n' "$DBT_DUCKDB_V"
        printf 'tool:dbt-adapters\t%s\n' "$(pkg_version "$DBT_PY" dbt-adapters)"
        printf 'tool:duckdb (dbt env)\t%s\n' "$(pkg_version "$DBT_PY" duckdb)"
        printf 'tool:python (dbt env)\t%s\n' "$("$DBT_PY" -c 'import platform; print(platform.python_version())')"
    fi
    [[ -z $DBT_OSS_SKIP ]] && printf 'tool:dbt-oss\t%s\n' "$DBT_OSS_V"
    if [[ -z $SQLMESH_SKIP ]]; then
        printf 'tool:sqlmesh\t%s\n' "$SQLMESH_V"
        printf 'tool:sqlglot\t%s\n' "$(pkg_version "$SQLMESH_PY" sqlglot)"
        printf 'tool:duckdb (sqlmesh env)\t%s\n' "$(pkg_version "$SQLMESH_PY" duckdb)"
        printf 'tool:python (sqlmesh env)\t%s\n' "$("$SQLMESH_PY" -c 'import platform; print(platform.python_version())')"
    fi
    [[ -z $SERVE_SKIP ]] && printf 'tool:curl\t%s\n' "$("$CURL" --version | awk 'NR == 1 { print $2 }')"
    return 0
}

# --- measurement -------------------------------------------------------------------
TIMEFORMAT='%3R %3U %3S'

# The command as recorded: the executable by name (its version is in provenance),
# arguments verbatim, quoted only where a shell would need it.
render_cmd() {
    local out=${1##*/} a
    shift
    for a in "$@"; do
        if [[ $a =~ ^[A-Za-z0-9_./:=+,@%-]+$ ]]; then out+=" $a"; else out+=" '${a//\'/\'\\\'\'}'"; fi
    done
    printf '%s' "$out"
}

HZ=$(getconf CLK_TCK 2> /dev/null || echo 100)

# "BUSY STEAL": system-wide busy and hypervisor-steal jiffies from the aggregate `cpu`
# line of /proc/stat, or "- -" where there is none (the manifest then records the
# contention columns as not measured rather than as zero).
cpu_jiffies() {
    local label user nice system _idle _iowait irq softirq steal rest
    if [[ -r /proc/stat ]] && read -r label user nice system _idle _iowait irq softirq steal rest < /proc/stat &&
        [[ $label == cpu ]]; then
        printf '%s %s' "$((user + nice + system + irq + softirq))" "${steal:-0}"
    else
        printf '%s' '- -'
    fi
}

# Wait up to QUIET_WAIT seconds for a one-second window in which everything else on
# the machine used under half a core, so a repetition does not start in the middle of
# another process's burst. Half a core, not zero: the agent runtimes sharing this kind
# of host idle at a few tenths of a core, and a gate that never opens measures nothing.
wait_quiet() {
    local waited b0 s0 b1 s1
    [[ $(cpu_jiffies) != '- -' ]] || return 0
    for ((waited = 0; waited < QUIET_WAIT; waited++)); do
        read -r b0 s0 <<< "$(cpu_jiffies)"
        sleep 1
        read -r b1 s1 <<< "$(cpu_jiffies)"
        (((b1 - b0 + s1 - s0) * 1000 / HZ < 500)) && return 0
    done
    log "$LABEL still busy after ${QUIET_WAIT}s; timing anyway (the contention is recorded per repetition)"
}

# measure ID TOOL OP NOTE PREPARE -- CMD...
# Runs from the current directory. PREPARE (untimed) runs before every repetition.
# A failing command aborts the whole run: a failed run's time is not a measurement.
measure() {
    local id=$1 tool=$2 op=$3 note=$4 prepare=$5 i kind real user sys b0 s0 b1 s1 other steal
    shift 5
    [[ $1 == -- ]] && shift
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$tool" "$op" "$(render_cmd "$@")" "$prepare" "$note" >> "$RUN/ops.tsv"
    for ((i = 1; i <= WARMUP + REPS; i++)); do
        kind=sample
        ((i <= WARMUP)) && kind=warmup
        [[ -z $prepare ]] || bash -c "$prepare" || die "$id: prepare step failed: $prepare"
        wait_quiet
        read -r b0 s0 <<< "$(cpu_jiffies)"
        if ! { time "$@" > "$RUN/out/$id.stdout" 2> "$RUN/out/$id.stderr"; } 2> "$RUN/out/$id.time"; then
            tail -n 30 "$RUN/out/$id.stderr" "$RUN/out/$id.stdout" >&2
            die "$id: $(render_cmd "$@") failed in $PWD"
        fi
        read -r b1 s1 <<< "$(cpu_jiffies)"
        read -r real user sys < "$RUN/out/$id.time"
        other=- steal=-
        if [[ $b0 != - ]]; then
            # Everyone's CPU during the repetition, minus the repetition's own (user+sys
            # of the timed process tree, from `time`, in ms). Tick accounting makes it
            # coarse (1/HZ s) and it can dip slightly below zero; it is recorded as is.
            other=$(((b1 - b0) * 1000 / HZ - (10#${user/./} + 10#${sys/./})))
            steal=$(((s1 - s0) * 1000 / HZ))
        fi
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$kind" "$real" "$user" "$sys" "$other" "$steal" >> "$RUN/samples.tsv"
        log "$LABEL $tool | $op | $kind $((i <= WARMUP ? i : i - WARMUP))/$((i <= WARMUP ? WARMUP : REPS)): ${real}s (elsewhere ${other} ms CPU, steal ${steal} ms)"
    done
}

# observe ID TEXT [ACTUAL EXPECTED]: what the command reported; with a count check.
# A tool that answers a different question makes its row incomparable, so a
# mismatch is written into the row itself and shouted, not quietly dropped.
observe() {
    local text=$2
    if (($# == 4)) && [[ $3 != "$4" ]]; then
        text="MISMATCH (expected $4): $text"
        log "WARNING: $1 reported $3, expected $4 — its row is not comparable"
    fi
    printf '%s\t%s\n' "$1" "$text" >> "$RUN/observed.tsv"
}
skip() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" >> "$RUN/skipped.tsv"; log "SKIP $1 | $2: $3"; }

freshet_count() { # "backfill plan (N model(s)" -> N ; "nothing to backfill" -> 0
    local first
    IFS= read -r first < "$1" || true
    if [[ $first =~ ^backfill\ plan\ \(([0-9]+) ]]; then echo "${BASH_REMATCH[1]}"; elif [[ $first == nothing* ]]; then echo 0; else echo "?"; fi
}
sqlmesh_backfill_count() { # entries under "Models needing backfill", "N more" lines expanded
    if grep -q 'No changes to plan' "$1"; then echo 0; return; fi
    awk '/^\*\*Models needing backfill:\*\*/ { s = 1; next }
         s && /^Explained plan/ { s = 0 }
         s && /^\* / { n++ }
         s && / more / { for (i = 1; i <= NF; i++) if ($i ~ /^[0-9]+$/) n += $i }
         END { print n + 0 }' "$1"
}
lines() { grep -c . "$1" || true; }
oneline() { tr '\n' ' ' < "$RUN/out/$1.stdout" | sed 's/  */ /g; s/ $//'; }
# "--explain applies nothing" is checked on every run, not asserted: after the last
# repetition the state file must still equal the copy it was restored from.
untouched() { cmp -s warehouse.duckdb "$1" && echo "state file unchanged" || echo "STATE FILE MODIFIED"; }

# --- per-tool runs -----------------------------------------------------------------
run_freshet() {
    local note_parse="single-threaded; Freshet keeps no parse cache, so every CLI run is cold and there is no separate warm CLI row"
    cd -- "$RUN/base/freshet"
    measure freshet.graph "$T_FRESHET" "LOAD: graph (parse + column lineage + topo order)" \
        "Reads every .sql file, parses it (sqlparser), derives column-level lineage and prints the models in topological order. Plans nothing. $note_parse. Compare with dbt parse / sqlmesh info, not with freshet plan." \
        "" -- "$FRESHET" graph --project .
    observe freshet.graph "$(lines "$RUN/out/freshet.graph.stdout") models in topological order" "$(lines "$RUN/out/freshet.graph.stdout")" "$N"

    measure freshet.plan-empty "$T_FRESHET" "PLAN0: plan vs no prior state (all N)" \
        "No state file, so every model is new: parse + lineage, AST-canonical fingerprint of every model, plan all N with reasons and a CU estimate, print it. Writes nothing (no --commit). $note_parse." \
        "rm -f ../../freshet-absent.json" -- "$FRESHET" plan --project . --state ../../freshet-absent.json
    observe freshet.plan-empty "$(freshet_count "$RUN/out/freshet.plan-empty.stdout") models in plan" "$(freshet_count "$RUN/out/freshet.plan-empty.stdout")" "$N"

    "$FRESHET" plan --project . --state ../../freshet-state.json --commit > /dev/null 2>&1
    measure freshet.plan-nochange "$T_FRESHET" "DIFF: plan vs committed state, nothing changed (0)" \
        "Against the base project's snapshot (committed once by an untimed plan --commit): the same full parse + fingerprint as a cold run (the CLI has no cache), a state diff, and 'nothing to backfill'. A CLI re-plan, not the daemon's warm path (see the freshet-serve row). $note_parse." \
        "" -- "$FRESHET" plan --project . --state ../../freshet-state.json
    observe freshet.plan-nochange "$(freshet_count "$RUN/out/freshet.plan-nochange.stdout") models in plan" "$(freshet_count "$RUN/out/freshet.plan-nochange.stdout")" 0

    cd -- "$RUN/edit/freshet"
    measure freshet.plan-edit "$T_FRESHET" "DIFF: plan vs committed state, $EDIT edited (k)" \
        "The edited project against the base project's committed snapshot: full parse + AST fingerprints, diff (only $EDIT changed), then the column-aware downstream plan with reasons and a CU estimate. $note_parse." \
        "" -- "$FRESHET" plan --project . --state ../../freshet-state.json
    observe freshet.plan-edit "$(freshet_count "$RUN/out/freshet.plan-edit.stdout") models in plan" "$(freshet_count "$RUN/out/freshet.plan-edit.stdout")" "$K"

    if [[ -n $SERVE_SKIP ]]; then skip "$T_SERVE" "WARM: daemon replan" "$SERVE_SKIP"; return; fi
    cd -- "$RUN/base/freshet"
    local port=${FRESHET_COMPARE_PORT:-18788} url token i
    url=http://127.0.0.1:$port
    "$SERVE" --project . --sources sources.json --port "$port" > "$RUN/out/serve.log" 2>&1 &
    SERVE_PID=$!
    for ((i = 0; i < 600; i++)); do
        "$CURL" -sf "$url/healthz" > /dev/null 2>&1 && break
        kill -0 "$SERVE_PID" 2> /dev/null || die "freshet-serve exited: $(tail -n 5 "$RUN/out/serve.log")"
        sleep 0.1
    done
    token=$("$CURL" -sS -X POST "$url/rpc" -d '{"op":"plan"}' | sed -n 's/.*"token":\([0-9][0-9]*\).*/\1/p') ||
        die "freshet-serve: the first plan request failed"
    [[ -n $token ]] || die "freshet-serve: no commit token from the first plan"
    "$CURL" -sS -X POST "$url/rpc" -d "{\"op\":\"commit\",\"token\":$token}" > /dev/null
    measure serve.replan-nochange "$T_SERVE" "WARM: daemon replan, nothing changed (0)" \
        "A whole curl process against a running freshet-serve that parsed the project once at start-up (untimed; the same load the freshet graph row times) and holds it in memory: loopback HTTP + the daemon's plan (AST-canonical re-fingerprint of all N models, which re-parses each one's SQL; diff against the committed in-memory snapshot) + the JSON reply. The CPU column covers the curl process only: the daemon is not its child, so the daemon's own work lands in this row's other-process CPU instead. A long-running daemon is a different architecture from the other tools' per-invocation caches; this is Freshet's warm path, not a like-for-like process." \
        "" -- "$CURL" -sS -X POST "http://127.0.0.1:$port/rpc" -d '{"op":"plan"}'
    local n
    n=$({ grep -o '"name"' "$RUN/out/serve.replan-nochange.stdout" || true; } | wc -l | tr -d ' ')
    observe serve.replan-nochange "$n models in plan" "$n" 0
    kill "$SERVE_PID" 2> /dev/null || true
    wait "$SERVE_PID" 2> /dev/null || true
    SERVE_PID=
}

run_dbt_core() {
    local tool=$T_DBT op
    if [[ -n $DBT_SKIP ]]; then
        for op in "LOAD: parse (cold)" "WARM: parse (partial parse)" "DIFF: ls state:modified+ (0)" "DIFF: ls state:modified+ (k)"; do skip "dbt-core" "$op" "$DBT_SKIP"; done
        return
    fi
    local common="single-threaded Python; includes interpreter start-up and adapter registration"
    cd -- "$RUN/base/dbt"
    measure dbt.parse-cold "$tool" "LOAD: parse, cold (fresh checkout, no target/)" \
        "Full parse: renders every model's Jinja (the static parser handles these ref/source-only models), resolves refs, builds the DAG and writes target/manifest.json + partial_parse.msgpack. Plans nothing: dbt has no plan step, parse is what every dbt command starts with. Compare with freshet graph / sqlmesh info, not with a plan. $common." \
        "rm -rf target logs" -- "$DBT" parse --profiles-dir .
    observe dbt.parse-cold "$("$DBT_PY" -c 'import json; d = json.load(open("target/perf_info.json")); print("dbt perf_info: load_all_elapsed %.3f s (parse_project %.3f s) over %d paths; static parser handled %d of %d models" % (d["load_all_elapsed"], d["parse_project_elapsed"], d["path_count"], d["static_analysis_parsed_path_count"], d["static_analysis_path_count"]))')"

    measure dbt.parse-warm "$tool" "WARM: parse, partial parse (default), nothing changed" \
        "target/partial_parse.msgpack from the previous run is present and no file changed, so dbt loads the pickled manifest instead of re-parsing (partial parsing is on by default). $common." \
        "" -- "$DBT" parse --profiles-dir .
    observe dbt.parse-warm "$("$DBT_PY" -c 'import json; d = json.load(open("target/perf_info.json")); print("dbt perf_info: load_all_elapsed %.3f s, partial parse enabled=%s" % (d["load_all_elapsed"], d["is_partial_parse_enabled"]))')"

    rm -rf target logs ../../dbt-core-state
    "$DBT" parse --profiles-dir . > /dev/null 2>&1
    mkdir -p ../../dbt-core-state && cp target/manifest.json ../../dbt-core-state/
    local sel=(--quiet ls --profiles-dir . -s state:modified+ --state ../../dbt-core-state --resource-type model --output name)
    local ls_note="Slim-CI selection on a fresh checkout: full parse (as the cold parse row), compare every node's checksum and config with the base manifest (from an untimed dbt parse of the unedited project), select the modified nodes and all descendants, print their names. Model-level: no column lineage. $common."
    measure dbt.ls-nochange "$tool" "DIFF: ls -s state:modified+, nothing changed (0)" "$ls_note" \
        "rm -rf target logs" -- "$DBT" "${sel[@]}"
    observe dbt.ls-nochange "$(lines "$RUN/out/dbt.ls-nochange.stdout") models selected" "$(lines "$RUN/out/dbt.ls-nochange.stdout")" 0
    cd -- "$RUN/edit/dbt"
    measure dbt.ls-edit "$tool" "DIFF: ls -s state:modified+, $EDIT edited (k)" "$ls_note" \
        "rm -rf target logs" -- "$DBT" "${sel[@]}"
    observe dbt.ls-edit "$(lines "$RUN/out/dbt.ls-edit.stdout") models selected" "$(lines "$RUN/out/dbt.ls-edit.stdout")" "$K"
}

run_dbt_oss() {
    local tool=$T_OSS op
    if [[ -n $DBT_OSS_SKIP ]]; then
        for op in "LOAD: parse (cold)" "WARM: parse --partial-parse" "DIFF: ls state:modified+ (0)" "DIFF: ls state:modified+ (k)"; do skip "dbt-oss" "$op" "$DBT_OSS_SKIP"; done
        return
    fi
    local flags=(--no-version-check --no-send-anonymous-usage-stats)
    local common="dbt v2.0, the Rust rewrite (beta), run as the dbt-core-experimental-parser binary that dbt-core 1.12 installs for its opt-in --use-v2-parser; default thread count (the core count); --no-version-check because the default is an online check"
    cd -- "$RUN/base/dbt-oss"
    measure oss.parse-cold "$tool" "LOAD: parse, cold (fresh checkout, no target/)" \
        "Full parse in its default mode, which writes target/manifest.json and keeps no parse cache. Plans nothing. Compare with freshet graph / dbt parse / sqlmesh info. $common." \
        "rm -rf target logs" -- "$DBT_OSS" parse "${flags[@]}"
    observe oss.parse-cold "dbt-oss summary: $(sed -n "s/^Finished 'parse' successfully.*\(\[[^]]*\]\).*/\1/p" "$RUN/out/oss.parse-cold.stdout" | tail -1)"

    rm -rf target logs
    "$DBT_OSS" parse --partial-parse "${flags[@]}" > /dev/null 2>&1
    measure oss.parse-warm "$tool" "WARM: parse --partial-parse (opt-in), nothing changed" \
        "With --partial-parse and the state it wrote on a previous run (created by one untimed run before the warm-ups), nothing changed. Opt-in: the flag is not in parse --help (it is referenced by --dirty), and the default parse keeps no cache. $common." \
        "" -- "$DBT_OSS" parse --partial-parse "${flags[@]}"
    observe oss.parse-warm "dbt-oss summary: $(sed -n "s/^Finished 'parse' successfully.*\(\[[^]]*\]\).*/\1/p" "$RUN/out/oss.parse-warm.stdout" | tail -1)"

    rm -rf target logs ../../dbt-oss-state
    "$DBT_OSS" parse "${flags[@]}" > /dev/null 2>&1
    mkdir -p ../../dbt-oss-state && cp target/manifest.json ../../dbt-oss-state/
    local sel=(ls -q -s state:modified+ --state ../../dbt-oss-state --resource-type model --output name "${flags[@]}")
    local ls_note="Slim-CI selection on a fresh checkout: full parse, compare with the base manifest dbt-oss wrote for the unedited project (untimed), select modified + descendants, print names. Model-level. $common."
    measure oss.ls-nochange "$tool" "DIFF: ls -s state:modified+, nothing changed (0)" "$ls_note" \
        "rm -rf target logs" -- "$DBT_OSS" "${sel[@]}"
    observe oss.ls-nochange "$(lines "$RUN/out/oss.ls-nochange.stdout") models selected" "$(lines "$RUN/out/oss.ls-nochange.stdout")" 0
    cd -- "$RUN/edit/dbt-oss"
    measure oss.ls-edit "$tool" "DIFF: ls -s state:modified+, $EDIT edited (k)" "$ls_note" \
        "rm -rf target logs" -- "$DBT_OSS" "${sel[@]}"
    observe oss.ls-edit "$(lines "$RUN/out/oss.ls-edit.stdout") models selected" "$(lines "$RUN/out/oss.ls-edit.stdout")" "$K"
}

run_sqlmesh() {
    local tool=$T_SQLMESH op
    if [[ -n $SQLMESH_SKIP ]]; then
        for op in "LOAD: info (cold)" "WARM: info (.cache)" "PLAN0: plan --explain (empty state)" "DIFF: plan --explain (0)" "DIFF: plan --explain (k)"; do skip "sqlmesh" "$op" "$SQLMESH_SKIP"; done
        return
    fi
    local common="SQLMesh loads models in parallel worker processes (CPU time above wall time); analytics disabled in config.yaml and the environment"
    cd -- "$RUN/base/sqlmesh"
    measure sqlmesh.info-cold "$tool" "LOAD: info --skip-connection, cold (no .cache/)" \
        "Loads the project: parses every model file (sqlglot), renders and optimizes each query with column types from external_models.yaml, builds the DAG, prints model/macro counts. --skip-connection: no warehouse or state access. Plans nothing. Compare with freshet graph / dbt parse. $common." \
        "rm -rf .cache logs" -- "$SQLMESH" info --skip-connection
    observe sqlmesh.info-cold "sqlmesh info: $(oneline sqlmesh.info-cold) (the count includes the external model bench.src)"
    measure sqlmesh.info-warm "$tool" "WARM: info --skip-connection, .cache/ present, nothing changed" \
        "As the cold row, with the .cache/ directory (parsed model definitions, optimized queries) left by the previous run. $common." \
        "" -- "$SQLMESH" info --skip-connection
    observe sqlmesh.info-warm "sqlmesh info: $(oneline sqlmesh.info-warm) (the count includes the external model bench.src)"

    # Untimed setup: an initialized empty state, and the base project applied once.
    rm -rf .cache logs warehouse.duckdb
    "$SQLMESH" plan --explain > /dev/null 2>&1 || die "sqlmesh: could not initialize an empty state"
    cp warehouse.duckdb ../../sqlmesh-empty.duckdb
    rm -rf .cache logs warehouse.duckdb
    "$SQLMESH_PY" -c "import duckdb; c = duckdb.connect('warehouse.duckdb'); c.execute('create schema bench'); c.execute('create table bench.src (' + ', '.join('c%d int' % i for i in range($COLS)) + ')')"
    log "$LABEL sqlmesh: applying the base plan once (untimed setup; executes every model over an empty bench.src)"
    local t0=$EPOCHREALTIME
    "$SQLMESH" plan --auto-apply --no-prompts > "$RUN/out/sqlmesh.apply.log" 2>&1 || die "sqlmesh apply failed: $(tail -n 5 "$RUN/out/sqlmesh.apply.log")"
    log "$LABEL sqlmesh: apply took $(awk -v a="$t0" -v b="$EPOCHREALTIME" 'BEGIN { printf "%.1f", b - a }')s"
    cp warehouse.duckdb ../../sqlmesh-applied.duckdb

    local plan_note="Plan without execution: load (cold .cache, as in a fresh checkout), read the state, fingerprint every model's snapshot, categorize changes, compute missing intervals and print the explained plan. --explain applies nothing; each row reports whether the state file was byte-identical after its last repetition. $common."
    measure sqlmesh.plan-empty "$tool" "PLAN0: plan --explain vs an initialized empty state (all N)" \
        "Against a state initialized once (untimed) with nothing applied, so every model is added. $plan_note" \
        "rm -rf .cache logs && cp ../../sqlmesh-empty.duckdb warehouse.duckdb" -- "$SQLMESH" plan --explain
    local n
    n=$(sqlmesh_backfill_count "$RUN/out/sqlmesh.plan-empty.stdout")
    observe sqlmesh.plan-empty "$n models needing backfill; $(untouched ../../sqlmesh-empty.duckdb)" "$n" "$N"

    measure sqlmesh.plan-nochange "$tool" "DIFF: plan --explain vs applied state, nothing changed (0)" \
        "Against the base project's applied state (applied once, untimed). $plan_note" \
        "rm -rf .cache logs && cp ../../sqlmesh-applied.duckdb warehouse.duckdb" -- "$SQLMESH" plan --explain
    n=$(sqlmesh_backfill_count "$RUN/out/sqlmesh.plan-nochange.stdout")
    observe sqlmesh.plan-nochange "$n models needing backfill; $(untouched ../../sqlmesh-applied.duckdb)" "$n" 0

    cd -- "$RUN/edit/sqlmesh"
    measure sqlmesh.plan-edit "$tool" "DIFF: plan --explain vs applied state, $EDIT edited (k)" \
        "The edited project against the base project's applied state: $EDIT is directly modified (breaking) and its dependents indirectly modified. $plan_note" \
        "rm -rf .cache logs && cp ../../sqlmesh-applied.duckdb warehouse.duckdb" -- "$SQLMESH" plan --explain
    n=$(sqlmesh_backfill_count "$RUN/out/sqlmesh.plan-edit.stdout")
    observe sqlmesh.plan-edit "$n models needing backfill; $(untouched ../../sqlmesh-applied.duckdb)" "$n" "$K"
}

# --- main --------------------------------------------------------------------------
SERVE_PID=''
cleanup() { [[ -z ${SERVE_PID:-} ]] || kill "$SERVE_PID" 2> /dev/null || true; }
trap cleanup EXIT

RUN_DIRS=()
IFS=, read -r -a SIZE_LIST <<< "$SIZES"
for size in "${SIZE_LIST[@]}"; do
    [[ $size =~ ^([1-9][0-9]*)x([1-9][0-9]*)$ ]] || die "--sizes entries look like 16x64 (layers x width), got $size"
    L=${BASH_REMATCH[1]} W=${BASH_REMATCH[2]}
    RUN=$WORK/run-${L}x${W}
    [[ ! -e $RUN ]] || die "$RUN already exists; pass a fresh --work"
    mkdir -p -- "$RUN/out"
    "$EXPORT" --layers "$L" --width "$W" --cols "$COLS" --seed "$SEED" --out "$RUN/base" > "$RUN/export.json"
    "$EXPORT" --layers "$L" --width "$W" --cols "$COLS" --seed "$SEED" --edit "$EDIT" --out "$RUN/edit" > "$RUN/export-edit.json"
    # Each dbt engine gets its own copy so neither ever sees the other's target/.
    cp -R -- "$RUN/base/dbt" "$RUN/base/dbt-oss"
    cp -R -- "$RUN/edit/dbt" "$RUN/edit/dbt-oss"
    N=$(sed -n 's/^  "models": \([0-9]*\),$/\1/p' "$RUN/export.json")
    K=$(sed -n 's/^    "closure": \([0-9]*\)$/\1/p' "$RUN/export-edit.json")
    [[ -n $N && -n $K ]] || die "could not read the model count / edit closure from the export summaries"
    LABEL="[$N models]"
    provenance > "$RUN/provenance.tsv"
    : > "$RUN/ops.tsv"
    : > "$RUN/samples.tsv"
    : > "$RUN/observed.tsv"
    : > "$RUN/skipped.tsv"
    log "$LABEL layers=$L width=$W cols=$COLS seed=$SEED; editing $EDIT rebuilds k=$K; reps=$REPS warmup=$WARMUP; in $RUN"
    # An excluded tool is still named in the manifest, so a partial run cannot pass
    # for a complete one.
    for t in freshet dbt-core dbt-oss sqlmesh; do
        if wanted "$t"; then "run_${t//-/_}"; else skip "$t" "all operations" "not selected (--only $ONLY)"; fi
    done
    RUN_DIRS+=("$RUN")
done

"$MANIFEST" "${RUN_DIRS[@]}" > "$OUT"
log "wrote $OUT"
"$MANIFEST" --table "$OUT" >&2
