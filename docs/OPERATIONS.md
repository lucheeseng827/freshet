# Freshet operations & troubleshooting

Short, honest runbook. `freshet` is a one-shot CLI; `freshet-serve` is the same
`ProjectCache` either embedded in-process or as a daemon over HTTP (flags, the
`FRESHET_SERVE_TOKEN` bearer token and `--watch` in [CONFIG.md](./CONFIG.md)). This
page covers the stateful things — the CLI's state file, a `ProjectCache`, and a
`freshet-state-server`'s directory — plus symptom-first troubleshooting.

## CLI (`freshet`) — state & recovery

- **The only persistent state** is the `--state` file (default
  `.planner/state.json`), written **only** by `plan --commit`. Back it up if
  you care about incremental behavior; losing it merely causes one full
  cold plan (over-rebuild is safe by design; under-rebuild is the bug class
  this tool exists to prevent).
- Commit discipline: run `freshet plan`, execute the plan with your
  orchestrator, and only then run `freshet plan --commit` (or commit in the
  same invocation only when execution is guaranteed to follow). Committing
  before the run means a later failure leaves state claiming work that never
  happened.
- `freshet submit` reads the state file and **never writes it**, deliberately: it
  creates a run, and a created run is not a finished one. The commit still belongs
  after the run succeeds. It prints that reminder on stderr with the exact
  `plan --commit` to run, so the two halves cannot drift apart.
- An empty plan makes `submit` exit 0 having opened no socket — "nothing to
  rebuild" is the good outcome, and a caller that submits on every commit hits it
  constantly.
- Restatements (`freshet restate`) never touch fingerprints; with `--state` they
  record the restatement in the state's partition ledger (owed before the run,
  settled by `--commit` after it).
- **Hosted state** (`--state https://…`, token in `FRESHET_STATE_TOKEN`) is the
  same document kept by a server, versioned, and committed by compare-and-swap.
  The server's directory is what to back up (below). Every run that reads it prints the version it
  planned against; pass that version to the later commit with `--expect-version`,
  so a commit that landed while your plan ran is caught rather than overwritten.
  Exit **3** means exactly that happened: re-plan (just run freshet again) and
  commit what the new plan rebuilt. Retrying the same commit keeps failing, correctly.

## Hosted state server (`freshet-state-server`) — runbook

- **One process per directory.** It locks `<dir>/.lock`; a second server on the same
  directory refuses to start, because two would each accept a commit against the
  same version. Scale by giving each server its own directory, not by sharing one.
- **Backups**: stop the server, then copy the directory; or take an atomic snapshot of
  the filesystem it is on. A copy made while it serves can be unusable: it may pick up
  a commit file and miss the snapshot written just before it, and a server restored
  from it refuses to load that state.
- **Restoring an older version**: every kept commit keeps its snapshot
  (`<version>.snapshot.json`). Restore one by committing it again, as a new version
  against the current one, so every job that planned against the current version
  sees its commit refused and re-plans:

  ```sh
  URL=https://state.example.com/v1/analytics; AUTH="Authorization: Bearer $FRESHET_STATE_TOKEN"
  v=$(curl -s -H "$AUTH" "$URL/current" | jq .version)
  # The snapshot is spliced in as text: a JSON tool that parses it may round its u64s.
  printf '{"expected_version":%s,"snapshot":%s,"summary":{"restored_from":5}}' \
    "$v" "$(cat /var/lib/freshet/analytics/5.snapshot.json)" |
    curl -s -H "$AUTH" -H 'Content-Type: application/json' --data-binary @- "$URL/commit"
  ```

  `freshet plan --state 5.snapshot.json` shows what that version would plan, and changes
  nothing anywhere. **Never delete a newer `.commit.json`** to roll back: the next commit
  would take its version number again, and a job that planned against the deleted
  commit would pass the compare-and-swap and commit over the restore.
- **Retention** (`--keep`, default 100) deletes a state's oldest commits as new ones
  land. History then ends at the oldest kept, and `freshet history` says so.
- **Tokens**: `FRESHET_STATE_SERVER_TOKEN` reads and commits; give CI jobs that only
  plan `FRESHET_STATE_SERVER_READ_TOKEN`, which cannot commit. Rotating either is a
  restart with the new value. It speaks plain HTTP: put TLS in front of it.
- **The console** (`--console apps/freshet-console`; on in the image) is at
  `/console/`. It lists every state, each one's commits and its snapshot, and downloads
  any kept version as the exact `state.json`. It only reads: sign in with
  `FRESHET_STATE_SERVER_READ_TOKEN`, which is all it needs. Its files are public; what
  they read is behind the token. Turn it off by leaving `--console` unset (in the
  image, `FRESHET_STATE_SERVER_CONSOLE=`).
- **A crash** loses at most the commit in flight, whose client then sees an error
  rather than a version, re-plans, and commits again. An orphaned
  `<version>.snapshot.json` with no `.commit.json` beside it is that commit's
  leftover, and the next commit overwrites it.

## Embedded daemon (`freshet_serve::ProjectCache`) — runbook

- **State = process memory, nothing else.** The committed snapshot
  (fingerprints + watermarks), the warm `Arc<Project>`, the revision counter,
  and up to 64 staged (un-committed) plans all live in the `RwLock` inside
  `ProjectCache`. There is no persistence: **a restart loses the committed
  snapshot**, and the first `plan` afterwards is a full cold plan. If your
  embedder needs continuity across restarts, persist the plan/commit results
  yourself (e.g. mirror commits into a freshet-state `FileStateBackend`).
- **Restart semantics:** construct a fresh `ProjectCache`, `reload` the
  project, re-plan. All previously issued tokens are gone (they were memory).
- **Stale tokens:** a `reload` (a request, or a `--watch` reload after an edit) that
  changes the project or the incremental config bumps the revision and invalidates
  every outstanding token — `commit` then
  returns the `stale plan` error and the caller must re-plan. A `reload` that
  changes nothing keeps the revision and the tokens. A staged plan is also
  evicted after 24 h uncommitted (`expired plan`) or when 64 newer plans push it
  out (`evicted plan`); a token already committed, or evicted longer ago than
  the last 1,024 evictions, is `unknown`. Treat every one the same way
  operationally: re-plan, re-run, re-commit.
- **Concurrency:** `ProjectCache` is `RwLock`-guarded and safe to share
  (`handle` takes `&self`); `plan`/`reload`/`commit` take the write lock, so
  they serialize.

## Troubleshooting (symptom first)

| Symptom (what you see) | Cause | Fix |
|---|---|---|
| CLI: `nothing to backfill — state is up to date.` | No fingerprint changed vs the committed state | Expected. Delete the state file (or point `--state` elsewhere) to force a full plan. |
| CLI exits **3**: `hosted state at … moved: … another commit landed after this plan was computed; re-plan` | Another job committed hosted state after this plan was computed (or `--expect-version` names a version the state has moved past) | Re-plan: run freshet again, execute what the new plan says, commit with the version it printed. Nothing was written. |
| CLI: `--state … is hosted state: set FRESHET_STATE_TOKEN …` / `… refused (HTTP 401)` | No token, or not the server's | Export the server's `FRESHET_STATE_SERVER_TOKEN` (or, to plan only, its read token) as `FRESHET_STATE_TOKEN`. It is never read from a flag or the URL. |
| CLI: `… refused the commit (HTTP 403): this token may read state but not commit it` | The server's read-only token (it can still plan and list history) | Commit with the server's `FRESHET_STATE_SERVER_TOKEN`. |
| CLI: `… does not page its history: it ignored --before N` | The server answers history without a `next_before` cursor | List without `--before`; the server can show only its newest page. |
| Server exits: `--console (or FRESHET_STATE_SERVER_CONSOLE)` … `has no index.html` | `--console` names a directory that is not the console | Point it at `apps/freshet-console` (in the image, `/usr/share/freshet/console`), or leave it unset. |
| Console: "That is not a token this server accepts" | The token pasted is neither of the server's | Paste `FRESHET_STATE_SERVER_READ_TOKEN`'s value. A server restarted with new tokens signs every open console out. |
| Server: `another freshet-state-server is serving <dir>` | A second server was started on a directory one already serves | Stop one, or give it its own `--dir`. Two on one directory could each accept a commit against the same version. |
| CLI: `hosted state carries field(s) [...] this freshet does not understand` | The server stores state a newer freshet wrote | Upgrade freshet. Planning without those fields, then committing, would drop them. |
| CLI: `… refused the plan (HTTP 400): options.command_template is required` | `submit` reached the consumer, which needs a per-model command | Pass `--command '<shell>'`, or supply the key via `--options-file`. The message is the consumer's own — freshet does not validate `options`. |
| CLI: `… refused the plan (HTTP 422)` | The consumer treats an empty plan as unprocessable | Should not happen from `submit`, which short-circuits an empty plan locally. If it does, the plan was non-empty here and empty there — check that `--project` and `--state` point where you think. |
| CLI: `submit` succeeded but nothing rebuilt | The run was created, not awaited | Check the run id the consumer returned. `submit` does not follow the run, and does not commit state on its behalf. |
| CLI: every model is `(directly changed)` after touching one file | No committed state yet (cold plan), or the state file was written by a different fingerprint scheme | Commit once (`--commit` after a run); subsequent plans are minimal. |
| CLI: a model rebuilds `full` instead of `partitions[…]` | The model (or the downstream hop) is not marked `incremental` — a `Table`/`View` can't be partition-scoped | Add it to `materializations.json` (`{"stg":"incremental"}`). This widening is deliberate: over-rebuild is safe. |
| CLI stderr: `N model(s) fell back to the text scheme (SQL did not parse)` | `sqlparser` couldn't parse that model; the fingerprint fell back to v1's text scheme | Cosmetic edits to that model will now trigger rebuilds (correct, just less minimal). Fix the SQL if it should parse. |
| CLI: `project has a dependency cycle involving: …` / daemon `{"status":"error","message":"dependency cycle: …"}` | Models reference each other cyclically | Break the cycle; the planner refuses to order it. |
| CLI: `…: unknown materialization ` | `materializations.json` (or a wire `materialization`) isn't `table`/`view`/`incremental` | Fix the value. |
| CLI: `error: unexpected argument` / missing `--model` | `restate` requires `--model` and `--partitions` | See [CONFIG.md](./CONFIG.md#freshet-cli-flags). |
| Daemon: `{"status":"error","message":"stale plan: project changed since plan; re-plan"}` | A `reload` bumped the revision between `plan` and `commit` | Re-plan against the current project, then commit the new token. Never retry the old token. |
| Daemon: `{"status":"error","message":"expired plan: token <t> waited longer than the 24h staged-plan TTL …"}` | The plan sat uncommitted past the staged-plan TTL | Re-plan. If runs legitimately take longer than the TTL, embed with `ProjectCache::with_staged_plan_ttl`. |
| Daemon: `{"status":"error","message":"evicted plan: token <t> was dropped to keep at most 64 uncommitted plans staged; re-plan"}` | More than 64 plans were staged after this one without being committed | Re-plan. If you routinely hit this, commit (or abandon) plans promptly. |
| Daemon: `{"status":"error","message":"unknown or already-committed token <t>"}` | Token never existed, was already committed, or was evicted before the last 1,024 evictions | Re-plan. Never retry a committed token. |
| Daemon: `{"status":"error","message":"bad request: unknown variant …"}` | `handle_json` got an op outside `reload`/`plan`/`restate`/`graph`/`commit` | Fix the request; contract in [API.md](./API.md). |
| Bindings: response has `"error":{"code":"bad_request"…}` | The request JSON didn't deserialize as a `PlanRequest` | Check field names/shapes against [API.md §2](./API.md#2-freshet-embed--the-json-funnel-the-frozen-wire-contract). |
| Bindings: `"error":{"code":"ffi_error"…}` (C ABI only) | Null pointer, non-UTF-8 input, or an internal panic caught at the boundary | Fix the caller; the library never unwinds into C. |
| `cargo build -p freshet-py` / `-p freshet-wasm` pulls pyo3/wasm-bindgen | You enabled the glue feature | The default build compiles them as pure re-exports; real artifacts come from `maturin` / `wasm-pack` (see [CONFIG.md](./CONFIG.md#binding-feature-gates)). |
