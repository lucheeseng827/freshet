# Contributing to Freshet

Thanks for your interest in improving Freshet.

## Developer Certificate of Origin (DCO)

We use the [DCO](https://developercertificate.org/) rather than a CLA. By signing off
on your commits you certify that you wrote the patch or otherwise have the right to
submit it under the project's Apache-2.0 license (inbound = outbound).

```bash
git commit -s -m "your message"
```

## Layout

Two Cargo workspaces, one dependency direction:

- `core/` — the planner core: `freshet-core` (graph, lineage, plan), `freshet-sql`
  (SQL → lineage), `freshet-state` (state model + file backend), `freshet-api` (the
  embeddable facade). It never depends on anything outside `core/`.
- `.` — everything built on it: `freshet-incremental`, `freshet-fingerprint`,
  `freshet-render`, `freshet-embed` (the frozen JSON wire contract), `freshet-dbt`,
  `freshet-cli` (the `freshet` binary), `freshet-serve`, `freshet-ffi`,
  `freshet-wasm`, `freshet-py` and `freshet-bench`.
- `sdks/python`, `integrations/airflow`, `apps/` — the Python client, the Airflow
  adapter and the effort-map apps.

## Development

```bash
(cd core && cargo test --workspace --locked)
cargo test --locked                                   # default members
cargo test --locked -p freshet-serve --features http
cargo fmt --all && cargo clippy --all-targets -- -D warnings
(cd sdks/python && python -m unittest -v)
(cd integrations/airflow && python -m pytest -q)
```

## Expectations

- The wire contract (`freshet-embed`'s `WIRE_CONTRACT_VERSION`) is frozen per
  revision: a change to its shape is a new revision, with fixtures, never an edit.
- A plan may widen (rebuild more) when it is unsure; it must never narrow. Add a
  test for any change to what gets selected.
- Add tests for new behaviour, and keep commit messages conventional (`feat:`,
  `fix:`, `docs:`).

## Code of Conduct

This project follows the [Contributor Covenant](https://www.contributor-covenant.org/).
Be respectful.
