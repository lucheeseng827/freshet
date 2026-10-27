# freshet-airflow: run a Freshet state plan as an Airflow DAG

Freshet decides the minimal set of SQL models a change requires rebuilding and says
so as JSON on a frozen wire contract. dagron was the first orchestrator to read that
contract (its `dagron-state` crate). This package is the second: it turns the
same JSON into an Airflow DAG with one task per model, wired from the plan's own
edges.

That second reader is the point. The planner core's design notes call the planner
orchestrator-agnostic because consumers couple to the wire, not the linker, and a
claim with one consumer is a dagron feature with extra words. Here the claim is
checked mechanically: the tests are a port of dagron-state's, run against
byte-identical fixtures, and an opt-in suite compiles every plan with the real
`dagron-state` binary as well and requires the same task names, edges, commands and
recorded facts.

## Use it

```sh
freshet plan --project ./models --state .planner/state.json --json > dags/plan.json
```

```python
# dags/freshet_state_plan.py
from pathlib import Path
from freshet_airflow import build_dag

dag = build_dag(
    Path(__file__).with_name("plan.json").read_text(),
    dag_id="freshet_state_plan",
    command_template="dbt run --select {{ model }} --vars 'run_date: {{ ds }}'",
)
```

If the plan carries SQL that Freshet rendered (contract v4), run that SQL instead of
a command:

```python
dag = build_dag(plan_json, dag_id="freshet_state_plan", conn_id="warehouse")
```

Install it from PyPI:

```sh
pip install freshet-airflow              # into the environment that parses your DAGs
pip install 'freshet-airflow[airflow]'   # a fresh environment, Airflow included
```

The package declares no dependencies. It runs inside an Airflow deployment that has
already pinned Airflow and its providers with a constraints file, and a hard
dependency on `apache-airflow` would invite pip to re-resolve the scheduler's own
install. `conn_id` also needs `apache-airflow-providers-common-sql` and the
provider for your database.

## What you get

| | `command_template=` | `conn_id=` |
|---|---|---|
| Operator | `BashOperator` | `SQLExecuteQueryOperator` |
| Runs | your command, placeholders expanded per model | the statements Freshet rendered for the model |
| Needs | any plan | every model to carry v4 `sql` |

Both are thin subclasses of the stock operators (`isinstance` holds). When a task
runs, it logs the planner's facts about its model and, at WARNING level, anything the
plan made loud. The DAG gets no schedule (a state plan has no cron; it is
triggered), no catchup, the tags `freshet` and `state-plan`, and a `doc_md` that
explains the plan: how many models rebuild, why each one does, what each writes.
`dag_kwargs` overrides any of that; put retries and pools in its `default_args`.

Everything up to the operator constructors is decided without Airflow, by
`plan_operators()`, which returns plain records. `import freshet_airflow` needs no
Airflow at all; `build_dag()` imports it when called.

## Edges

Each task waits on the models its plan entry names, found in this order of trust,
with only the first source present used. This is dagron-state's `derived_edges`,
rule for rule:

1. **The plan's `depends_on`** (contract v2): the planner's own edge set, narrowed to
   the plan. Exact, and needs nothing from you. `[]` is an answer ("waits for
   nothing") and stops here; an *absent* field means a v1 producer and falls
   through. The two are never collapsed.
2. **Your `graph`** (`{"model": ["upstream", ...]}`), looked up per model, for plans
   from a v1 producer.
3. **The `because_of` attribution.** It names one cause, not every parent, so a model
   downstream of two changed models waits for only one of them. It is the last
   resort, reached only by a v1 plan with no `graph` entry for that model.

Whatever the source, an edge survives only if it names a model that is in the plan
and earlier in it, so a stale `graph` can never produce a cycle or a dangling task.
Edges are sorted and deduplicated. `ordering="sequential"` ignores all of this and
chains the plan in order.

Task ids are model names with everything outside `[A-Za-z0-9_-]` turned into `_`,
and collisions suffixed `_2`, `_3`, and so on. `.` in particular must not survive:
Airflow uses it to nest a task inside a TaskGroup, so `analytics.stg_orders` would
read as task `stg_orders` in a group called `analytics`. The real name is kept in
the task's facts, which are the key back to the model.

## Placeholders, and why they expand before Airflow's Jinja does

| Placeholder | Expands to |
|---|---|
| `{{ model }}` | the model name, unsanitized |
| `{{ unit }}` | `full_model` or `partitions` |
| `{{ partitions }}` | comma-joined; empty for a whole-model rebuild |
| `{{ replace }}` | the replace strategy (v3), e.g. `insert_overwrite`, never SQL |
| `{{ partition_column }}` | the replace op's partition column (v3) |
| `{{ unique_key }}` | the replace op's key columns, comma-joined (v3) |
| `{{ sql }}` | the rendered statements, joined by `";\n"` (v4) |
| `{{ dialect }}` | the dialect they were rendered for (v4) |

The first six are dagron-state's. Each works spelled `{{ name }}` or `{{name}}`, and
a fact the plan does not carry expands to the empty string.

The expansion happens in Python, when the DAG is built, before Airflow ever sees
the string:

* **The values are facts of the plan, not of the run.** They are fixed when the DAG
  is built. Airflow's Jinja context has no `model`; left for Airflow, the
  placeholder would fail as an undefined name. `{{ ds }}` and every other Airflow
  macro belong to the run, and are left exactly as written for Airflow to render.
* **One template means the same thing to both readers.** The same two spellings, the
  same empty-string rule, so a command written for dagron-state works here unchanged.
* **Then the values are protected from Airflow's pass.** A substituted value can
  contain braces (SQL does: `'{}'::jsonb`, or a `{#` in a regex that Jinja would
  silently read as the start of a comment). Every `{` in a value is rewritten as
  `{{ '{' }}`, which Airflow's render turns back into one brace. The shell sees the
  value as the plan had it. For that to hold, Jinja's default delimiters must
  apply, so a DAG whose `jinja_environment_kwargs` change them is refused.
* **A misspelling fails when the DAG is built, not when the task runs.**
  `{{model }}` or `{{ model|upper }}` would reach Airflow's Jinja, fail as undefined,
  and fail *at run time*, after upstream tasks have already written. `build_dag`
  refuses them instead.

Values are substituted as they are, not shell-quoted, exactly as in dagron-state.
To execute `{{ sql }}`, prefer `conn_id` to quoting SQL through a shell.

## Running the plan's SQL (`conn_id`)

Each model's statement list goes to `SQLExecuteQueryOperator` whole. The provider's
`DbApiHook.run` executes a list on **one connection and one cursor, in order**, and
commits once after the last statement, so the "must run in order on one session"
rule of contract v4 holds. `autocommit=False`, `parameters=None` (a `%` in the SQL is
never read as a driver placeholder) and `split_statements=False` are passed
explicitly, so `default_args` cannot change them.

The SQL is **not templated**. The stock operator runs `sql` through Jinja and treats a
string ending in `.sql` as a template *file*; both are hazards for SQL Freshet
rendered, which has nothing for Jinja to do. The subclass drops `sql` from
`template_fields` and executes it byte for byte. The statements are in the task's
doc, and the provider logs each one as it runs.

## Said loudly

Two things are put in the task's doc, in the DAG's doc under **read before
triggering**, and at WARNING level in every run's log:

* **A widened replace op**, whichever operator runs the model. The planner could not
  honour the declared strategy and widened to a full refresh of the whole relation:
  the difference between rewriting two partitions and rewriting a table.
* **A non-atomic write** (`"atomic": false`), when the task runs the SQL (through
  `conn_id`, or a command that uses `{{ sql }}`): a failure part-way can leave the
  relation partially written, and a retry starts again from the first statement.

## Refusals

| Raised | When |
|---|---|
| `PlannerFailed` | the plan carries `error` (its `code` and `message` are on the exception) |
| `EmptyPlan` | the plan has no models: nothing to rebuild, which is good news with no DAG in it |
| `NoCommand` | neither `command_template` nor `conn_id` |
| `RefusedRequest` | both of them; `conn_id` with models that carry no `sql` (they are named, not skipped); an empty statement list; SQL rendered for more than one dialect; a misspelled placeholder; custom Jinja delimiters |
| `ContractViolation` | a payload the contract does not define: a missing field, a wrong type, an unknown reason, unit, strategy or widening, a model named twice |

Unknown *fields* are ignored, because every contract revision so far has been
additive; that is how dagron-state keeps reading v4 payloads today. The first three
errors, and their order, are dagron-state's `CompileError`.

## What it does not do

* **Commit `next_state`.** It is carried opaquely on `parse_plan(...).next_state`.
  The planner's rule is that it is committed only after the plan has run
  successfully; that step belongs to whatever owns the state file.
* **Render SQL.** `{{ replace }}` expands to the planner's word, never to a
  statement. SQL comes from Freshet (v4) or from your command.
* **Check the dialect against the connection.** A Postgres-rendered plan pointed at
  a Snowflake `conn_id` fails in the warehouse, not here.

## Tests

```sh
pip install -e '.[test]'
pytest                    # the core suite needs no Airflow; the Airflow tests skip without it
```

With Airflow and the common-sql provider installed, `tests/test_airflow.py` builds
real DAGs and checks `upstream_task_ids`, operator types and fields, templating,
and a serialization round trip. The whole suite, opt-in parts included, passes on
Airflow 3.3.2 and on 2.11.2. Two suites are opt-in:

```sh
# Compare every plan against the real dagron-state binary (DAGRON = a dagron checkout).
cargo build --manifest-path "$DAGRON/Cargo.toml" -p dagron-state --no-default-features
FRESHET_DAGRON_STATE_BIN="$DAGRON/target/debug/dagron-state" pytest tests/test_dagron_state_agreement.py

# Run plans through `airflow dags test` (needs the sqlite provider; takes tens of seconds).
FRESHET_AIRFLOW_E2E=1 pytest tests/test_airflow_end_to_end.py
```

The fixtures in `tests/fixtures/` other than `handwritten_v4_sql.json` are
byte-for-byte copies of dagron-state's, and a test fails if they drift. Do not edit
one to make a test pass: if a contract test fails, the contract moved.
