# freshet-dagster: run a Freshet state plan as a Dagster job

Freshet decides the minimal set of SQL models a change requires rebuilding and says
so as JSON on a frozen wire contract. This package turns that JSON into a Dagster job
with one op per model, wired from the plan's own edges. It reads the plan through
[`freshet-plan`](../plan), as [`freshet-airflow`](../airflow) does, so a plan gets the
same edges, placeholders, facts and explanations in Dagster, Airflow and dagron.

## Use it

```sh
freshet plan --project ./models --state .planner/state.json --json > plan.json
```

```python
# definitions.py
from pathlib import Path

import dagster as dg
from freshet_dagster import build_job

job = build_job(
    Path(__file__).with_name("plan.json").read_text(),
    name="freshet_state_plan",
    command_template="dbt run --select {{ model }}",
)
defs = dg.Definitions(jobs=[job])
```

To run the SQL Freshet rendered (contract v4) on a connection of your own, pass `run`
instead of a command. It is called as `run(context, task)` for each model:

```python
def run_sql(context, task):
    with context.resources.warehouse.get_connection() as conn:  # your resource
        with conn.cursor() as cur:
            for statement in task.model.sql.statements:  # in order, on one connection
                cur.execute(statement)
        conn.commit()

job = build_job(
    plan_json,
    name="freshet_state_plan",
    run=run_sql,
    required_resource_keys={"warehouse"},
    job_kwargs={"resource_defs": {"warehouse": warehouse}},
)
```

Install it beside your Dagster:

```sh
pip install freshet-dagster              # into the environment your code location runs in
pip install 'freshet-dagster[dagster]'   # a fresh environment, Dagster included
```

The package depends only on `freshet-plan`, which is standard library only. A Dagster
deployment pins `dagster` and its sibling packages to one release together, and a
dependency on `dagster` here would invite pip to move it without them.

## What you get

* **One op per model**, with a `Nothing` input per op it waits on and a `Nothing`
  output. The edges order the ops and carry no data: a rebuild's result is in the
  warehouse, not in an IO manager.
* **The command** (`command_template=`) runs under `bash -c`, as Airflow's
  `BashOperator` runs it, after the model's placeholders are expanded. Its stdout and
  stderr are logged line by line as they are printed. A non-zero exit fails the op
  with the exit code and the last 20 lines as metadata, and nothing downstream runs.
* **Facts and warnings.** Each op logs the planner's facts about its model when it
  runs and, at WARNING level, anything the plan made loud: a replace op the planner
  widened to a full refresh, and a non-atomic write when the op runs the SQL. Each
  op's description says why it runs, what it writes and what it waits on. The job's
  description is the plan's headline and a table of every model. The words are
  `freshet_plan.explain`'s, the same as in an Airflow DAG's docs.
* **Tags.** The job is tagged `freshet: state-plan`. Each op carries
  `freshet/model` (the real name), `freshet/reason`, `freshet/unit` and, from contract
  v3, `freshet/replace`.

`job_kwargs` goes to `GraphDefinition.to_job` and overrides the description and tags;
put `resource_defs`, `executor_def`, `op_retry_policy` or `config` in it. `graph=` and
`ordering="sequential"` are `freshet_plan.compile_plan`'s.

## Op names

Dagster accepts `[A-Za-z0-9_]` in a name and reserves Python's keywords and a few
words of its own (`input`, `type`, `config`, …). A name it accepts as it is never
changes. Any other has each disallowed character turned into `_` and, when that is
taken or reserved, a suffix `_2`, `_3`, … until free. So `analytics.stg` becomes
`analytics_stg`, and a model called `type` gets an op called `type_2`. The real name
is in the op's `freshet/model` tag and its facts.

## Placeholders, and why nothing else in braces is allowed

The placeholders are freshet-plan's: `{{ model }}`, `{{ unit }}`, `{{ partitions }}`,
`{{ replace }}`, `{{ partition_column }}`, `{{ unique_key }}`, `{{ sql }}` and
`{{ dialect }}`, each spelled `{{ name }}` or `{{name}}`.

Airflow renders a command with Jinja after Freshet has, so `{{ ds }}` means
something in an Airflow command. Dagster renders nothing afterwards, so anything else
in `{{ }}` would reach the shell as written. `build_job` refuses it, a misspelling
such as `{{model }}` included, rather than let a command fail or do something else
at run time. A run's own values (a date, a target) come from its environment or its
config.

## Refusals

| Raised | When |
|---|---|
| `PlannerFailed` | the plan carries `error` |
| `EmptyPlan` | the plan has no models: nothing to rebuild |
| `NoCommand` | neither `command_template` nor `run` |
| `RefusedRequest` | both of them; a placeholder Freshet would not expand |
| `ContractViolation` | a payload the contract does not define |
| `TypeError` | `command_template` that is not a string, `run` that is not callable |

The first three, and their order, are dagron-state's.

## What it does not do

* **Commit `next_state`.** It rides along on `parse_plan(...).next_state`. The
  planner's rule is that it is committed only after the plan ran successfully.
* **Assets.** A state plan is a set of rebuilds to run once, so it becomes a job,
  not software-defined assets. It does not record materializations in the asset
  catalog yet.
* **Partitions.** A model's partitions reach its command through
  `{{ partitions }}`. They are not mapped onto a Dagster `PartitionsDefinition`,
  which is the job's, not one model's.

## Tests

```sh
pip install -e ../plan -e '.[test]'   # freshet-plan from beside it, until it is on PyPI
pytest                    # the decisions need no Dagster; the Dagster tests skip without it
```

With Dagster installed, `tests/test_dagster.py` builds real jobs and runs them in
process: the graph's edges, a run in dependency order, a failed command stopping
everything downstream, `run` with a resource, the warnings in the run's log, reserved
names, and `job_kwargs`. It passes on Dagster 1.13.27, which Freshet's CI pins.
