# freshet-plan: read a Freshet plan the way dagron reads it

Freshet answers one question: *given this SQL change, what is the minimal set of
models to rebuild?* The answer crosses every boundary as the same JSON, the
`PlanResponse` on a frozen wire contract. This package reads it in Python, with the
standard library only, by the rules dagron's `dagron-state` reads it by. It is what
the orchestrator adapters build on:

| Package | Turns a plan into |
|---|---|
| [`freshet-airflow`](../airflow) | an Airflow DAG, one task per model |
| [`freshet-dagster`](../dagster) | a Dagster job, one op per model |

Each one decides nothing of its own about the plan. Which task waits for which,
what each is called, what its command says and why it runs all come from here, so
the answers are the same in every orchestrator and are tested once.

```python
from freshet_plan import compile_plan
from freshet_plan.explain import summary, why

tasks = compile_plan(open("plan.json").read())
print(summary(tasks))
for task in tasks:
    print(task.task_id, "waits on", task.upstream, "-", why(task.model))
    print("  ", task.render("dbt run --select {{ model }} --vars 'p: {{ partitions }}'"))
```

## What is in it

* **`parse_plan`**: the contract, v1 to v5, strict about what it defines (a missing
  field or an unknown variant is a `ContractViolation`) and blind to what it does not
  (an unknown field is ignored), as serde is.
* **`compile_plan`**: tasks with ids and edges. The edges come from the plan's own
  `depends_on`, else a `graph` you pass, else the single-cause `because_of`, and only
  to models earlier in the plan, so a stale graph cannot make a cycle. Task ids are
  dagron-state's: `[A-Za-z0-9_-]`, collisions suffixed `_2`.
* **`task_facts`**: the planner's facts about a model, kept with its task.
* **`render_template`**: the placeholders `{{ model }}`, `{{ unit }}`,
  `{{ partitions }}`, `{{ replace }}`, `{{ partition_column }}`, `{{ unique_key }}`,
  `{{ sql }}` and `{{ dialect }}`, in the two spellings dagron-state expands, in one
  pass. `misspelled_placeholders` and `unknown_placeholders` find what it would not
  expand, for an adapter to refuse.
* **`freshet_plan.explain`**: the plan in words. The headline (with a pruning
  percentage only when the project size is known), why each model is in the plan,
  what each one writes, and the warnings that must not be missed: a replace op the
  planner widened to a full refresh, and a non-atomic write.

`next_state` rides along opaquely on `parse_plan(...).next_state`. Committing it
after a successful run belongs to whatever owns the state file.

## Tests

```sh
pip install -e '.[test]'
pytest
```

The fixtures in `tests/fixtures/` other than `handwritten_v4_sql.json` are
byte-for-byte copies of dagron-state's, and a test fails if they drift
(`FRESHET_DAGRON_DIR` = a dagron checkout). Do not edit one to make a test pass: if a
contract test fails, the contract moved. One suite asks the real dagron-state binary
for the same names, edges, commands and facts:

```sh
cargo build --manifest-path "$DAGRON/Cargo.toml" -p dagron-state --no-default-features --bin dagron-state
pip install pyyaml
FRESHET_DAGRON_STATE_BIN="$DAGRON/target/debug/dagron-state" pytest tests/test_dagron_state_agreement.py
```

Freshet's CI runs both.
