"""Two consumers, one contract, checked against each other rather than against a spec.

The other test files port dagron-state's assertions. This one asks dagron-state
itself: it compiles each plan with the real ``dagron-state`` binary and with this
package, and requires the same task names, the same edges, the same rendered
commands and the same recorded facts. It runs when ``FRESHET_DAGRON_STATE_BIN``
points at a built binary::

    cd <dagron checkout>
    cargo build -p dagron-state --no-default-features --bin dagron-state
    FRESHET_DAGRON_STATE_BIN=$PWD/target/debug/dagron-state pytest tests/test_dagron_state_agreement.py

Comparing commands and facts needs PyYAML (dagron-state prints its spec as YAML);
edges and names come from its JSON explanation and need nothing extra.

Two places where the readers differ on purpose are left out: a plan naming a model
twice (this package refuses it) and contract v4's ``sql``, which dagron-state does
not read yet and therefore neither expands nor records.
"""

import json
import os
import pathlib
import subprocess

import pytest

from freshet_airflow import compile_plan
from freshet_airflow.dag import _summary

BIN = os.environ.get("FRESHET_DAGRON_STATE_BIN")
pytestmark = pytest.mark.skipif(
    not BIN,
    reason="set FRESHET_DAGRON_STATE_BIN to a built dagron-state binary to compare against it",
)

FIXTURES = pathlib.Path(__file__).with_name("fixtures")

# Every placeholder the two readers share, in both spellings.
TEMPLATE = (
    "m={{ model }} u={{unit}} p={{ partitions }} r={{replace}} "
    "c={{ partition_column }} k={{unique_key}} again={{model}}"
)


def fixture(name):
    return json.loads((FIXTURES / name).read_text())


def direct(name, **extra):
    return {"name": name, "reason": "directly_changed", "unit": "full_model", **extra}


def downstream(name, because_of, **extra):
    return {
        "name": name,
        "reason": {"downstream": {"because_of": because_of, "via_columns": ["amount"]}},
        "unit": "full_model",
        **extra,
    }


CASES = {
    "golden": (fixture("golden.json"), None),
    "golden with a graph": (fixture("golden.json"), {"mart_revenue": ["stg_orders", "raw_orders"]}),
    "captured v1": (fixture("captured_from_planner_cli.json"), None),
    "captured v2 fan-in": (fixture("captured_v2_fan_in.json"), None),
    "v2 edges beat a stale graph": (fixture("captured_v2_fan_in.json"), {"mart_revenue": ["stg_orders"]}),
    "captured v3 widening": (fixture("captured_v3_with_a_widening.json"), None),
    "handwritten v4": (fixture("handwritten_v4_sql.json"), None),
    "v1 fan-in, attribution only": ({"models": [direct("a"), direct("b"), downstream("mart", "a")]}, None),
    "v1 fan-in with a graph": ({"models": [direct("a"), direct("b"), downstream("mart", "a")]}, {"mart": ["a", "b"]}),
    "a stale graph": ({"models": [direct("a"), downstream("mart", "a")]}, {"a": ["mart", "deleted_model"]}),
    "a partial graph": (
        {"models": [direct("a"), direct("b"), downstream("mart", "a"), downstream("other", "b")]},
        {"mart": ["a", "b"]},
    ),
    "forward and unknown planner edges": (
        {"models": [direct("a", depends_on=["mart", "ghost"]), direct("mart", depends_on=["a"])]},
        None,
    ),
    "an empty edge list on a downstream model": (
        {"models": [direct("a", depends_on=[]), downstream("mart", "a", depends_on=[])]},
        None,
    ),
    "names that sanitize and collide": (
        {
            "models": [
                direct("analytics.stg_orders"),
                direct("analytics/stg_orders"),
                direct("analytics-stg_orders"),
                direct("analytics_stg_orders_2"),
                downstream("mart", "analytics/stg_orders"),
            ]
        },
        None,
    ),
    "partitions and a merge key": (
        {
            "models": [
                direct(
                    "events",
                    unit={"partitions": ["2026-09-01", "2026-09-02"]},
                    replace={"strategy": "merge", "unique_key": ["id", "dt"], "target": {"partitions": ["2026-09-01"]}},
                ),
                downstream("rollup", "events", replace={"strategy": "delete_insert", "partition_column": "dt",
                                                        "target": {"partitions": []}}),
            ]
        },
        None,
    ),
}


def dagron_state(tmp_path, subcommand, plan, graph, ordering, *flags):
    envelope = {"plan": plan, "options": {"ordering": ordering, "command_template": ["sh", "-c", TEMPLATE]}}
    if graph is not None:
        envelope["graph"] = graph
    path = tmp_path / "envelope.json"
    path.write_text(json.dumps(envelope))
    done = subprocess.run([BIN, subcommand, str(path), *flags], capture_output=True, text=True, check=False)
    assert done.returncode == 0, f"dagron-state {subcommand} failed: {done.stderr}"
    return done.stdout


@pytest.mark.parametrize("ordering", ["derived", "sequential"])
@pytest.mark.parametrize("case", sorted(CASES))
def test_both_readers_name_and_wire_every_task_the_same_way(tmp_path, case, ordering):
    plan, graph = CASES[case]
    theirs = json.loads(dagron_state(tmp_path, "explain", plan, graph, ordering, "--json"))
    ours = compile_plan(plan, graph=graph, ordering=ordering)

    assert [(row["model"], row["task"], row["depends_on"]) for row in theirs["rows"]] == [
        (t.model.name, t.task_id, list(t.upstream)) for t in ours
    ]
    assert theirs["summary"] == _summary(ours, graph), "and the same headline"


@pytest.mark.parametrize("case", sorted(CASES))
def test_both_readers_render_the_same_commands_and_record_the_same_facts(tmp_path, case):
    yaml = pytest.importorskip("yaml", reason="comparing commands needs PyYAML to read dagron-state's spec")
    plan, graph = CASES[case]
    # BaseLoader keeps every scalar a string. The default loaders would turn an
    # unquoted partition like 2026-09-01 into a date, a disagreement YAML invents.
    spec = yaml.load(dagron_state(tmp_path, "plan", plan, graph, "derived"), Loader=yaml.BaseLoader)
    ours = compile_plan(plan, graph=graph)

    assert len(spec["tasks"]) == len(ours)
    for theirs, task in zip(spec["tasks"], ours):
        assert theirs["name"] == task.task_id
        assert theirs["command"] == ["sh", "-c", task.render(TEMPLATE)]
        # dagron-state records a v4 model's SQL flat (`sql_dialect`, `sql_statements`,
        # `sql_atomic`); this side keeps it nested. Same facts, compared as such —
        # with YAML's BaseLoader, a boolean arrives as its string.
        facts = {k: v for k, v in task.facts.items() if k != "sql"}
        if "sql" in task.facts:
            sql = task.facts["sql"]
            facts.update(
                sql_dialect=sql["dialect"],
                sql_statements=sql["statements"],
                sql_atomic="true" if sql["atomic"] else "false",
            )
        assert theirs["input"] == facts
