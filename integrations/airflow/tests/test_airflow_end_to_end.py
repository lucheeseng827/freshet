"""A plan, run by Airflow itself: DAG files in a dags folder, executed by ``airflow dags test``.

Everything else in this suite inspects a DAG object. This runs one, through the
same path a deployment takes: Airflow parses a DAG file that calls
:func:`freshet_airflow.build_dag`, serializes it, schedules the tasks in dependency
order, renders their templates and executes them against a real connection.

Opt in with ``FRESHET_AIRFLOW_E2E=1``. It needs the ``airflow`` CLI of the running
interpreter and, for the SQL half, ``apache-airflow-providers-sqlite``; it creates a
throwaway ``AIRFLOW_HOME`` with its own SQLite metadata database, so it touches no
existing Airflow install. It takes tens of seconds, which is why it is opt-in.

The SQL half runs on SQLite rather than the Postgres dialect Freshet renders first,
so it proves the execution mechanics (one connection, statements in order, SQL that
is never templated) and not Postgres transaction semantics.
"""

import json
import os
import pathlib
import sqlite3
import subprocess
import sys

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("FRESHET_AIRFLOW_E2E") != "1",
    reason="set FRESHET_AIRFLOW_E2E=1 to run plans through `airflow dags test`",
)

PACKAGE_ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURES = pathlib.Path(__file__).with_name("fixtures")
LOGICAL_DATE = "2026-09-22"

DAG_FILE = '''\
from pathlib import Path

from freshet_airflow import build_dag

here = Path(__file__).parent
dag = build_dag((here / "{plan}").read_text(), dag_id="{dag_id}", {how})
'''


@pytest.fixture(scope="module")
def airflow_home(tmp_path_factory):
    pytest.importorskip("airflow", reason="apache-airflow is not installed")
    home = tmp_path_factory.mktemp("airflow_home")
    (home / "dags").mkdir()
    env = {
        **os.environ,
        "AIRFLOW_HOME": str(home),
        "AIRFLOW__CORE__DAGS_FOLDER": str(home / "dags"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "PYTHONPATH": os.pathsep.join(filter(None, [str(PACKAGE_ROOT), os.environ.get("PYTHONPATH")])),
    }
    airflow(env, "db", "migrate")
    return home, env


def airflow(env, *args):
    done = subprocess.run(
        [sys.executable, "-m", "airflow", *args], env=env, capture_output=True, text=True, timeout=600
    )
    assert done.returncode == 0, f"airflow {' '.join(args)} failed:\n{done.stdout[-4000:]}\n{done.stderr[-4000:]}"
    return done.stdout + done.stderr


def write_dag(home, dag_id, plan, how):
    (home / "dags" / f"{dag_id}.json").write_text(json.dumps(plan))
    (home / "dags" / f"{dag_id}.py").write_text(DAG_FILE.format(plan=f"{dag_id}.json", dag_id=dag_id, how=how))


def test_a_v2_fan_in_runs_each_model_after_both_of_its_parents(airflow_home):
    home, env = airflow_home
    ran = home / "ran.txt"
    plan = json.loads((FIXTURES / "captured_v2_fan_in.json").read_text())
    write_dag(home, "freshet_bash_e2e", plan, f"command_template=\"echo '{{{{ model }}}} {{{{ ds }}}}' >> {ran}\"")

    output = airflow(env, "dags", "test", "freshet_bash_e2e", LOGICAL_DATE)

    lines = ran.read_text().splitlines()
    assert sorted(lines) == sorted(f"{m} {LOGICAL_DATE}" for m in ("dim_customer", "stg_orders", "mart_revenue"))
    assert lines[-1] == f"mart_revenue {LOGICAL_DATE}", "the fan-in ran after BOTH parents"
    assert "Freshet plan facts:" in output, "each run logs the planner's record"


def test_a_plans_sql_runs_on_one_connection_in_order_and_untemplated(airflow_home):
    pytest.importorskip("airflow.providers.sqlite", reason="apache-airflow-providers-sqlite is not installed")
    home, env = airflow_home
    warehouse = home / "warehouse.db"
    env = {**env, "AIRFLOW_CONN_FRESHET_WAREHOUSE": json.dumps({"conn_type": "sqlite", "host": str(warehouse)})}

    def model(name, statements, depends_on, atomic=True, **extra):
        return {
            "name": name,
            "reason": "directly_changed" if not depends_on else {
                "downstream": {"because_of": depends_on[0], "via_columns": ["amount"]}
            },
            "unit": {"partitions": [LOGICAL_DATE]},
            "depends_on": depends_on,
            "sql": {"dialect": "sqlite", "statements": statements, "atomic": atomic},
            **extra,
        }

    plan = {
        "models": [
            model(
                "stg_orders",
                [
                    "CREATE TABLE IF NOT EXISTS stg_orders (id INTEGER, amount INTEGER, dt TEXT)",
                    f"DELETE FROM stg_orders WHERE dt IN ('{LOGICAL_DATE}')",
                    # A TEMP table lives only as long as its connection, so the next
                    # statement finds it only if both run on the same one.
                    "CREATE TEMP TABLE freshet_src AS SELECT 1 AS id, 10 AS amount, "
                    f"'{LOGICAL_DATE}' AS dt UNION ALL SELECT 2, 32, '{LOGICAL_DATE}'",
                    "INSERT INTO stg_orders SELECT * FROM freshet_src",
                ],
                [],
            ),
            model(
                "mart_revenue",
                [
                    "CREATE TABLE IF NOT EXISTS mart_revenue (dt TEXT PRIMARY KEY, total INTEGER, note TEXT)",
                    f"DELETE FROM mart_revenue WHERE dt = '{LOGICAL_DATE}'",
                    # Jinja would have eaten the comment and rendered ds; neither may happen.
                    "INSERT INTO mart_revenue SELECT dt, sum(amount), '{# kept #} {{ ds }}' "
                    f"FROM stg_orders WHERE dt = '{LOGICAL_DATE}' GROUP BY dt",
                ],
                ["stg_orders"],
                atomic=False,
            ),
        ]
    }
    write_dag(home, "freshet_sql_e2e", plan, 'conn_id="freshet_warehouse"')

    output = airflow(env, "dags", "test", "freshet_sql_e2e", LOGICAL_DATE)

    with sqlite3.connect(warehouse) as db:
        assert db.execute("SELECT id, amount FROM stg_orders ORDER BY id").fetchall() == [(1, 10), (2, 32)]
        assert db.execute("SELECT dt, total, note FROM mart_revenue").fetchall() == [
            (LOGICAL_DATE, 42, "{# kept #} {{ ds }}")
        ]
    assert "FRESHET WARNING: `mart_revenue` is a non-atomic write" in output, "said loudly, in the run's log"
