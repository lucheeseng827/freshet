"""Real Dagster: the job's graph, and runs of it in process.

Skips without Dagster. CI installs a pinned release and runs these; they pass on
Dagster 1.13.27.
"""

import pathlib

import pytest

dg = pytest.importorskip("dagster")

from freshet_dagster import RefusedRequest, build_job  # noqa: E402

FIXTURES = pathlib.Path(__file__).resolve().parents[2] / "plan" / "tests" / "fixtures"


def fixture(name):
    return (FIXTURES / name).read_text()


def plan_of(*models):
    return {"models": [{"reason": "directly_changed", "unit": "full_model", "depends_on": [], **m} for m in models]}


def upstream_of(job):
    """Op name -> the op names it waits on, read back from Dagster's own graph."""
    structure = job.graph.dependency_structure
    return {
        node.name: sorted(
            output.node_name
            for outputs in structure.input_to_upstream_outputs_for_node(node.name).values()
            for output in outputs
        )
        for node in job.graph.nodes
    }


def steps(result, kind):
    return [e.step_key for e in result.all_events if e.event_type_value == kind]


def test_the_graph_is_the_plans_edges():
    job = build_job(fixture("captured_v2_fan_in.json"), name="fan_in", command_template="true")
    assert upstream_of(job) == {"dim_customer": [], "stg_orders": [], "mart_revenue": ["dim_customer", "stg_orders"]}
    assert job.tags == {"freshet": "state-plan"}
    assert "3 models rebuild" in job.description


def test_a_run_rebuilds_each_model_once_and_after_what_it_waits_on(tmp_path):
    ran = tmp_path / "ran.txt"
    job = build_job(
        fixture("captured_v3_with_a_widening.json"),
        name="rebuild",
        command_template=f"echo '{{{{ model }}}} {{{{ partitions }}}}' >> {ran}",
    )
    result = job.execute_in_process()
    assert result.success
    lines = ran.read_text().splitlines()
    assert sorted(lines) == ["daily_rollup 2026-06-20", "events 2026-06-20", "user_dim "]
    assert lines[0] == "events 2026-06-20", "both dependents wait on events"


def test_a_failed_command_fails_its_op_and_nothing_downstream_runs(tmp_path):
    ran = tmp_path / "ran.txt"
    plan = plan_of({"name": "stg"}, {"name": "mart", "depends_on": ["stg"]})
    job = build_job(
        plan,
        name="fails",
        command_template=f"echo {{{{ model }}}} >> {ran}; [ {{{{ model }}}} != stg ] || (echo boom >&2; exit 3)",
    )
    result = job.execute_in_process(raise_on_error=False)
    assert not result.success
    assert steps(result, "STEP_FAILURE") == ["stg"]
    assert "mart" not in steps(result, "STEP_START")
    assert ran.read_text().splitlines() == ["stg"]
    failure = result.failure_data_for_node("stg")
    assert failure.user_failure_data.description == "the command for `stg` exited 3"
    assert failure.user_failure_data.metadata["exit code"].value == 3
    assert failure.user_failure_data.metadata["last output"].value == "boom", "what it printed, not what it wrote"


def test_run_receives_the_task_and_the_resources_it_asked_for():
    seen = []

    def run(context, task):
        context.resources.warehouse.append((task.model.name, task.model.sql.statements))

    warehouse = dg.ResourceDefinition.hardcoded_resource(seen)
    job = build_job(
        fixture("handwritten_v4_sql.json"),
        name="by_run",
        run=run,
        required_resource_keys={"warehouse"},
        job_kwargs={"resource_defs": {"warehouse": warehouse}},
    )
    assert job.execute_in_process().success
    assert [name for name, _ in seen][0] == "events"
    assert dict(seen)["events"][0] == "DELETE FROM events WHERE dt IN ('2026-06-20')"
    assert len(seen) == 3


def test_the_warnings_are_logged_at_warning_level_when_the_op_runs():
    instance = dg.DagsterInstance.ephemeral()
    job = build_job(fixture("captured_v3_with_a_widening.json"), name="loud", command_template="true")
    result = job.execute_in_process(instance=instance)
    warned = [
        (e.step_key, e.user_message)
        for e in instance.all_logs(result.run_id)
        if e.level >= 30 and e.step_key is not None
    ]
    assert len(warned) == 1, warned
    assert warned[0][0] == "user_dim"
    assert "widened to a full refresh" in warned[0][1]


def test_a_model_named_like_a_python_keyword_still_builds():
    plan = plan_of({"name": "type"}, {"name": "class", "depends_on": ["type"]}, {"name": "input-x"})
    job = build_job(plan, name="reserved", command_template="true")
    assert upstream_of(job) == {"type_2": [], "class_2": ["type_2"], "input_x": []}
    assert job.execute_in_process().success


def test_job_kwargs_override_the_defaults():
    job = build_job(
        fixture("golden.json"),
        name="custom",
        command_template="true",
        job_kwargs={"tags": {"team": "data"}, "description": "mine"},
    )
    assert job.tags == {"team": "data"}
    assert job.description == "mine"


def test_an_airflow_macro_is_refused_before_dagster_sees_it():
    with pytest.raises(RefusedRequest, match="has no meaning here"):
        build_job(fixture("golden.json"), name="macro", command_template="dbt run -s {{ model }} --vars '{{ ds }}'")
