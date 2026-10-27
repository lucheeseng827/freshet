"""Compilation: edges, ordering, naming, rendering, refusals.

A port of dagron-state's ``tests/compile.rs``, test for test and assertion for
assertion, so that the two consumers are held to the same answers. The helpers
build wire-shaped dicts rather than dataclasses, so every case also goes through the
parser. dagron-state's argv becomes a single string here (BashOperator takes one),
and each assertion compares the string dagron-state compares as ``command[2]``.

The last section covers what this reader adds: refusing a model named twice, and
checking the ``graph`` argument's shape.
"""

import pytest

from freshet_airflow import (
    ContractViolation,
    EmptyPlan,
    NoCommand,
    PlannerFailed,
    compile_plan,
    plan_operators,
)


def direct(name, **extra):
    return {"name": name, "reason": "directly_changed", "unit": "full_model", **extra}


def downstream(name, because_of, **extra):
    return {
        "name": name,
        "reason": {"downstream": {"because_of": because_of, "via_columns": ["amount"]}},
        "unit": "full_model",
        **extra,
    }


def with_deps(name, deps):
    return direct(name, depends_on=list(deps))


def plan(*models):
    return {"models": list(models)}


def edges(tasks):
    return [list(t.upstream) for t in tasks]


def test_attribution_becomes_a_dependency_edge():
    tasks = compile_plan(plan(direct("stg"), downstream("mart", "stg")))
    assert list(tasks[0].upstream) == []
    assert list(tasks[1].upstream) == ["stg"]


def test_an_explicit_graph_supplies_the_edges_attribution_cannot():
    # `mart` consumes both `a` and `b`, but the planner blamed only `a`. Without a
    # graph the `b` edge is invisible: that is the documented v1 limitation.
    models = plan(direct("a"), direct("b"), downstream("mart", "a"))
    assert list(compile_plan(models)[2].upstream) == ["a"], "attribution alone sees one edge"

    exact = compile_plan(models, graph={"mart": ["a", "b"]})
    assert list(exact[2].upstream) == ["a", "b"], "the graph sees both"


def test_a_stale_graph_cannot_introduce_a_cycle():
    # A graph naming a model that comes LATER in the plan (or is absent entirely)
    # would produce an edge Airflow rejects as a cycle or cannot resolve. Both are
    # filtered out.
    tasks = compile_plan(
        plan(direct("a"), downstream("mart", "a")),
        graph={"a": ["mart", "deleted_model"]},
    )
    assert list(tasks[0].upstream) == [], "forward + unknown edges dropped"
    assert list(tasks[1].upstream) == ["a"]


def test_sequential_ordering_chains_the_whole_plan():
    tasks = compile_plan(plan(direct("a"), direct("b"), direct("c")), ordering="sequential")
    assert edges(tasks) == [[], ["a"], ["b"]]


def test_dotted_model_names_are_sanitized_and_kept_unique():
    # `.` is Airflow's TaskGroup separator (for dagron-state it was the fan-out
    # instance separator), so a schema-qualified model name would read as a task
    # nested in a group. Two names can sanitize to the same string; the second must
    # not silently become a duplicate task id.
    tasks = compile_plan(
        plan(
            direct("analytics.stg_orders"),
            direct("analytics/stg_orders"),
            direct("analytics-stg_orders"),
        )
    )
    # `.` and `/` both sanitize to `_`, so these two collide and the second is
    # suffixed. `-` is already legal in a task id and passes through, so the third
    # does NOT collide with them.
    assert [t.task_id for t in tasks] == [
        "analytics_stg_orders",
        "analytics_stg_orders_2",
        "analytics-stg_orders",
    ]
    # The unsanitized name survives in the facts, because the id no longer keys
    # back to the model.
    assert tasks[1].facts["model"] == "analytics/stg_orders"


def test_partitions_render_into_the_command_and_the_input():
    tasks = compile_plan(
        plan(
            {
                "name": "events",
                "reason": "directly_changed",
                "unit": {"partitions": ["2026-09-01", "2026-09-02"]},
            }
        )
    )
    assert tasks[0].render("build {{ model }} --parts {{ partitions }}") == (
        "build events --parts 2026-09-01,2026-09-02"
    )
    assert tasks[0].facts["unit"] == "partitions"


def test_a_full_model_rebuild_renders_empty_partitions():
    tasks = compile_plan(plan(direct("a")))
    assert tasks[0].render("b {{ unit }}/{{ partitions }}") == "b full_model/"


def test_the_reason_is_preserved_for_explainability():
    facts = compile_plan(plan(direct("stg"), downstream("mart", "stg")))[1].facts
    assert facts["reason"] == "downstream"
    assert facts["because_of"] == "stg"
    assert facts["via_columns"][0] == "amount"


def test_a_planner_failure_is_not_compiled_into_a_run():
    failed = {"models": [], "error": {"code": "parse", "message": "bad sql"}}
    with pytest.raises(PlannerFailed) as caught:
        compile_plan(failed)
    assert caught.value.code == "parse"
    assert caught.value.message == "bad sql"


def test_an_empty_plan_is_refused_distinctly_from_a_failure():
    # "Nothing to rebuild" is the planner's good outcome. It still has no run in it,
    # but the caller must be able to tell it apart from an error.
    with pytest.raises(EmptyPlan):
        compile_plan(plan())
    assert not issubclass(EmptyPlan, PlannerFailed)


def test_a_plan_with_no_command_is_refused():
    with pytest.raises(NoCommand):
        plan_operators(plan(direct("a")))
    with pytest.raises(NoCommand):
        plan_operators(plan(direct("a")), command_template="   ")


# ── contract v2: the planner supplies the edges ───────────────────────────────


def test_the_plans_own_edges_are_used_when_present():
    # No graph, no sequential, and `mart` still waits for both parents. Under v1
    # this required the caller to supply the project graph by hand.
    tasks = compile_plan(plan(with_deps("a", []), with_deps("b", []), with_deps("mart", ["a", "b"])))
    assert list(tasks[0].upstream) == []
    assert list(tasks[2].upstream) == ["a", "b"]


def test_the_plans_edges_beat_a_caller_supplied_graph():
    # The planner computed the plan and knows the project; a caller's graph is a v1
    # crutch that can be stale. When both exist the planner wins.
    tasks = compile_plan(
        plan(with_deps("a", []), with_deps("b", []), with_deps("mart", ["a", "b"])),
        graph={"mart": ["a"]},  # stale: misses b
    )
    assert list(tasks[2].upstream) == ["a", "b"], "the stale graph must not win"


def test_an_empty_edge_list_means_waits_for_nothing_not_fall_back():
    # `[]` is authoritative. If it were confused with absent, this model would fall
    # through to its because_of and gain an edge the planner denied.
    downstream_with_no_deps = {
        "name": "mart",
        "reason": {"downstream": {"because_of": "a", "via_columns": ["x"]}},
        "unit": "full_model",
        "depends_on": [],
    }
    tasks = compile_plan(plan(with_deps("a", []), downstream_with_no_deps))
    assert list(tasks[1].upstream) == [], "an explicit empty list is an answer, not a missing one"


def test_a_v1_plan_still_falls_back_through_graph_then_attribution():
    # Both legacy paths stay reachable, in order.
    models = plan(direct("a"), direct("b"), downstream("mart", "a"))
    graph = {"mart": ["a", "b"]}
    assert list(compile_plan(models, graph=graph)[2].upstream) == ["a", "b"], "graph is used when v1"
    assert list(compile_plan(models)[2].upstream) == ["a"], (
        "attribution is the last resort, and is the under-constrained one"
    )


def test_planner_edges_are_still_filtered_against_plan_position():
    # Defence in depth. The planner already restricts to plan members in topological
    # order, so this filter should be a no-op for v2, but a forward edge from a
    # malformed producer would make Airflow reject the whole DAG as cyclic.
    tasks = compile_plan(plan(with_deps("a", ["mart", "ghost"]), with_deps("mart", ["a"])))
    assert list(tasks[0].upstream) == []
    assert list(tasks[1].upstream) == ["a"]


def test_sequential_ordering_still_overrides_planner_edges():
    tasks = compile_plan(
        plan(with_deps("a", []), with_deps("b", []), with_deps("mart", ["a", "b"])),
        ordering="sequential",
    )
    assert list(tasks[2].upstream) == ["b"], "sequential chains regardless of edges"


def test_a_partial_graph_falls_through_per_model_not_per_envelope():
    # The lookup is graph.get(model), so a v1 plan whose graph covers only some
    # models is not all-or-nothing: covered models get the graph's edges, uncovered
    # ones still fall through to their attribution.
    tasks = compile_plan(
        plan(direct("a"), direct("b"), downstream("mart", "a"), downstream("other", "b")),
        graph={"mart": ["a", "b"]},  # `other` is deliberately absent
    )
    assert list(tasks[2].upstream) == ["a", "b"], "covered by the graph"
    assert list(tasks[3].upstream) == ["b"], "not in the graph, so it falls through to because_of"


# ── contract v3: replace ───────────────────────────────────────────────────────


def replaced(name, replace):
    return direct(name, replace=replace)


def insert_overwrite(col, parts):
    return {"strategy": "insert_overwrite", "partition_column": col, "target": {"partitions": list(parts)}}


def test_the_replace_placeholders_expand_to_the_declaration_not_to_sql():
    tasks = compile_plan(plan(replaced("events", insert_overwrite("dt", ["2026-06-20"]))))
    assert tasks[0].render(
        "run --select {{ model }} --strategy {{ replace }} --on {{ partition_column }}"
    ) == "run --select events --strategy insert_overwrite --on dt", (
        "the placeholder carries the planner's word, not a statement"
    )


def test_the_unique_key_placeholder_joins_on_commas_and_is_empty_without_one():
    merge = {"strategy": "merge", "unique_key": ["id", "dt"], "target": {"partitions": ["2026-06-20"]}}
    tasks = compile_plan(plan(replaced("a", merge), direct("b")))
    assert tasks[0].render("run [{{ unique_key }}]") == "run [id,dt]"
    assert tasks[1].render("run [{{ unique_key }}]") == "run []", (
        "a model with no replace op must not leave the placeholder in the command"
    )


def test_the_task_records_the_replace_facts_even_when_the_command_ignores_them():
    tasks = compile_plan(plan(replaced("events", insert_overwrite("dt", ["2026-06-20", "2026-06-21"]))))
    facts = tasks[0].facts
    assert facts["replace"] == "insert_overwrite"
    assert facts["partition_column"] == "dt"
    assert "replace_widened" not in facts, "an honoured declaration has nothing to warn about"


def test_a_widening_is_recorded_on_the_task_so_the_run_history_shows_it():
    widened = {"strategy": "full_refresh", "target": "whole", "widened": {"widened_because": "no_unique_key"}}
    facts = compile_plan(plan(replaced("events", widened)))[0].facts
    assert facts["replace"] == "full_refresh"
    assert "unique_key" in facts["replace_widened"], (
        f"the record should say why it rewrote the whole table: {facts['replace_widened']}"
    )


def test_a_v2_plan_compiles_exactly_as_before():
    for task in compile_plan(plan(direct("a"), direct("b"))):
        assert "replace" not in task.facts, "no replace key at all, not a null"
        assert "replace_widened" not in task.facts


def test_the_crate_level_example_renders_the_same_commands():
    # dagron-state's lib.rs doc example, including its last line: a model with no
    # replace op expands the placeholder to nothing rather than leaving the braces.
    tasks = compile_plan(
        plan(
            {
                "name": "stg_orders",
                "reason": "directly_changed",
                "unit": {"partitions": ["2026-06-20"]},
                "depends_on": [],
                "replace": insert_overwrite("dt", ["2026-06-20"]),
            },
            {
                "name": "mart_revenue",
                "reason": {"downstream": {"because_of": "stg_orders", "via_columns": ["amount"]}},
                "unit": "full_model",
                "depends_on": ["stg_orders"],
            },
        )
    )
    template = "dbt run --select {{ model }} --strategy {{ replace }}"
    assert list(tasks[1].upstream) == ["stg_orders"]
    assert tasks[0].render(template) == "dbt run --select stg_orders --strategy insert_overwrite"
    assert tasks[1].render(template) == "dbt run --select mart_revenue --strategy "


def test_every_widening_explains_itself_in_dagron_states_words():
    cases = {
        "not_incremental": (
            {"materialization": "view"},
            "the model is a view, so it has no partitions to replace in place",
        ),
        "whole_model_in_plan": ({}, "the plan rebuilds this model in full, so there is nothing to scope to"),
        "no_partition_column": (
            {"declared": "replace_where"},
            "`replace_where` needs a partition_column and the project declares none",
        ),
        "no_unique_key": ({}, "`merge` needs a unique_key and the project declares none"),
    }
    for because, (fields, sentence) in cases.items():
        replace = {"strategy": "full_refresh", "target": "whole", "widened": {"widened_because": because, **fields}}
        assert compile_plan(plan(replaced("m", replace)))[0].facts["replace_widened"] == sentence


# ── what this reader adds ──────────────────────────────────────────────────────


def test_a_model_named_twice_is_refused_rather_than_guessed_between():
    with pytest.raises(ContractViolation, match="appears twice"):
        compile_plan(plan(direct("a"), direct("a"), downstream("mart", "a")))


def test_a_graph_entry_given_as_a_bare_string_is_refused_not_split_into_letters():
    with pytest.raises(TypeError, match="list of model names"):
        compile_plan(plan(direct("stg"), downstream("mart", "stg")), graph={"mart": "stg"})


def test_an_unknown_ordering_is_refused():
    with pytest.raises(ValueError, match="ordering"):
        compile_plan(plan(direct("a")), ordering="parallel")


def test_a_name_with_nothing_legal_in_it_still_gets_a_task_id():
    tasks = compile_plan(plan(direct(""), direct("日本")))
    assert [t.task_id for t in tasks] == ["model", "__"]
