"""The wire contract, read by a second consumer: a port of dagron-state's ``tests/wire_contract.rs``.

Every fixture here except the v4 one is a byte-for-byte copy of a constant in
dagron's ``crates/dagron-state/tests/wire_contract.rs``, and the last test in this
file fails if a copy drifts. Two consumers agreeing on one contract means agreeing
on the same bytes, so these are not re-typed from memory and are not edited to make
a test pass: if one of these tests fails, the contract moved, and ``plan.py`` (and
``WIRE_CONTRACT_VERSION``) move with it.
"""

import json
import os
import pathlib
import re

import pytest

from freshet_airflow import (
    WIRE_CONTRACT_VERSION,
    ContractViolation,
    compile_plan,
    parse_plan,
)

FIXTURES = pathlib.Path(__file__).with_name("fixtures")

# fixture file -> the constant in dagron-state's wire_contract.rs it was copied from
COPIED_FROM_DAGRON_STATE = {
    "golden.json": "GOLDEN",
    "captured_v2_fan_in.json": "CAPTURED_V2_FAN_IN",
    "captured_v3_with_a_widening.json": "CAPTURED_V3_WITH_A_WIDENING",
    "captured_from_planner_cli.json": "CAPTURED_FROM_PLANNER_CLI",
}

# A dagron checkout, when there is one to compare against (CI sets it); the check
# skips without it.
DAGRON_STATE_WIRE_TESTS = (
    pathlib.Path(os.environ.get("FRESHET_DAGRON_DIR", "/nonexistent"))
    / "crates"
    / "dagron-state"
    / "tests"
    / "wire_contract.rs"
)


def fixture(name):
    return (FIXTURES / name).read_text()


def model(plan, name):
    return next(m for m in plan.models if m.name == name)


def test_a_v3_plan_carries_what_each_rebuild_does_to_the_relation():
    plan = parse_plan(fixture("captured_v3_with_a_widening.json"))

    r = model(plan, "events").replace
    assert r is not None, "a declared model carries its op"
    assert r.strategy == "insert_overwrite"
    assert r.partition_column == "dt"
    assert r.target_partitions == ("2026-06-20",)
    assert r.widened is None

    # merge scopes on a key, so it needs no partition column and is not widened.
    r = model(plan, "daily_rollup").replace
    assert r.strategy == "merge"
    assert r.unique_key == ("dt",)
    assert r.partition_column is None
    assert r.widened is None

    r = model(plan, "user_dim").replace
    assert r.strategy == "full_refresh"
    assert r.target_partitions is None, "`whole` is the whole relation"
    assert r.widened is not None, "a widened op must say why"
    assert r.widened.because == "not_incremental"
    assert r.widened.materialization == "table"


def test_a_widening_explains_itself_in_one_actionable_sentence():
    plan = parse_plan(fixture("captured_v3_with_a_widening.json"))
    said = model(plan, "user_dim").replace.widened.explain()
    assert "table" in said, f"it should name the materialization: {said}"
    assert "no partitions to replace in place" in said, f"and say what that means for the write: {said}"


def test_a_v2_plan_reports_no_replace_and_that_is_not_an_error():
    # The forward-compatibility direction that matters: a producer that predates v3
    # keeps working, and its silence reads as "no replace-specific behaviour" rather
    # than as a parse failure or a guessed default.
    plan = parse_plan(fixture("captured_v2_fan_in.json"))
    assert all(m.replace is None for m in plan.models)
    assert all(m.depends_on is not None for m in plan.models), "while its v2 half still parses as v2"


def test_a_v2_plan_carries_edges_the_attribution_cannot_express():
    plan = parse_plan(fixture("captured_v2_fan_in.json"))

    mart = model(plan, "mart_revenue")
    assert mart.reason == "downstream"
    assert mart.because_of == "dim_customer", "the attribution names ONE parent"
    assert mart.depends_on == ("dim_customer", "stg_orders"), (
        "but the model waits for BOTH, which is the gap v2 closes"
    )

    # Roots say so authoritatively, rather than being indistinguishable from v1.
    assert model(plan, "stg_orders").depends_on == ()


def test_a_v2_fan_in_compiles_to_both_edges_without_a_caller_supplied_graph():
    # The end-to-end payoff: no graph, no sequential, and the DAG is still exact.
    # Under v1 this same plan produced a single edge.
    tasks = {t.task_id: t for t in compile_plan(fixture("captured_v2_fan_in.json"))}
    assert tasks["mart_revenue"].upstream == ("dim_customer", "stg_orders")


def test_a_v1_plan_still_parses_and_reports_no_edges():
    # Backward compatibility is the reason depends_on is Optional: a v1 payload must
    # not look like a v2 plan whose models happen to wait for nothing.
    plan = parse_plan(fixture("golden.json"))
    for m in plan.models:
        assert m.depends_on is None, f"{m.name} must report absent, not empty"


def test_the_golden_plan_deserializes():
    plan = parse_plan(fixture("golden.json"))

    assert len(plan.models) == 2
    assert plan.error is None
    assert plan.next_state is not None, "the snapshot is carried through opaquely"
    assert plan.next_state == {"fingerprints": {"stg_orders": 12345, "mart_revenue": 67890}}

    first, second = plan.models
    assert first.reason == "directly_changed"
    assert first.unit == "full_model"

    assert second.reason == "downstream"
    assert second.because_of == "stg_orders"
    assert second.via_columns == ("amount", "qty")
    assert second.unit == "partitions"
    assert second.partitions == ("2026-09-01", "2026-09-02")


def test_a_cold_plan_without_state_or_error_parses():
    # Both optional fields are skip_serializing_if upstream, so a minimal plan omits
    # them entirely rather than sending nulls.
    plan = parse_plan('{"models":[{"name":"a","reason":"directly_changed","unit":"full_model"}]}')
    assert len(plan.models) == 1
    assert plan.next_state is None


def test_an_empty_plan_parses_as_success_not_failure():
    plan = parse_plan('{"models":[]}')
    assert plan.models == ()
    assert plan.error is None


def test_the_golden_plan_compiles_end_to_end():
    tasks = compile_plan(fixture("golden.json"))
    assert len(tasks) == 2
    assert tasks[1].upstream == ("stg_orders",)
    assert tasks[1].task_id == "mart_revenue"


def test_the_contract_version_is_stamped():
    assert WIRE_CONTRACT_VERSION == "planner-embed/5"


def test_real_planner_output_compiles_into_a_run_graph():
    tasks = compile_plan(fixture("captured_from_planner_cli.json"))

    assert len(tasks) == 2
    assert tasks[0].task_id == "stg_orders"
    # The planner's column-level attribution became a real dependency edge.
    assert tasks[1].upstream == ("stg_orders",)
    assert tasks[1].facts["via_columns"][0] == "amount"


# ── strictness: the same payloads dagron-state's serde accepts, and no others ───


@pytest.mark.parametrize(
    "payload, where",
    [
        ('{"models":[{"name":"a","reason":"renamed","unit":"full_model"}]}', "models[0].reason"),
        ('{"models":[{"name":"a","reason":"downstream","unit":"full_model"}]}', "models[0].reason"),
        ('{"models":[{"name":"a","reason":"directly_changed","unit":"whole"}]}', "models[0].unit"),
        ('{"models":[{"name":"a","unit":"full_model"}]}', "models[0]"),
        ('{"models":[{"reason":"directly_changed","unit":"full_model"}]}', "models[0]"),
        (
            '{"models":[{"name":"a","reason":"directly_changed","unit":"full_model",'
            '"replace":{"strategy":"upsert","target":"whole"}}]}',
            "models[0].replace.strategy",
        ),
        (
            '{"models":[{"name":"a","reason":"directly_changed","unit":"full_model",'
            '"replace":{"strategy":"merge","target":"whole",'
            '"widened":{"widened_because":"felt_like_it"}}}]}',
            "models[0].replace.widened.widened_because",
        ),
        ('{"models":[{"name":"a","reason":"directly_changed","unit":"full_model","depends_on":"b"}]}',
         "models[0].depends_on"),
        ('{"models": null}', "plan.models"),
        ('[1, 2]', "plan"),
        ("{not json", "not valid JSON"),
    ],
)
def test_a_payload_the_contract_does_not_define_is_refused_with_the_path_to_it(payload, where):
    with pytest.raises(ContractViolation, match=re.escape(where)):
        parse_plan(payload)


def test_an_unknown_variant_says_the_contract_moved():
    with pytest.raises(ContractViolation, match="contract moved"):
        parse_plan('{"models":[{"name":"a","reason":"renamed","unit":"full_model"}]}')


def test_unknown_fields_are_ignored_because_every_revision_so_far_is_additive():
    # How a reader of this revision keeps working against the next one, and how
    # dagron-state's reader keeps working against v4 today: cost, queue and headroom
    # are real response fields that neither reader consumes.
    plan = parse_plan(
        '{"models":[{"name":"a","reason":"directly_changed","unit":"full_model",'
        '"some_future_field":{"x":1}}],"cost":{"total_millicu":1000,"per_model":[]},'
        '"headroom":{"cap_millicu":1,"used_millicu":1,"remaining_millicu":0,"exceeded":false}}'
    )
    assert [m.name for m in plan.models] == ["a"]


def test_null_optionals_read_as_absent_exactly_as_serde_reads_them():
    plan = parse_plan(
        '{"models":[{"name":"a","reason":"directly_changed","unit":"full_model",'
        '"depends_on":null,"replace":null,"sql":null}],"next_state":null,"error":null}'
    )
    only = plan.models[0]
    assert only.depends_on is None, "null is absent (v1 fallback), never an empty edge list"
    assert only.replace is None and only.sql is None
    assert plan.next_state is None and plan.error is None


def test_bytes_and_decoded_mappings_are_the_same_plan_as_text():
    text = fixture("captured_v2_fan_in.json")
    assert parse_plan(text.encode()) == parse_plan(text) == parse_plan(json.loads(text))


def test_the_fixtures_are_still_byte_for_byte_what_dagron_state_tests_against():
    # The guard that makes "two consumers, one contract" mechanical rather than a
    # promise: if dagron-state's copy of a fixture changes, this copy must follow.
    if not DAGRON_STATE_WIRE_TESTS.exists():
        pytest.skip("dagron-state's sources are not beside this package (installed standalone)")
    source = DAGRON_STATE_WIRE_TESTS.read_text()
    consts = dict(re.findall(r'const ([A-Z0-9_]+): &str = r#"(.*?)"#;', source, re.S))
    assert set(COPIED_FROM_DAGRON_STATE.values()) <= set(consts), (
        f"dagron-state renamed or dropped a fixture: it now has {sorted(consts)}"
    )
    for name, const in COPIED_FROM_DAGRON_STATE.items():
        assert fixture(name) == consts[const] + "\n", f"{name} has drifted from {const}"
