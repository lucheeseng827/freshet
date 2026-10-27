"""Tests for the effort-map shaping logic.

Standard library only, and no Streamlit import anywhere — that is the point of
keeping `effortmap.py` free of the display stack. Run with `python -m unittest`.
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "sdks", "python"))

import effortmap  # noqa: E402
from freshet_sdk import Plan  # noqa: E402

COLD = {
    "revision": 1,
    "token": 0,
    "plan": {
        "models": [
            {"name": "stg", "reason": "directly_changed", "unit": "full_model"},
            {
                "name": "mart_a",
                "reason": {"downstream": {"because_of": "stg", "via_columns": ["amount"]}},
                "unit": "full_model",
            },
            {
                "name": "wide",
                "reason": {"downstream": {"because_of": "stg", "via_columns": ["amount"]}},
                "unit": {"partitions": ["2026-09-01", "2026-09-02"]},
            },
        ],
        "cost": {
            "total_millicu": 2200,
            "per_model": [
                {"model": "stg", "millicu": 1000, "basis": "table_full"},
                {"model": "mart_a", "millicu": 1000, "basis": "table_full"},
                {"model": "wide", "millicu": 200,
                 "basis": {"incremental_partitions": {"count": 2}}},
            ],
        },
        "queue": {
            "items": [
                {"seq": 0, "model": "stg", "depends_on": [], "millicu": 1000, "wave": 0},
                {"seq": 1, "model": "mart_a", "depends_on": ["stg"], "millicu": 1000, "wave": 1},
                {"seq": 2, "model": "wide", "partition": "2026-09-01",
                 "depends_on": ["stg"], "millicu": 100, "wave": 1},
                {"seq": 3, "model": "wide", "partition": "2026-09-02",
                 "depends_on": ["stg"], "millicu": 100, "wave": 1},
            ],
            "waves": [[0], [1, 2, 3]],
        },
    },
}

NO_COST = {"revision": 2, "plan": {"models": [
    {"name": "solo", "reason": "directly_changed", "unit": "full_model"}]}}


class EffortRowTests(unittest.TestCase):
    def setUp(self):
        self.rows = effortmap.effort_rows(Plan.from_wire(COLD))

    def test_sorted_by_cost_descending_so_the_dominant_models_lead(self):
        self.assertEqual([r["model"] for r in self.rows], ["mart_a", "stg", "wide"])

    def test_ties_break_by_name_so_the_order_is_stable_across_reruns(self):
        # stg and mart_a both cost 1000; the cheaper `wide` sorts last regardless.
        equal = [r["model"] for r in self.rows if r["millicu"] == 1000]
        self.assertEqual(equal, sorted(equal))

    def test_origin_separates_the_change_from_its_blast_radius(self):
        by_model = {r["model"]: r for r in self.rows}
        self.assertEqual(by_model["stg"]["origin"], "changed")
        self.assertEqual(by_model["mart_a"]["origin"], "downstream")
        self.assertEqual(by_model["mart_a"]["because_of"], "stg")
        self.assertEqual(by_model["mart_a"]["via_columns"], "amount")

    def test_a_partition_scoped_model_says_how_many(self):
        wide = next(r for r in self.rows if r["model"] == "wide")
        self.assertEqual(wide["unit"], "2 partitions")
        self.assertEqual(wide["basis_label"], "partition-scoped")

    def test_a_whole_model_rebuild_says_so(self):
        stg = next(r for r in self.rows if r["model"] == "stg")
        self.assertEqual(stg["unit"], "whole model")
        self.assertEqual(stg["basis_label"], "full table rebuild")

    def test_rows_survive_a_plan_with_no_cost(self):
        rows = effortmap.effort_rows(Plan.from_wire(NO_COST))
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["cu"])
        self.assertEqual(rows[0]["basis_label"], "", "no cost, no basis — not a fabricated one")


class WaveTests(unittest.TestCase):
    def test_waves_report_their_items_and_cost(self):
        waves = effortmap.wave_rows(Plan.from_wire(COLD))
        self.assertEqual([w["wave"] for w in waves], [0, 1])
        self.assertEqual(waves[0]["items"], 1)
        self.assertEqual(waves[1]["items"], 3)
        self.assertEqual(waves[1]["models"], ["mart_a", "wide"], "distinct, sorted")

    def test_wave_costs_sum_to_the_plan_total(self):
        plan = Plan.from_wire(COLD)
        self.assertEqual(sum(w["millicu"] for w in effortmap.wave_rows(plan)),
                         plan.cost.total_millicu)

    def test_no_queue_means_no_waves_rather_than_an_error(self):
        self.assertEqual(effortmap.wave_rows(Plan.from_wire(NO_COST)), [])


class BreakdownTests(unittest.TestCase):
    def test_only_partition_scoped_models_appear(self):
        rows = effortmap.breakdown_rows(Plan.from_wire(COLD))
        self.assertEqual([r["model"] for r in rows], ["wide"])
        self.assertEqual(rows[0]["partitions"], 2)
        self.assertEqual((rows[0]["first"], rows[0]["last"]), ("2026-09-01", "2026-09-02"))

    def test_a_whole_model_rebuild_is_not_a_breakdown_of_one(self):
        self.assertEqual(effortmap.breakdown_rows(Plan.from_wire(NO_COST)), [])


class QueueTests(unittest.TestCase):
    def test_items_are_in_execution_order_and_labelled(self):
        rows = effortmap.queue_rows(Plan.from_wire(COLD))
        self.assertEqual([r["seq"] for r in rows], [0, 1, 2, 3])
        self.assertEqual(rows[2]["target"], "wide[2026-09-01]")
        self.assertEqual(rows[1]["depends_on"], "stg")

    def test_ndjson_is_one_object_per_line_and_parses(self):
        text = effortmap.queue_ndjson(Plan.from_wire(COLD))
        lines = text.splitlines()
        self.assertEqual(len(lines), 4)
        parsed = [json.loads(line) for line in lines]
        self.assertEqual([p["seq"] for p in parsed], [0, 1, 2, 3])
        self.assertEqual(parsed[2]["partition"], "2026-09-01")

    def test_ndjson_costs_sum_to_the_plan_total(self):
        plan = Plan.from_wire(COLD)
        total = sum(json.loads(l)["millicu"] for l in effortmap.queue_ndjson(plan).splitlines())
        self.assertEqual(total, plan.cost.total_millicu)


class SummaryTests(unittest.TestCase):
    def test_counts_the_change_and_its_blast_radius(self):
        s = effortmap.summary(Plan.from_wire(COLD))
        self.assertEqual((s["models"], s["directly_changed"], s["downstream"]), (3, 1, 2))
        self.assertEqual((s["work_items"], s["depth"], s["width"]), (4, 2, 3))
        self.assertAlmostEqual(s["total_cu"], 2.2)

    def test_absent_cost_is_none_not_zero(self):
        # A zero would read as "this rebuild is free", which is a different claim
        # from "nobody asked what it costs".
        s = effortmap.summary(Plan.from_wire(NO_COST))
        self.assertIsNone(s["total_cu"])
        self.assertIsNone(s["total_millicu"])
        self.assertIsNone(s["work_items"])


class SourcesTests(unittest.TestCase):
    def test_parses_a_source_map(self):
        self.assertEqual(effortmap.sources_from_json('{"raw": ["a", "b"]}'), {"raw": ["a", "b"]})

    def test_an_empty_box_is_an_empty_map_not_an_error(self):
        self.assertEqual(effortmap.sources_from_json("   "), {})

    def test_a_json_array_is_refused_with_a_useful_message(self):
        with self.assertRaises(ValueError) as ctx:
            effortmap.sources_from_json('["raw"]')
        self.assertIn("JSON object", str(ctx.exception))

    def test_a_bare_string_column_list_is_refused_not_split_into_letters(self):
        # `list("id")` is `["i", "d"]`: two columns nobody has, and no error to notice.
        with self.assertRaises(ValueError) as ctx:
            effortmap.sources_from_json('{"raw": "id"}')
        self.assertIn("raw", str(ctx.exception))

    def test_a_non_iterable_value_is_a_value_error_not_a_type_error(self):
        # TypeError is the one the app does not catch, so it reaches the page raw.
        with self.assertRaises(ValueError):
            effortmap.sources_from_json('{"raw": 3}')

    def test_a_list_of_non_strings_is_refused(self):
        with self.assertRaises(ValueError):
            effortmap.sources_from_json('{"raw": ["id", 7]}')


class MaterializationTests(unittest.TestCase):
    def test_parses_a_materialization_map(self):
        self.assertEqual(effortmap.materializations_from_json('{"events": "incremental"}'),
                         {"events": "incremental"})

    def test_an_empty_box_is_an_empty_map_not_an_error(self):
        self.assertEqual(effortmap.materializations_from_json(" \n "), {})

    def test_a_value_that_is_not_a_name_is_refused(self):
        # The sources parser would have turned this into eleven single-letter
        # materializations; here the shapes are checked separately for that reason.
        with self.assertRaises(ValueError) as ctx:
            effortmap.materializations_from_json('{"events": ["incremental"]}')
        self.assertIn("events", str(ctx.exception))

    def test_a_json_array_is_refused_with_a_useful_message(self):
        with self.assertRaises(ValueError) as ctx:
            effortmap.materializations_from_json('["events"]')
        self.assertIn("JSON object", str(ctx.exception))


class PaletteTests(unittest.TestCase):
    def test_both_modes_define_both_series(self):
        for dark in (False, True):
            colors = effortmap.series_colors(dark)
            self.assertEqual(set(colors), {"direct", "downstream"})
            for hex_value in colors.values():
                self.assertRegex(hex_value, r"^#[0-9a-f]{6}$")

    def test_the_two_modes_are_stepped_differently(self):
        # The dark column is the same hues re-stepped for the dark surface, not the
        # light palette reused — reusing it is what fails the contrast check.
        self.assertNotEqual(effortmap.series_colors(False), effortmap.series_colors(True))

    def test_the_category_order_is_fixed(self):
        # A fixed domain is what stops a filter that drops one category from
        # repainting the other.
        self.assertEqual(tuple(effortmap.origin_domain()), ("changed", "downstream"))


if __name__ == "__main__":
    unittest.main()
