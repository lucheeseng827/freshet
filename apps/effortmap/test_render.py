"""Tests for the renderer seam.

Standard library only, and none of these import streamlit, pandas or altair — which is
the property under test as much as anything else: choosing a renderer must not drag in
the cost of the one you did not choose.

Run with `python -m unittest`.
"""

import io
import json
import unittest

import render
from freshet_sdk import Plan

COLD = {
    "revision": 1,
    "token": 0,
    "plan": {
        "models": [
            {"name": "stg", "reason": "directly_changed", "unit": "full_model"},
            {"name": "mart", "reason": {"downstream": {"because_of": "stg", "via_columns": ["amount"]}},
             "unit": {"partitions": ["2026-09-01", "2026-09-02"]}},
        ],
        "cost": {
            "total_millicu": 1200,
            "per_model": [
                {"model": "stg", "millicu": 1000, "basis": "table_full"},
                {"model": "mart", "millicu": 200, "basis": {"incremental_partitions": {"count": 2}}},
            ],
        },
        "queue": {
            "items": [
                {"seq": 0, "model": "stg", "depends_on": [], "millicu": 1000, "wave": 0},
                {"seq": 1, "model": "mart", "partition": "2026-09-01", "depends_on": ["stg"],
                 "millicu": 100, "wave": 1},
                {"seq": 2, "model": "mart", "partition": "2026-09-02", "depends_on": ["stg"],
                 "millicu": 100, "wave": 1},
            ],
            "waves": [[0], [1, 2]],
        },
    },
}
EMPTY = {"revision": 4, "plan": {"models": []}}


def draw(wire, to, **opts):
    buf = io.StringIO()
    render.render(Plan.from_wire(wire), to=to, out=buf, **opts)
    return buf.getvalue()


class RegistryTests(unittest.TestCase):
    def test_the_stdlib_renderers_need_nothing_installed(self):
        stdlib = {n for n, r in render.RENDERERS.items() if r.stdlib}
        self.assertEqual(stdlib, {"text", "html", "json", "ndjson"})

    def test_streamlit_is_declared_as_needing_extras(self):
        # The seam's whole purpose: the interactive renderer is one option, and the
        # registry says out loud that it costs an install.
        self.assertFalse(render.RENDERERS["streamlit"].stdlib)

    def test_every_renderer_carries_help_for_the_cli(self):
        for name, r in render.RENDERERS.items():
            self.assertTrue(r.help.strip(), f"{name} has no help text")

    def test_an_unknown_renderer_names_the_known_ones(self):
        with self.assertRaises(KeyError) as ctx:
            draw(COLD, "crayon")
        self.assertIn("text", str(ctx.exception))


class TextTests(unittest.TestCase):
    def test_reports_the_headline_and_every_model(self):
        out = draw(COLD, "text")
        self.assertIn("1.20 CU", out)
        self.assertIn("stg", out)
        self.assertIn("mart", out)
        self.assertIn("2 partitions", out)

    def test_shows_attribution_under_the_model_it_belongs_to(self):
        out = draw(COLD, "text")
        self.assertIn("pulled in by stg via amount", out)

    def test_an_empty_plan_says_so_instead_of_printing_an_empty_table(self):
        out = draw(EMPTY, "text")
        self.assertIn("Nothing to rebuild", out)
        self.assertNotIn("MODEL", out)

    def test_the_unit_caveat_travels_with_the_number(self):
        # A cost figure that leaves without it eventually gets read as minutes.
        self.assertIn("1 CU is one full rebuild", draw(COLD, "text"))

    def test_a_restatement_is_described_as_restated_not_changed(self):
        restated = json.loads(json.dumps(COLD))
        restated["token"] = None
        self.assertIn("restated", draw(restated, "text"))
        self.assertIn("changed directly", draw(COLD, "text"))


class JsonTests(unittest.TestCase):
    def test_emits_every_shaped_section(self):
        d = json.loads(draw(COLD, "json"))
        self.assertEqual(sorted(d), ["breakdown", "models", "queue", "summary", "unit_note", "waves"])
        self.assertEqual(len(d["models"]), 2)
        self.assertEqual(d["summary"]["total_millicu"], 1200)

    def test_ndjson_is_one_work_item_per_line(self):
        lines = draw(COLD, "ndjson").strip().split("\n")
        self.assertEqual(len(lines), 3)
        self.assertEqual(json.loads(lines[1])["partition"], "2026-09-01")


class HtmlTests(unittest.TestCase):
    def setUp(self):
        self.out = draw(COLD, "html")

    def test_is_a_whole_document(self):
        self.assertTrue(self.out.startswith("<!doctype html>"))
        self.assertIn("</html>", self.out)

    def test_fetches_nothing_and_runs_nothing(self):
        # The point of this renderer: a file you can mail, commit, or attach to a CI
        # run, that renders identically with no network and no scripting.
        for tag in ("<script", "<link", "<img", "http://", "https://"):
            self.assertNotIn(tag, self.out, f"{tag} would make this file depend on something")

    def test_draws_the_bars_as_inline_svg(self):
        self.assertIn("<svg", self.out)
        self.assertIn("compute units (CU)", self.out)

    def test_carries_both_the_display_and_the_exact_figure(self):
        self.assertIn("1.20", self.out)   # CU, for reading
        self.assertIn("1000", self.out)   # mCU, exact

    def test_escapes_content_that_would_otherwise_be_markup(self):
        hostile = json.loads(json.dumps(COLD))
        hostile["plan"]["models"][0]["name"] = "<script>alert(1)</script>"
        hostile["plan"]["cost"]["per_model"][0]["model"] = "<script>alert(1)</script>"
        out = draw(hostile, "html")
        self.assertNotIn("<script>alert(1)</script>", out)
        self.assertIn("&lt;script&gt;", out)

    def test_defines_both_themes(self):
        self.assertIn("prefers-color-scheme: dark", self.out.replace("prefers-color-scheme:dark",
                                                                     "prefers-color-scheme: dark"))

    def test_an_empty_plan_still_produces_a_valid_page(self):
        out = draw(EMPTY, "html")
        self.assertTrue(out.startswith("<!doctype html>"))
        self.assertIn("Nothing to rebuild", out)


if __name__ == "__main__":
    unittest.main()
