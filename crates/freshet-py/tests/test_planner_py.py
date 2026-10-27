"""The Python half of the cross-binding golden test, and the wheel's own contract.

``freshet-embed`` promises that every binding answers a request identically. The
native Rust build (``crates/freshet-wasm/tests/golden.rs``), the WebAssembly build
(``apps/effortmap-web/test/wasm.test.mjs``) and the C ABI
(``crates/freshet-ffi/tests/c/run.sh``) are each pinned to one fixture; this pins
the wheel to the same one.

Runs against the built extension, not the source tree::

    cd crates/freshet-py
    maturin develop            # or: pip install target/wheels/freshet_planner-*.whl
    python -m unittest discover -s tests -v

Standard library only.
"""

import json
import pathlib
import unittest

import freshet_planner

GOLDEN = pathlib.Path(__file__).resolve().parents[3] / "apps" / "effortmap-web" / "test" / "golden"


class GoldenTest(unittest.TestCase):
    def test_the_wheel_reproduces_the_golden_response(self):
        request = (GOLDEN / "request.json").read_text()
        expected = json.loads((GOLDEN / "response.json").read_text())
        # Compared as parsed JSON: the fixture is pretty-printed, the binding compact;
        # neither layout nor key order is the contract.
        self.assertEqual(json.loads(freshet_planner.plan(request)), expected)

    def test_plan_dict_agrees_with_plan(self):
        request = json.loads((GOLDEN / "request.json").read_text())
        self.assertEqual(freshet_planner.plan_dict(request), json.loads(freshet_planner.plan(json.dumps(request))))


class ErrorTest(unittest.TestCase):
    def test_plan_returns_an_error_response_verbatim(self):
        # The cross-binding surface never raises for a planner error: it answers with
        # the same `error` field every other binding does.
        response = json.loads(freshet_planner.plan("not json"))
        self.assertEqual(response["error"]["code"], "bad_request")
        self.assertEqual(response["models"], [])

    def test_plan_dict_raises_planner_error_with_code_and_response(self):
        with self.assertRaises(freshet_planner.PlannerError) as caught:
            freshet_planner.plan_dict({"models": [{"name": "m", "sql": "selec 1"}]})
        err = caught.exception
        self.assertEqual(err.code, "plan_error")
        self.assertIn("m", err.message)
        self.assertEqual(err.response["error"]["code"], "plan_error")
        self.assertTrue(str(err).startswith("plan_error: "))

    def test_a_render_error_is_a_planner_error_too(self):
        with self.assertRaises(freshet_planner.PlannerError) as caught:
            freshet_planner.plan_dict(
                {"models": [{"name": "m", "sql": "select 1 as x"}], "render": {"dialect": "oracle"}}
            )
        self.assertEqual(caught.exception.code, "render_error")

    def test_planner_error_is_an_exception(self):
        self.assertTrue(issubclass(freshet_planner.PlannerError, Exception))

    def test_a_non_dict_is_rejected_at_the_boundary(self):
        with self.assertRaises(TypeError):
            freshet_planner.plan_dict(["not", "a", "dict"])


class SurfaceTest(unittest.TestCase):
    def test_the_contract_version_is_exposed(self):
        self.assertEqual(freshet_planner.WIRE_CONTRACT_VERSION, "planner-embed/5")

    def test_a_plan_can_carry_rendered_sql(self):
        response = freshet_planner.plan_dict(
            {"models": [{"name": "m", "sql": "select 1 as x"}], "render": {"dialect": "postgres"}}
        )
        (model,) = response["models"]
        self.assertEqual(model["sql"]["dialect"], "postgres")
        self.assertTrue(model["sql"]["statements"])


if __name__ == "__main__":
    unittest.main()
