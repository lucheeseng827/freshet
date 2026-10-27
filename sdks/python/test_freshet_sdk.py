"""Tests for the planner Python SDK — wire parsing + a real-socket client.

Standard-library only. The :class:`Client` tests run against a threaded
``http.server`` fake daemon that records each request and returns canned responses,
so we exercise actual URL/method/body construction rather than mocks. The canned
bodies are the shapes ``freshet-serve`` really emits — see
``crates/freshet-serve/tests/http.rs``, which asserts the same shapes off a real
socket from the Rust side.

Run with ``python -m unittest``.
"""

import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from freshet_sdk import (
    Client,
    CostEstimate,
    Plan,
    PlannerError,
    Queue,
    WorkItem,
    load_project,
)


# ── wire parsing ──────────────────────────────────────────────────────────────

# A cold plan over stg -> {mart_a, mart_b}: three table models, one CU each.
COLD_PLAN = {
    "status": "plan",
    "revision": 1,
    "token": 0,
    "plan": {
        "models": [
            {"name": "stg", "reason": "directly_changed", "unit": "full_model"},
            {
                "name": "mart_a",
                "reason": {"downstream": {"because_of": "stg", "via_columns": ["a"]}},
                "unit": "full_model",
            },
            {
                "name": "mart_b",
                "reason": {"downstream": {"because_of": "stg", "via_columns": ["b"]}},
                "unit": "full_model",
            },
        ],
        "cost": {
            "total_millicu": 3000,
            "per_model": [
                {"model": "stg", "millicu": 1000, "basis": "table_full"},
                {"model": "mart_a", "millicu": 1000, "basis": "table_full"},
                {"model": "mart_b", "millicu": 1000, "basis": "table_full"},
            ],
        },
        "queue": {
            "items": [
                {"seq": 0, "model": "stg", "depends_on": [], "millicu": 1000, "wave": 0},
                {"seq": 1, "model": "mart_a", "depends_on": ["stg"], "millicu": 1000, "wave": 1},
                {"seq": 2, "model": "mart_b", "depends_on": ["stg"], "millicu": 1000, "wave": 1},
            ],
            "waves": [[0], [1, 2]],
        },
    },
}

# A restatement of three partitions: no token, partition-scoped units, 100 mCU each.
RESTATEMENT = {
    "status": "plan",
    "revision": 4,
    "token": None,
    "plan": {
        "models": [
            {
                "name": "events",
                "reason": "directly_changed",
                "unit": {"partitions": ["2026-09-01", "2026-09-02", "2026-09-03"]},
            }
        ],
        "cost": {
            "total_millicu": 300,
            "per_model": [
                {
                    "model": "events",
                    "millicu": 300,
                    "basis": {"incremental_partitions": {"count": 3}},
                }
            ],
        },
        "queue": {
            "items": [
                {"seq": 0, "model": "events", "partition": "2026-09-01",
                 "depends_on": [], "millicu": 100, "wave": 0},
                {"seq": 1, "model": "events", "partition": "2026-09-02",
                 "depends_on": [], "millicu": 100, "wave": 0},
                {"seq": 2, "model": "events", "partition": "2026-09-03",
                 "depends_on": [], "millicu": 100, "wave": 0},
            ],
            "waves": [[0, 1, 2]],
        },
    },
}


class PlanParsingTests(unittest.TestCase):
    def test_reads_models_reasons_and_attribution(self):
        plan = Plan.from_wire(COLD_PLAN)
        self.assertEqual([m.name for m in plan.models], ["stg", "mart_a", "mart_b"])
        self.assertTrue(plan.models[0].directly_changed)
        self.assertIsNone(plan.models[0].because_of)
        self.assertEqual(plan.models[1].because_of, "stg")
        self.assertEqual(plan.models[1].via_columns, ["a"])
        self.assertIsNone(plan.models[1].partitions)

    def test_cost_converts_millicu_to_cu_without_losing_the_exact_value(self):
        plan = Plan.from_wire(COLD_PLAN)
        self.assertEqual(plan.cost.total_millicu, 3000)
        self.assertAlmostEqual(plan.total_cu, 3.0)
        self.assertEqual(sum(m.millicu for m in plan.cost.per_model), 3000)

    def test_a_view_sized_cost_does_not_round_to_zero(self):
        cost = CostEstimate.from_wire(
            {"total_millicu": 50, "per_model": [{"model": "v", "millicu": 50, "basis": "view_full"}]}
        )
        self.assertEqual(cost.total_millicu, 50)
        self.assertAlmostEqual(cost.total_cu, 0.05)

    def test_partition_basis_exposes_its_count(self):
        plan = Plan.from_wire(RESTATEMENT)
        cost = plan.cost.per_model[0]
        self.assertEqual(cost.basis_name, "incremental_partitions")
        self.assertEqual(cost.partition_count, 3)
        self.assertEqual(plan.models[0].partitions, ["2026-09-01", "2026-09-02", "2026-09-03"])

    def test_a_whole_model_basis_has_no_partition_count(self):
        plan = Plan.from_wire(COLD_PLAN)
        self.assertEqual(plan.cost.per_model[0].basis_name, "table_full")
        self.assertIsNone(plan.cost.per_model[0].partition_count)

    def test_queue_reports_depth_and_width(self):
        queue = Plan.from_wire(COLD_PLAN).queue
        self.assertEqual(queue.depth, 2, "stg, then the marts")
        self.assertEqual(queue.width, 2, "the two marts run together")
        self.assertEqual(queue.total_millicu, 3000)

    def test_wave_items_groups_the_queue_for_a_runner(self):
        waves = Plan.from_wire(COLD_PLAN).queue.wave_items()
        self.assertEqual([[i.model for i in w] for w in waves], [["stg"], ["mart_a", "mart_b"]])

    def test_a_partitioned_item_labels_itself_with_its_partition(self):
        queue = Plan.from_wire(RESTATEMENT).queue
        self.assertEqual([i.label for i in queue.items],
                         ["events[2026-09-01]", "events[2026-09-02]", "events[2026-09-03]"])
        self.assertEqual(queue.depth, 1, "one model, one wave")
        self.assertEqual(queue.width, 3, "its three partitions are independent")

    def test_item_shares_sum_to_the_model_cost(self):
        # The invariant freshet-embed guarantees; asserted here too because the SDK
        # renders both numbers side by side and a mismatch would look like the
        # planner disagreeing with itself.
        plan = Plan.from_wire(RESTATEMENT)
        self.assertEqual(plan.queue.total_millicu, plan.cost.total_millicu)

    def test_heaviest_is_stable_under_ties(self):
        cost = CostEstimate.from_wire(
            {
                "total_millicu": 3000,
                "per_model": [
                    {"model": "b", "millicu": 1000, "basis": "table_full"},
                    {"model": "a", "millicu": 1000, "basis": "table_full"},
                    {"model": "c", "millicu": 1000, "basis": "table_full"},
                ],
            }
        )
        self.assertEqual([m.model for m in cost.heaviest(2)], ["a", "b"])

    def test_a_plan_without_cost_says_so_rather_than_returning_zero(self):
        plan = Plan.from_wire({"revision": 1, "plan": {"models": []}})
        self.assertIsNone(plan.cost)
        self.assertIsNone(plan.queue)
        with self.assertRaises(PlannerError) as ctx:
            _ = plan.total_cu
        self.assertIn("include_cost", str(ctx.exception))

    def test_an_empty_plan_is_falsy(self):
        self.assertFalse(Plan.from_wire({"revision": 9, "plan": {"models": []}}))
        self.assertTrue(Plan.from_wire(COLD_PLAN))

    def test_an_item_without_a_partition_is_a_whole_model_rebuild(self):
        item = WorkItem.from_wire({"seq": 0, "model": "m", "depends_on": [], "millicu": 1000, "wave": 0})
        self.assertIsNone(item.partition)
        self.assertEqual(item.label, "m")

    def test_an_absent_queue_section_parses_as_empty_not_as_an_error(self):
        queue = Queue.from_wire({})
        self.assertEqual(queue.items, [])
        self.assertEqual(queue.depth, 0)
        self.assertEqual(queue.width, 0)


# ── the client, against a real socket ─────────────────────────────────────────


class _Handler(BaseHTTPRequestHandler):
    """Records every request on the server, replies with whatever it was primed with."""

    def do_POST(self):  # noqa: N802 (BaseHTTPRequestHandler's spelling)
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode("utf-8")
        self.server.requests.append(
            {"path": self.path, "method": "POST", "body": json.loads(raw) if raw else None,
             "content_type": self.headers.get("Content-Type")}
        )
        self._reply(self.server.next_response)

    def do_GET(self):  # noqa: N802
        self.server.requests.append({"path": self.path, "method": "GET", "body": None})
        self._reply(self.server.next_response)

    def _reply(self, body):
        # freshet-serve answers 200 even for planner errors; the failure is in the
        # document. `http_status` lets a test force a real transport-level failure,
        # and `response_delay` lets one stall past the client's timeout.
        if self.server.response_delay:
            time.sleep(self.server.response_delay)
        payload = json.dumps(body).encode("utf-8")
        try:
            self.send_response(self.server.http_status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except OSError:
            # The timeout test hangs up in the middle of this reply on purpose, so
            # writing into a socket the client already abandoned is the expected end
            # of that exchange rather than a failure worth a stderr traceback.
            pass

    def log_message(self, *args):
        pass  # keep the test output readable


class DaemonTestCase(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.server.requests = []
        self.server.next_response = {"status": "ok"}
        self.server.http_status = 200
        self.server.response_delay = 0
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.client = Client(f"http://127.0.0.1:{self.server.server_address[1]}", timeout=5)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    @property
    def last(self):
        return self.server.requests[-1]


class ClientConstructionTests(unittest.TestCase):
    def test_rejects_a_non_http_scheme(self):
        for bad in ("ftp://host", "file:///etc/passwd", "not-a-url"):
            with self.assertRaises(ValueError):
                Client(bad)

    def test_strips_a_trailing_slash_so_paths_do_not_double_up(self):
        self.assertEqual(Client("http://h:1/").base_url, "http://h:1")


class ClientPlanTests(DaemonTestCase):
    def test_plan_posts_to_rpc_and_asks_for_cost_by_default(self):
        self.server.next_response = COLD_PLAN
        plan = self.client.plan()

        self.assertEqual(self.last["path"], "/rpc")
        self.assertEqual(self.last["content_type"], "application/json")
        self.assertEqual(self.last["body"]["op"], "plan")
        self.assertTrue(self.last["body"]["include_cost"], "the SDK's default")
        self.assertFalse(self.last["body"]["include_queue"], "the expensive half stays off")
        self.assertAlmostEqual(plan.total_cu, 3.0)
        self.assertEqual(plan.token, 0)

    def test_the_queue_is_requested_only_when_asked_for(self):
        self.server.next_response = COLD_PLAN
        self.client.plan(include_queue=True)
        self.assertTrue(self.last["body"]["include_queue"])

    def test_restate_sends_the_partitions_and_expects_no_token(self):
        self.server.next_response = RESTATEMENT
        plan = self.client.restate("events", ["2026-09-01", "2026-09-02", "2026-09-03"])

        self.assertEqual(self.last["body"]["op"], "restate")
        self.assertEqual(self.last["body"]["model"], "events")
        self.assertEqual(len(self.last["body"]["partitions"]), 3)
        self.assertIsNone(plan.token)

    def test_reload_sends_models_as_sorted_pairs(self):
        self.server.next_response = {"status": "reloaded", "revision": 7, "models": 2}
        revision = self.client.reload(
            {"b": "select 1", "a": "select 2"},
            sources={"raw": ["x"]},
            materializations={"a": "incremental"},
        )
        self.assertEqual(revision, 7)
        # Ordered pairs, not a map: dict order is not a contract and the project's
        # model order reaches the plan's output.
        self.assertEqual(self.last["body"]["models"], [["a", "select 2"], ["b", "select 1"]])
        self.assertEqual(self.last["body"]["sources"], {"raw": ["x"]})
        self.assertEqual(self.last["body"]["materializations"], {"a": "incremental"})

    def test_a_source_given_as_a_bare_string_is_refused_not_spelled_out(self):
        # list("id") is ["i", "d"]: two columns that exist nowhere, which the planner
        # would resolve against and answer wrongly but confidently. Refuse before the
        # request leaves.
        before = len(self.server.requests)
        with self.assertRaises(ValueError) as ctx:
            self.client.reload({"a": "select 1"}, sources={"raw": "id"})
        self.assertIn("raw", str(ctx.exception))
        self.assertEqual(len(self.server.requests), before, "nothing should have been sent")

    def test_a_source_column_that_is_not_a_string_is_refused(self):
        with self.assertRaises(ValueError):
            self.client.reload({"a": "select 1"}, sources={"raw": ["id", 7]})
        with self.assertRaises(ValueError):
            self.client.reload({"a": "select 1"}, sources={"raw": None})

    def test_any_non_string_sequence_of_columns_is_still_accepted(self):
        self.server.next_response = {"status": "reloaded", "revision": 1, "models": 1}
        self.client.reload({"a": "select 1"}, sources={"raw": ("id", "amount")})
        self.assertEqual(self.last["body"]["sources"], {"raw": ["id", "amount"]})

    def test_graph_returns_the_topological_order(self):
        self.server.next_response = {"status": "graph", "revision": 1, "order": ["stg", "mart"]}
        self.assertEqual(self.client.graph(), ["stg", "mart"])

    def test_commit_sends_the_token(self):
        self.server.next_response = {"status": "committed", "revision": 2}
        self.assertEqual(self.client.commit(0), 2)
        self.assertEqual(self.last["body"], {"op": "commit", "token": 0})

    def test_committing_a_restatement_is_refused_before_a_request_is_sent(self):
        before = len(self.server.requests)
        with self.assertRaises(PlannerError) as ctx:
            self.client.commit(None)
        self.assertIn("restatement", str(ctx.exception))
        self.assertEqual(len(self.server.requests), before, "nothing should have been sent")

    def test_health_reports_the_revision(self):
        self.server.next_response = {"status": "ok", "revision": 3}
        self.assertEqual(self.client.health()["revision"], 3)
        self.assertEqual(self.last["path"], "/healthz")
        self.assertEqual(self.last["method"], "GET")


class ClientErrorTests(DaemonTestCase):
    def test_an_error_document_raises_even_though_the_status_was_200(self):
        self.server.next_response = {"status": "error", "message": "stale plan: re-plan"}
        with self.assertRaises(PlannerError) as ctx:
            self.client.plan()
        self.assertIn("stale plan", str(ctx.exception))
        self.assertEqual(ctx.exception.status, 0, "not a transport failure")

    def test_a_real_http_error_carries_its_status(self):
        self.server.next_response = {"nope": True}
        self.server.http_status = 503
        with self.assertRaises(PlannerError) as ctx:
            self.client.plan()
        self.assertEqual(ctx.exception.status, 503)

    def test_an_unreachable_daemon_is_a_planner_error_not_a_urlerror(self):
        # Port 1 on loopback: nothing listens, and the connection is refused fast.
        with self.assertRaises(PlannerError) as ctx:
            Client("http://127.0.0.1:1", timeout=2).graph()
        self.assertIn("could not reach", str(ctx.exception))

    def test_a_daemon_that_connects_then_stalls_is_a_planner_error_not_a_timeout(self):
        # The connect leg is the easy one — urllib wraps that in URLError. This is the
        # response leg, where the socket is open and the daemon simply never answers:
        # there `TimeoutError` comes out raw, past any URLError handler. Callers only
        # know PlannerError, so an escaping timeout takes the app down with it.
        self.server.response_delay = 0.75
        with self.assertRaises(PlannerError) as ctx:
            Client(self.client.base_url, timeout=0.05).graph()
        self.assertEqual(ctx.exception.status, 0, "it never reached the planner")
        self.assertIn("could not reach", str(ctx.exception))


# ── load_project ──────────────────────────────────────────────────────────────


class LoadProjectTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir)

    def _write(self, name, text):
        with open(os.path.join(self.dir, name), "w", encoding="utf-8") as fh:
            fh.write(text)

    def test_reads_sql_files_by_stem(self):
        self._write("stg_orders.sql", "select 1")
        self._write("mart.sql", "select 2")
        self.assertEqual(load_project(self.dir), {"stg_orders": "select 1", "mart": "select 2"})

    def test_ignores_everything_that_is_not_sql(self):
        self._write("model.sql", "select 1")
        self._write("README.md", "# notes")
        self._write("schema.yml", "version: 2")
        self.assertEqual(list(load_project(self.dir)), ["model"])

    def test_does_not_descend_into_subdirectories(self):
        # Two models named orders.sql in different folders would collide silently,
        # so nesting is refused rather than flattened.
        os.mkdir(os.path.join(self.dir, "staging"))
        self._write(os.path.join("staging", "orders.sql"), "select 1")
        self._write("orders.sql", "select 2")
        self.assertEqual(load_project(self.dir), {"orders": "select 2"})

    def test_an_empty_directory_is_an_error_not_an_empty_project(self):
        with self.assertRaises(ValueError):
            load_project(self.dir)


if __name__ == "__main__":
    unittest.main()


class AvoidedWorkTest(unittest.TestCase):
    def test_avoided_work_is_the_unpruned_baseline_minus_the_plan(self):
        est = CostEstimate.from_wire({"total_millicu": 2000, "per_model": [], "unpruned_millicu": 5000})
        self.assertEqual(est.avoided_millicu, 3000)

    def test_an_older_daemon_reports_no_baseline_and_no_saving(self):
        est = CostEstimate.from_wire({"total_millicu": 2000, "per_model": []})
        self.assertEqual((est.unpruned_millicu, est.avoided_millicu), (0, 0))

    def test_a_saving_is_never_negative(self):
        est = CostEstimate.from_wire({"total_millicu": 3000, "per_model": [], "unpruned_millicu": 1000})
        self.assertEqual(est.avoided_millicu, 0)
