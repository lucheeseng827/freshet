"""planner Python SDK — ask the backfill planner what a change rebuilds and what it costs.

Standard library only (``json`` + ``urllib``), one module, no dependencies — the same
shape as dagron's ``dagron.py`` SDK.

The planner is Rust and stays Rust. This talks to ``freshet-serve`` over HTTP, so a
Python caller never builds, links or installs a Rust toolchain::

    freshet-serve --project ./models --sources sources.json --port 8788

    from freshet_sdk import Client, load_project

    api = Client("http://127.0.0.1:8788")
    api.reload(load_project("./models"), sources={"raw_orders": ["id", "amount"]})

    plan = api.plan(include_cost=True, include_queue=True)
    print(f"{len(plan.models)} models, {plan.total_cu:.1f} CU")
    for item in plan.queue.items:
        print(item.seq, item.label, f"{item.cu:.2f} CU")

    api.commit(plan.token)      # only after the plan has actually run

There is a second, in-process route: ``freshet-py`` is a pyo3 wheel wrapping the same
JSON funnel, with no server and no socket. Use it when the caller is a one-shot script;
use this when the project is large enough that keeping it parsed and warm between calls
is worth a daemon.

## On the unit

Cost is reported in the planner's own unit, where **1 CU is one full rebuild of one
``table`` model**. It is a prediction read off a rate card, not a measurement: there is
no benchmark behind it, and nothing here is denominated in seconds. Anything this SDK
prints says CU, and so should anything built on it.

``millicu`` is the exact integer the planner computed and the one to compare, sum or
store. ``cu`` is that divided by 1000 for display, and is the only lossy number here.
"""

from __future__ import annotations

import collections.abc
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence

__all__ = [
    "Client",
    "PlannerError",
    "Plan",
    "PlanModel",
    "CostEstimate",
    "ModelCost",
    "Queue",
    "WorkItem",
    "load_project",
    "MILLICU_PER_CU",
    "__version__",
]
__version__ = "0.1.0"

#: The planner stores cost as an integer in thousandths of a compute unit, so that
#: sums and comparisons stay exact and a `view` (50 mCU) is not rounded to zero.
MILLICU_PER_CU = 1000


class PlannerError(Exception):
    """The planner refused a request, or the daemon could not be reached.

    ``freshet-serve`` answers every request with HTTP 200 and reports failure *in the
    document* (``{"status": "error", "message": ...}``), so that every binding — HTTP,
    C-ABI, WASM, in-process — reads errors the same way. This exception is therefore
    raised from the body, not from the status line; ``status`` is 0 for a transport
    failure that never reached the planner.
    """

    def __init__(self, message: str, status: int = 0) -> None:
        super().__init__(message)
        self.message = message
        self.status = status


# ── the plan, as the wire describes it ────────────────────────────────────────


@dataclass(frozen=True)
class PlanModel:
    """One model the planner selected, and why."""

    name: str
    #: ``"directly_changed"``, or ``{"downstream": {"because_of": ..., "via_columns": [...]}}``.
    reason: Any
    #: ``"full_model"``, or ``{"partitions": [...]}``.
    unit: Any

    @property
    def partitions(self) -> Optional[List[str]]:
        """The partitions this model rebuilds, or ``None`` for a whole-model rebuild."""
        if isinstance(self.unit, Mapping) and "partitions" in self.unit:
            return list(self.unit["partitions"])
        return None

    @property
    def because_of(self) -> Optional[str]:
        """The upstream model this one was attributed to, or ``None`` if it changed itself."""
        if isinstance(self.reason, Mapping) and "downstream" in self.reason:
            return self.reason["downstream"].get("because_of")
        return None

    @property
    def via_columns(self) -> List[str]:
        """The changed columns that pulled this model in. Empty if it changed itself."""
        if isinstance(self.reason, Mapping) and "downstream" in self.reason:
            return list(self.reason["downstream"].get("via_columns", []))
        return []

    @property
    def directly_changed(self) -> bool:
        return self.reason == "directly_changed"

    @classmethod
    def from_wire(cls, d: Mapping[str, Any]) -> "PlanModel":
        return cls(name=d["name"], reason=d["reason"], unit=d["unit"])


@dataclass(frozen=True)
class ModelCost:
    """What one model contributes to the plan's cost, and on what grounds."""

    model: str
    millicu: int
    #: ``table_full`` | ``view_full`` | ``incremental_full`` |
    #: ``{"incremental_partitions": {"count": n}}`` | ``unknown_model``.
    basis: Any

    @property
    def cu(self) -> float:
        return self.millicu / MILLICU_PER_CU

    @property
    def basis_name(self) -> str:
        """The basis as a bare string, flattening the one variant that carries data."""
        if isinstance(self.basis, Mapping):
            return next(iter(self.basis))
        return str(self.basis)

    @property
    def partition_count(self) -> Optional[int]:
        """How many partitions were charged for, when that is the basis."""
        if isinstance(self.basis, Mapping):
            inner = self.basis.get("incremental_partitions")
            if isinstance(inner, Mapping):
                return inner.get("count")
        return None

    @classmethod
    def from_wire(cls, d: Mapping[str, Any]) -> "ModelCost":
        return cls(model=d["model"], millicu=int(d["millicu"]), basis=d["basis"])


@dataclass(frozen=True)
class CostEstimate:
    """The effort map: a predicted total, and the per-model breakdown behind it."""

    total_millicu: int
    per_model: List[ModelCost] = field(default_factory=list)
    #: What the same change would cost without pruning — every model downstream of
    #: the changed ones, rebuilt whole (contract v5; 0 from an older daemon).
    unpruned_millicu: int = 0

    @property
    def total_cu(self) -> float:
        return self.total_millicu / MILLICU_PER_CU

    @property
    def avoided_millicu(self) -> int:
        """Rebuild work the plan avoids: the unpruned baseline minus the plan, never
        below zero. Freshet's north-star metric."""
        return max(0, self.unpruned_millicu - self.total_millicu)

    def heaviest(self, n: int = 5) -> List[ModelCost]:
        """The `n` costliest models, descending. Ties break by name, so the order is stable."""
        return sorted(self.per_model, key=lambda m: (-m.millicu, m.model))[:n]

    @classmethod
    def from_wire(cls, d: Mapping[str, Any]) -> "CostEstimate":
        return cls(
            total_millicu=int(d["total_millicu"]),
            per_model=[ModelCost.from_wire(m) for m in d.get("per_model", [])],
            unpruned_millicu=int(d.get("unpruned_millicu", 0)),
        )


@dataclass(frozen=True)
class WorkItem:
    """One unit of executable work: a model, at a granularity, with its barriers."""

    seq: int
    model: str
    #: The partition this item rebuilds, or ``None`` for a whole-model rebuild.
    partition: Optional[str]
    #: Models that must finish before this item may start.
    depends_on: List[str]
    millicu: int
    #: Items sharing a wave may run concurrently.
    wave: int

    @property
    def cu(self) -> float:
        return self.millicu / MILLICU_PER_CU

    @property
    def label(self) -> str:
        """``model`` or ``model[partition]`` — what to print in a queue listing."""
        return self.model if self.partition is None else f"{self.model}[{self.partition}]"

    @classmethod
    def from_wire(cls, d: Mapping[str, Any]) -> "WorkItem":
        return cls(
            seq=int(d["seq"]),
            model=d["model"],
            partition=d.get("partition"),
            depends_on=list(d.get("depends_on", [])),
            millicu=int(d["millicu"]),
            wave=int(d["wave"]),
        )


@dataclass(frozen=True)
class Queue:
    """The plan as an executable queue: what to run, in what order, at what width."""

    items: List[WorkItem] = field(default_factory=list)
    #: ``waves[i]`` holds the ``seq`` of every item runnable in wave ``i``.
    waves: List[List[int]] = field(default_factory=list)

    @property
    def depth(self) -> int:
        """The critical path: how many waves deep the plan is."""
        return len(self.waves)

    @property
    def width(self) -> int:
        """The widest wave — the most parallelism this plan can use."""
        return max((len(w) for w in self.waves), default=0)

    @property
    def total_millicu(self) -> int:
        return sum(i.millicu for i in self.items)

    def wave_items(self) -> List[List[WorkItem]]:
        """The queue grouped into waves, as items rather than indices.

        This is the shape a runner wants: submit a wave, wait for it, submit the next.
        Draining ``items`` in ``seq`` order is always correct too, just serial.
        """
        by_seq = {i.seq: i for i in self.items}
        return [[by_seq[s] for s in wave] for wave in self.waves]

    @classmethod
    def from_wire(cls, d: Mapping[str, Any]) -> "Queue":
        return cls(
            items=[WorkItem.from_wire(i) for i in d.get("items", [])],
            waves=[list(w) for w in d.get("waves", [])],
        )


@dataclass(frozen=True)
class Plan:
    """A planner answer: what to rebuild, optionally what it costs and how to run it."""

    models: List[PlanModel]
    revision: int
    #: Hand back to :meth:`Client.commit` **after** the plan has actually run. ``None``
    #: for a restatement, which rebuilds data and so must not advance code fingerprints.
    token: Optional[int] = None
    #: Present only when ``include_cost`` asked for it.
    cost: Optional[CostEstimate] = None
    #: Present only when ``include_queue`` asked for it.
    queue: Optional[Queue] = None
    #: The snapshot to persist, carried opaquely.
    next_state: Optional[Any] = None

    def __bool__(self) -> bool:
        """A plan is truthy when it has work in it, so ``if not plan:`` reads right."""
        return bool(self.models)

    @property
    def total_cu(self) -> float:
        """Predicted cost in whole CU. Raises if cost was not requested."""
        if self.cost is None:
            raise PlannerError("no cost in this plan — pass include_cost=True to plan()")
        return self.cost.total_cu

    @classmethod
    def from_wire(cls, body: Mapping[str, Any]) -> "Plan":
        plan = body.get("plan", {})
        cost = plan.get("cost")
        queue = plan.get("queue")
        return cls(
            models=[PlanModel.from_wire(m) for m in plan.get("models", [])],
            revision=int(body.get("revision", 0)),
            token=body.get("token"),
            cost=CostEstimate.from_wire(cost) if cost is not None else None,
            queue=Queue.from_wire(queue) if queue is not None else None,
            next_state=plan.get("next_state"),
        )


# ── the client ────────────────────────────────────────────────────────────────


class Client:
    """A thin, typed wrapper over ``freshet-serve``.

    Every method is one ``POST /rpc``. The daemon has a single funnel by design — one
    endpoint that dispatches the ``op`` — so this class holds the ergonomics and the
    server holds no request vocabulary it has to keep in sync with a REST surface.
    """

    def __init__(self, base_url: str = "http://127.0.0.1:8788", timeout: float = 30.0) -> None:
        scheme = urllib.parse.urlparse(base_url).scheme
        if scheme not in ("http", "https"):
            raise ValueError("base_url must use http or https")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    # --- operations ---

    def reload(
        self,
        models: Mapping[str, str],
        sources: Optional[Mapping[str, Sequence[str]]] = None,
        materializations: Optional[Mapping[str, str]] = None,
    ) -> int:
        """Replace the daemon's project. Returns the new revision.

        ``models`` maps model name to SQL text; :func:`load_project` builds one from a
        directory. ``sources`` gives the columns of tables the project selects from but
        does not define — without them the planner cannot resolve those columns and
        falls back to assuming they changed. ``materializations`` marks models
        ``table`` (the default), ``view`` or ``incremental``; only ``incremental``
        models are partition-scopable, so a project with no hints plans every rebuild
        as a whole model.

        The revision moves only when the reload changes something a plan depends on
        — the models' SQL, lineage, materializations or incremental config. Then any
        outstanding commit token becomes stale; an identical reload keeps the
        revision, so a token staged before it can still be committed.
        """
        body = self._rpc(
            {
                "op": "reload",
                # The daemon takes ordered pairs, not a map: a project's model order
                # reaches the plan's output, and dict order is not a contract.
                "models": sorted(models.items()),
                "sources": _source_columns(sources),
                "materializations": dict(materializations or {}),
            }
        )
        return int(body["revision"])

    def plan(self, include_cost: bool = True, include_queue: bool = False) -> Plan:
        """Plan the code changes since the last commit.

        Both flags default the way a caller most often wants rather than the way the
        wire does: the wire defaults them off so that a client which never heard of
        cost gets unchanged bytes, but anyone reaching for this SDK is asking the
        cost question. ``include_queue`` stays off because it is the expensive half —
        a plan scoped to N partitions expands to N items per model.
        """
        return Plan.from_wire(
            self._rpc({"op": "plan", "include_cost": include_cost, "include_queue": include_queue})
        )

    def restate(
        self,
        model: str,
        partitions: Sequence[str],
        include_cost: bool = True,
        include_queue: bool = False,
    ) -> Plan:
        """Plan a *data* restatement of ``partitions`` of ``model``.

        Distinct from :meth:`plan`, which answers "what did the code change break?".
        This answers "I need this data rebuilt", and deliberately returns no commit
        token: rebuilding data must not advance the fingerprints that record which
        *code* has been built.
        """
        return Plan.from_wire(
            self._rpc(
                {
                    "op": "restate",
                    "model": model,
                    "partitions": list(partitions),
                    "include_cost": include_cost,
                    "include_queue": include_queue,
                }
            )
        )

    def graph(self) -> List[str]:
        """The project's models in topological order."""
        return list(self._rpc({"op": "graph"})["order"])

    def commit(self, token: Optional[int]) -> int:
        """Persist the snapshot staged by :meth:`plan`. Returns the revision.

        Call this **only after the plan has actually run**. Committing a plan that did
        not run advances the fingerprints anyway, after which the next :meth:`plan`
        sees nothing to do and the rebuild is silently skipped.

        The daemon rejects a token whose project was reparsed in between, so a lost
        update across the request boundary is an error rather than a wrong answer.
        """
        if token is None:
            raise PlannerError("no commit token — a restatement does not produce one")
        return int(self._rpc({"op": "commit", "token": token})["revision"])

    def health(self) -> Dict[str, Any]:
        """Liveness, and the project revision the daemon is currently holding."""
        return self._get("/healthz")

    # --- transport ---

    def _rpc(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        body = self._post("/rpc", payload)
        if body.get("status") == "error":
            raise PlannerError(body.get("message", "planner reported an error"))
        return body

    def _post(self, path: str, payload: Mapping[str, Any]) -> Dict[str, Any]:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.base_url + path,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        return self._send(req)

    def _get(self, path: str) -> Dict[str, Any]:
        return self._send(urllib.request.Request(self.base_url + path, method="GET"))

    def _send(self, req: urllib.request.Request) -> Dict[str, Any]:
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:  # noqa: S310
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            raise PlannerError(
                f"planner returned HTTP {e.code}: {e.read().decode('utf-8', 'replace')[:500]}",
                status=e.code,
            ) from None
        except urllib.error.URLError as e:
            raise PlannerError(f"could not reach {req.full_url}: {e.reason}") from None
        except OSError as e:
            # Where a timeout surfaces depends on which layer gave up: urllib wraps the
            # connect leg in URLError, but the response leg — `getresponse()` and the
            # `read()` above — is outside that wrapping, so a slow daemon raises a bare
            # `TimeoutError` (a plain OSError) straight through the two clauses above.
            # Callers are written against PlannerError alone, so an unwrapped timeout
            # takes the whole app down instead of showing "the planner is not
            # answering". This clause must stay *last*: HTTPError and URLError are both
            # OSError subclasses and would otherwise be swallowed here, losing the
            # status code and the error document with it.
            raise PlannerError(f"could not reach {req.full_url}: {e}") from None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            raise PlannerError(f"planner returned a non-JSON body: {e}") from None


# ── helpers ───────────────────────────────────────────────────────────────────


def _source_columns(sources: Optional[Mapping[str, Sequence[str]]]) -> Dict[str, List[str]]:
    """Normalise ``sources`` to ``{table: [column, ...]}``, refusing a bare string.

    ``list("id")`` is ``["i", "d"]``, so ``{"raw": "id"}`` — the obvious slip for a
    single-column source — would ship two columns that exist nowhere. Nothing downstream
    catches it: the planner resolves against the columns it was handed, finds neither,
    and answers with a plan built on a source it has the wrong shape for. That is a
    wrong answer delivered confidently, which is worse than a refusal, so the string is
    rejected here rather than iterated.
    """
    out: Dict[str, List[str]] = {}
    for table, columns in (sources or {}).items():
        if isinstance(columns, str) or not isinstance(columns, collections.abc.Sequence):
            raise ValueError(
                f"sources[{table!r}] must be a list of column names, "
                f"not {type(columns).__name__}"
            )
        names = list(columns)
        for name in names:
            if not isinstance(name, str):
                raise ValueError(
                    f"sources[{table!r}] must hold column-name strings, "
                    f"found {type(name).__name__}"
                )
        out[table] = names
    return out


def load_project(directory: str) -> Dict[str, str]:
    """Read ``*.sql`` from ``directory`` into a ``{model_name: sql}`` map.

    Non-recursive, matching ``freshet-serve --project``. Nested directories in
    dbt-shaped projects carry meaning this has no model for, and flattening them would
    silently collide two models named ``orders.sql`` in different folders. A project
    laid out that way should build the map itself and name each model explicitly.
    """
    models: Dict[str, str] = {}
    for entry in sorted(os.listdir(directory)):
        if not entry.endswith(".sql"):
            continue
        path = os.path.join(directory, entry)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as fh:
            models[entry[: -len(".sql")]] = fh.read()
    if not models:
        raise ValueError(f"no .sql files in {directory}")
    return models
