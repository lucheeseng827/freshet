"""``SQLExecuteQueryOperator`` for one plan model's rendered statements (contract v4)."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional, Sequence

try:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
except ImportError as e:  # pragma: no cover - depends on the environment
    raise ImportError(
        "running a plan's SQL through conn_id needs the common-sql provider: "
        "pip install apache-airflow-providers-common-sql (plus the provider for your "
        "database, e.g. apache-airflow-providers-postgres)"
    ) from e

from . import FreshetTaskMixin

__all__ = ["FreshetSQLExecuteQueryOperator"]


class FreshetSQLExecuteQueryOperator(FreshetTaskMixin, SQLExecuteQueryOperator):
    """The stock ``SQLExecuteQueryOperator``, except that the SQL is never templated.

    The stock operator runs ``sql`` through Jinja before executing it, and resolves a
    string ending in ``.sql`` or ``.json`` as a template *file*. Both are right for SQL
    a person wrote with Airflow in mind, and both are hazards for SQL Freshet
    rendered: a ``{#`` in a string literal opens a Jinja comment that silently eats
    SQL up to the next ``#}``, and a statement that happens to end in ``.sql`` (a
    trailing ``-- models/events.sql`` comment, say) makes Airflow look for a file.
    Freshet's statements are plain SQL with nothing for Jinja to do, so this class
    drops ``sql`` from ``template_fields`` and executes them byte for byte as rendered.

    The cost is that Airflow's *Rendered Template* view has no SQL to show. The
    statements are still in the task's doc, and the provider logs each one as it
    runs, so the task log is the exact record of what executed.

    Everything else is the stock operator: ``conn_id``, ``database`` and
    ``hook_params`` stay templated, and a list of statements runs on one connection,
    in order, in one transaction unless ``autocommit`` is on.
    """

    template_fields: Sequence[str] = tuple(
        field for field in SQLExecuteQueryOperator.template_fields if field != "sql"
    )
    template_ext: Sequence[str] = ()

    def __init__(
        self,
        *,
        freshet_facts: Optional[Mapping[str, Any]] = None,
        freshet_warnings: Iterable[str] = (),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._init_freshet(freshet_facts, freshet_warnings)

    def execute(self, context: Any) -> Any:
        self._announce_freshet()
        return super().execute(context)
