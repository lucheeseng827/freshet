"""``BashOperator`` for one plan model."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional

try:  # Airflow 3, and Airflow 2 with the standard provider installed.
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2 without it, where BashOperator is still in core.
    from airflow.operators.bash import BashOperator  # type: ignore[no-redef]

from . import FreshetTaskMixin

__all__ = ["FreshetBashOperator"]


class FreshetBashOperator(FreshetTaskMixin, BashOperator):
    """The stock ``BashOperator``, plus the planner's facts and warnings in the task log.

    ``bash_command`` arrives with the Freshet placeholders already expanded and
    protected (see :func:`freshet_airflow.plan.protect_from_jinja`), and is templated
    by Airflow as usual, so ``{{ ds }}`` and every other Airflow macro still work.
    """

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
