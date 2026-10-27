"""The Airflow operators a Freshet plan becomes: the stock ones, plus the planner's record.

Nothing in this ``__init__`` imports Airflow. The operator classes live in
:mod:`.bash` and :mod:`.sql`, and each imports only the provider it needs, so a DAG
that only runs commands never needs the common-sql provider installed, and a DAG
that only runs SQL never touches BashOperator.

Both are thin subclasses, and ``isinstance`` against the stock operator holds for
each. They add one thing: when the task runs, it logs the planner's facts and, at
WARNING level, any note the plan made loud (a widened replace op, a non-atomic
write). A DAG's docs describe the DAG; the task log is the record of one run, and
that is where dagron-state's "the run history says what it meant to do to the
table" has to land in Airflow. A subclass does it without taking over
``on_execute_callback``, which belongs to the DAG's author.
"""

from __future__ import annotations

import json
from typing import Any, Iterable, Mapping, Optional


class FreshetTaskMixin:
    """Carries a plan model's facts and warnings, and logs them when the task runs."""

    freshet_facts: Mapping[str, Any]
    freshet_warnings: tuple

    def _init_freshet(
        self,
        facts: Optional[Mapping[str, Any]],
        warnings: Iterable[str],
    ) -> None:
        self.freshet_facts = dict(facts or {})
        self.freshet_warnings = tuple(warnings)

    def _announce_freshet(self) -> None:
        # Preformatted rather than passed as logging args: Airflow 3's task logger is
        # structlog-backed, and a message with no args is logged verbatim everywhere.
        log = self.log  # type: ignore[attr-defined]
        log.info("Freshet plan facts: " + json.dumps(self.freshet_facts, sort_keys=True))
        for warning in self.freshet_warnings:
            log.warning("FRESHET WARNING: " + warning)
