"""Freshet's plan contract as Airflow reads it: :mod:`freshet_plan`, and the one Jinja rule.

The reader that was here (parse, compile, facts, placeholders) is the
``freshet-plan`` package now, so that every Python orchestrator reads a plan by one
set of rules and words it with one set of sentences. This module re-exports it
under the names it always had, and keeps the one rule that is Airflow's alone:
protecting substituted values from the Jinja pass Airflow makes afterwards.
"""

from __future__ import annotations

from freshet_plan import *  # noqa: F401,F403 — the re-export is the point
from freshet_plan import PlanInput  # noqa: F401
from freshet_plan import __all__ as _reader

__all__ = [*_reader, "protect_from_jinja"]


def protect_from_jinja(value: str) -> str:
    """Rewrite ``value`` so that a Jinja render reproduces it exactly.

    A substituted value lands in a string that Airflow renders with Jinja later. If
    it contains ``{{``, ``{%`` or ``{#`` (SQL can: a JSON literal, a regex, a comment)
    Jinja would read it as template syntax: fail on it, or worse, silently eat a
    ``{# ... #}`` span. Every Jinja construct opens with ``{``, so replacing each
    ``{`` in the value with the expression ``{{ '{' }}``, which renders to a single
    brace, leaves nothing in the value for Jinja to open. Nothing else needs
    escaping: a lone ``}`` or ``%`` outside a construct is plain text.

    This assumes Jinja's default delimiters, which is why the glue refuses a DAG
    whose ``jinja_environment_kwargs`` change them.
    """
    return value.replace("{", "{{ '{' }}")
