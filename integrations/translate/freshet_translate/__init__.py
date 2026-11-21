"""freshet-translate: translate a project's SQL to another warehouse, and say what changed.

Freshet translates nothing. When a project moves to a new warehouse, this package
runs `sqlglot <https://github.com/tobymao/sqlglot>`_ over every model, writes the
result as a project for the new side, and then has Freshet's own planner name the
models whose translation changed them in substance, and the models that read those::

    freshet-translate --project ./models --from postgres --to snowflake --out ./models.snowflake

    from freshet_translate import translate_project
    report = translate_project("models", "models.snowflake", "postgres", "snowflake")
    [m.name for m in report.models if m.status == "rewritten"]

The rewritten and downstream models are the ones whose new side runs SQL the old
side never ran: rebuild them there, and check them before the others.
:mod:`freshet_translate.translate` says how the question is put to Freshet, and what
an "unchanged" model does not prove.
"""

from .translate import (
    CONTRACT,
    DIALECTS,
    SIDE_FILES,
    STATUSES,
    Model,
    Translation,
    Report,
    TranslateError,
    classify,
    translate_project,
    translate_sql,
)

__version__ = "0.2.0"

__all__ = [
    "__version__",
    "CONTRACT",
    "DIALECTS",
    "SIDE_FILES",
    "STATUSES",
    "TranslateError",
    "Model",
    "Translation",
    "Report",
    "translate_sql",
    "translate_project",
    "classify",
]
