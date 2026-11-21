"""Translate a project's SQL to another warehouse, and let Freshet say what changed.

Moving a project to a new warehouse is three jobs: copy the data, keep both sides in
step, and translate each model's SQL into the new side's dialect. Freshet does none of
the translating. This module hands it to `sqlglot <https://github.com/tobymao/sqlglot>`_
and then asks Freshet the one question a translator cannot answer about itself:
*which models did the translation change in substance?* Those are the models whose
new side runs SQL the old side never ran, so they are the ones to rebuild there and
check. Everything else was only respelled.

How the question is put
-----------------------
Freshet's AST fingerprint already separates spelling from substance: it ignores
layout, comments, keyword case and redundant parentheses, and keeps everything that
can change a result. So the translated project is planned against a state committed
from the source, and the models Freshet plans are the ones that changed, each with
its reason. A model that reads a changed one is planned too, by Freshet's own
column lineage.

The source is not committed as written, though. A translator respells every model
it touches. ``sum`` becomes ``SUM``, and the fingerprint keeps function-name case on
purpose, because a user-defined function's name can be case-sensitive. Against the
original, nearly every model would read as changed. So the state is committed from
a *baseline*: the source translated into its own dialect, by the same generator
that writes the translation. One cosmetic choice still differs by dialect, the case
of function names (BigQuery keeps ``count`` as written, Postgres writes ``COUNT``),
so both sides are compared with function names in upper case. What still differs
is what the dialect change rewrote. What is written for the new side keeps
sqlglot's own spelling for it, and that is planned too, cold, to show Freshet can
render it for the new side.

What "unchanged" does not mean
------------------------------
An unchanged model runs the same SQL text on both sides. It can still give a
different answer: integer division, NULL ordering and string comparison are each a
warehouse's own. The parity check on the moved partitions is what shows the results
equal. This report says where the translator stepped in, which is where to look
first. It is not a proof.
"""

from __future__ import annotations

import difflib
import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel, SqlglotError

__all__ = [
    "CONTRACT",
    "DIALECTS",
    "SIDE_FILES",
    "MARKER",
    "STATUSES",
    "TranslateError",
    "Model",
    "Translation",
    "Report",
    "translate_sql",
    "translate_project",
    "classify",
]

#: The report's own contract, carried in ``--json`` output.
CONTRACT = "freshet-translate/1"

#: The dialects Freshet renders (``freshet plan --sql``). Each is also sqlglot's name
#: for the same warehouse, so one name serves both.
DIALECTS = ("postgres", "snowflake", "bigquery", "databricks", "redshift")

#: Everything a project directory holds besides its models, copied as it is. These
#: are the files Freshet reads; their contents are not SQL.
SIDE_FILES = ("sources.json", "materializations.json", "incremental.json")

#: Written into every ``--out``, and required in one before it is replaced: the record
#: that this tool wrote it. Without it, a team's own project of ``.sql`` files would
#: look exactly like a translation and be deleted by the next run.
MARKER = ".freshet-translate"

#: What a model's ``status`` can be.
#:
#: ``unchanged``
#:     The translation is the source in substance; the translator rewrote nothing.
#: ``rewritten``
#:     The translator rewrote it, or a person did (an override). Rebuild it on the
#:     new side and check it.
#: ``downstream``
#:     It reads a rewritten model through a column that changed, per Freshet's
#:     lineage. Check it too.
#: ``translated``
#:     It has SQL for the new side, but another model does not, so nothing was
#:     written or classified.
#: ``failed``
#:     It could not be translated. Write it by hand as an override.
STATUSES = ("unchanged", "rewritten", "downstream", "translated", "failed")


class TranslateError(Exception):
    """A run that cannot produce a report: bad arguments, or a project Freshet refuses."""


@dataclass
class Model:
    """One model's outcome."""

    name: str
    status: str
    #: Its target SQL came from ``--overrides``, written by a person.
    override: bool = False
    #: For ``downstream``: the planned model it reads.
    because_of: Optional[str] = None
    #: Functions the translator does not know and passed through as written. The new
    #: warehouse has to have each one, with the same meaning.
    untranslated_functions: List[str] = field(default_factory=list)
    #: For ``failed``: why.
    error: Optional[str] = None
    #: The SQL written for the new side.
    translated: Optional[str] = None
    #: The source and the translation, spelled alike: what Freshet compares.
    baseline: Optional[str] = None
    compared: Optional[str] = None

    def diff(self, read: str, write: str) -> str:
        """What the translation changed, as a unified diff of the compared forms."""
        if self.baseline is None or self.compared is None:
            return ""
        return "".join(
            difflib.unified_diff(
                _lines(self.baseline),
                _lines(self.compared),
                fromfile=f"{self.name}.sql ({read})",
                tofile=f"{self.name}.sql ({write})",
            )
        )


@dataclass
class Report:
    """What a run did, model by model."""

    read: str
    write: str
    out: str
    translator: str
    models: List[Model]

    @property
    def ok(self) -> bool:
        """Every model has SQL for the new side, and the project was written."""
        return not any(m.status == "failed" for m in self.models)

    def to_json(self) -> dict:
        return {
            "contract": CONTRACT,
            "from": self.read,
            "to": self.write,
            "translator": self.translator,
            "out": self.out,
            "ok": self.ok,
            "models": [
                {
                    "name": m.name,
                    "status": m.status,
                    "override": m.override,
                    "because_of": m.because_of,
                    "untranslated_functions": m.untranslated_functions,
                    "error": m.error,
                }
                for m in self.models
            ],
        }


@dataclass(frozen=True)
class Translation:
    """One model's SQL, translated, and the two forms that are compared."""

    #: What is written for the new side, spelled as sqlglot spells ``write``.
    sql: str
    #: The source in its own dialect, and the translation, spelled alike for comparing.
    baseline: str
    compared: str
    #: Functions the translator does not know and passed through as written.
    unknown: List[str]


def translate_sql(sql: str, read: str, write: str) -> Translation:
    """Translate one model's SQL.

    A construct the target has no way to write is an error, not a warning: sqlglot
    would otherwise emit its best guess, which is SQL nobody asked for.

    The written SQL keeps sqlglot's own spelling for each dialect, which knows that
    a BigQuery UDF's name is case-sensitive and the others' are not. The compared
    forms write every function name in upper case instead. Dialects case function
    names differently (``count`` and ``COUNT`` for the same call), and the
    fingerprint keeps function-name case, so the casing alone would read as a rewrite.
    """
    statements = [s for s in sqlglot.parse(sql, read=read) if s is not None]
    if len(statements) != 1:
        raise ValueError(f"a model is one statement, and this is {len(statements)}")
    tree = statements[0]
    strict = dict(pretty=True, unsupported_level=ErrorLevel.RAISE)
    return Translation(
        sql=tree.sql(dialect=write, **strict),
        baseline=tree.sql(dialect=read, normalize_functions="upper", **strict),
        compared=tree.sql(dialect=write, normalize_functions="upper", **strict),
        unknown=sorted({f.name for f in tree.find_all(exp.Anonymous)}, key=str.lower),
    )


def _spelled_alike(sql: str, dialect: str) -> str:
    """Hand-written SQL in the compared spelling, or as written if sqlglot cannot read it."""
    try:
        statements = [s for s in sqlglot.parse(sql, read=dialect) if s is not None]
        if len(statements) == 1:
            return statements[0].sql(dialect=dialect, pretty=True, normalize_functions="upper")
    except SqlglotError:
        pass
    return sql


def translate_project(
    project: os.PathLike,
    out: os.PathLike,
    read: str,
    write: str,
    *,
    overrides: Optional[os.PathLike] = None,
    freshet: Optional[str] = None,
) -> Report:
    """Translate every model of ``project`` into ``out``, then classify each one.

    ``out`` is generated output: it must not exist, be empty, or hold only what an
    earlier run wrote, and it is replaced whole. A model that fails to translate
    fails the run, and then nothing is written at all. A partial project on the new
    side would plan without the missing models. ``overrides`` holds hand-written SQL
    for the new side, ``<model>.sql``, used as written instead of a translation.
    """
    project, out = Path(project), Path(out)
    for flag, dialect in (("--from", read), ("--to", write)):
        if dialect not in DIALECTS:
            raise TranslateError(
                f"{flag} {dialect!r} is not a dialect Freshet renders: {', '.join(DIALECTS)}"
            )
    if not project.is_dir():
        raise TranslateError(f"{project} is not a project directory")
    sources = sorted(project.glob("*.sql"))
    if not sources:
        raise TranslateError(f"{project} holds no models (*.sql)")
    hand = _overrides(overrides, {p.stem for p in sources}, out)
    _check_out(project, out)

    models: List[Model] = []
    for path in sources:
        name, sql = path.stem, path.read_text()
        if name in hand:
            written = hand[name].read_text()
            models.append(
                Model(
                    name,
                    "translated",
                    override=True,
                    translated=written,
                    # The source may be the very SQL the translator could not read,
                    # and then it is compared as written.
                    baseline=_spelled_alike(sql, read),
                    compared=_spelled_alike(written, write),
                )
            )
            continue
        try:
            t = translate_sql(sql, read, write)
        except (SqlglotError, ValueError) as e:
            models.append(Model(name, "failed", error=_one_line(e)))
            continue
        models.append(
            Model(
                name,
                "translated",
                translated=t.sql,
                baseline=t.baseline,
                compared=t.compared,
                untranslated_functions=t.unknown,
            )
        )

    report = Report(read, write, str(out), f"sqlglot {sqlglot.__version__}", models)
    if not report.ok:
        return report

    with tempfile.TemporaryDirectory(prefix="freshet-translate-") as tmp:
        base_dir, cmp_dir, new_dir = (Path(tmp) / d for d in ("baseline", "compared", "translated"))
        _write_project(project, base_dir, {m.name: m.baseline for m in models})
        _write_project(project, cmp_dir, {m.name: m.compared for m in models})
        _write_project(project, new_dir, {m.name: m.translated for m in models})
        planned = classify(base_dir, cmp_dir, new_dir, write, _freshet(freshet), Path(tmp) / "state.json")
        for m in models:
            # An override is classified like a translation: `override` says who wrote it.
            m.status, m.because_of = planned.get(m.name, ("unchanged", None))
        _replace(new_dir, out)
    return report


def classify(
    baseline: Path, compared: Path, translated: Path, write: str, freshet: str, state: Path
) -> Dict[str, Tuple[str, Optional[str]]]:
    """Ask Freshet which translated models differ from the baseline in substance.

    The baseline is committed to a scratch state, and the compared form of the
    translation is planned against it. Then the translation as written is planned
    cold and rendered for the new side. That is a check of its own: a translation
    Freshet cannot plan for ``write`` is a project the new side cannot run, and it
    fails here rather than on the warehouse.

    Returns ``{model: (status, because_of)}`` for every planned model. A model
    missing from it is unchanged.
    """
    _run(freshet, "plan", "--project", str(baseline), "--state", str(state), "--commit", what="the source")
    plan = json.loads(
        _run(freshet, "plan", "--project", str(compared), "--state", str(state), "--json", what="the translation")
    )
    _run(freshet, "plan", "--project", str(translated), "--sql", write, "--json", what=f"the translation for {write}")
    planned: Dict[str, Tuple[str, Optional[str]]] = {}
    for model in plan.get("models", []):
        reason = model.get("reason")
        if reason == "directly_changed":
            planned[model["name"]] = ("rewritten", None)
        elif isinstance(reason, dict) and "downstream" in reason:
            planned[model["name"]] = ("downstream", reason["downstream"].get("because_of"))
        else:
            raise TranslateError(f"freshet planned {model.get('name')} for a reason this tool does not know: {reason!r}")
    return planned


def _overrides(overrides: Optional[os.PathLike], models: set, out: Path) -> Dict[str, Path]:
    if overrides is None:
        return {}
    directory = Path(overrides)
    if not directory.is_dir():
        raise TranslateError(f"--overrides {directory} is not a directory")
    if directory.resolve() == out.resolve():
        raise TranslateError("--overrides and --out are the same directory, and --out is replaced whole")
    hand = {p.stem: p for p in sorted(directory.glob("*.sql"))}
    stray = sorted(set(hand) - models)
    if stray:
        # A misspelled override would silently not apply.
        raise TranslateError(f"--overrides names no model of the project: {', '.join(s + '.sql' for s in stray)}")
    return hand


def _check_out(project: Path, out: Path) -> None:
    if out.resolve() == project.resolve():
        raise TranslateError("--out is the project itself; the translation goes beside it")
    if not out.exists():
        return
    if not out.is_dir():
        raise TranslateError(f"--out {out} is not a directory")
    entries = list(out.iterdir())
    for entry in entries:
        if not (entry.is_file() and (entry.suffix == ".sql" or entry.name in SIDE_FILES or entry.name == MARKER)):
            raise TranslateError(
                f"--out {out} holds {entry.name}, which freshet-translate does not write, so it is not "
                "a translation this tool may replace. Point --out at a new directory."
            )
    if entries and not (out / MARKER).is_file():
        raise TranslateError(
            f"--out {out} was not written by freshet-translate (it has no {MARKER}), so it is not a "
            "translation this tool may replace, however much it looks like one. Point --out at a new "
            "directory."
        )


def _write_project(project: Path, into: Path, sql: Dict[str, Optional[str]]) -> None:
    into.mkdir(parents=True)
    for name, text in sql.items():
        assert text is not None, name
        (into / f"{name}.sql").write_text(text.rstrip("\n") + "\n")
    for side in SIDE_FILES:
        if (project / side).is_file():
            shutil.copyfile(project / side, into / side)


def _replace(new: Path, out: Path) -> None:
    """Put the translated project at ``out``, in place of what an earlier run wrote.

    It is staged beside ``out``, the earlier translation is renamed aside, and the
    staged one renamed into its place; the earlier one is deleted only after that.
    A run that dies part-way leaves the earlier translation, not half of this one,
    and a rename that fails puts it back.
    """
    parent = out.resolve().parent
    parent.mkdir(parents=True, exist_ok=True)
    # A plain mkdir, not mkdtemp: the directory becomes `out`, and mkdtemp's 0700
    # would leave it unreadable to everyone but this user.
    staged = parent / f".{out.name}.{os.getpid()}.{os.urandom(4).hex()}"
    staged.mkdir()
    earlier = None
    try:
        for entry in new.iterdir():
            shutil.copyfile(entry, staged / entry.name)
        (staged / MARKER).write_text("written by freshet-translate, and replaced whole on every run\n")
        if out.exists():
            # Holds only what this tool writes (see _check_out); kept until replaced.
            earlier = parent / f".{out.name}.earlier.{os.getpid()}.{os.urandom(4).hex()}"
            os.replace(out, earlier)
        try:
            os.replace(staged, out)
        except BaseException:
            if earlier is not None:
                os.replace(earlier, out)
                earlier = None
            raise
    except BaseException:
        shutil.rmtree(staged, ignore_errors=True)
        raise
    if earlier is not None:
        shutil.rmtree(earlier, ignore_errors=True)


def _freshet(given: Optional[str]) -> str:
    found = given or os.environ.get("FRESHET") or shutil.which("freshet")
    if not found:
        raise TranslateError(
            "freshet-translate asks the freshet CLI which models changed: put `freshet` on PATH, "
            "set FRESHET, or pass --freshet"
        )
    return found


def _run(freshet: str, *args: str, what: str) -> str:
    try:
        done = subprocess.run([freshet, *args], capture_output=True, text=True)
    except OSError as e:
        raise TranslateError(f"running {freshet}: {e}") from e
    if done.returncode != 0:
        raise TranslateError(f"freshet could not plan {what}:\n{done.stderr.strip()}")
    return done.stdout


def _one_line(e: Exception) -> str:
    text = " ".join(str(e).split())
    return text or type(e).__name__


def _lines(text: str) -> Sequence[str]:
    return [line + "\n" for line in text.rstrip("\n").split("\n")]
