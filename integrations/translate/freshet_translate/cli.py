"""``freshet-translate``: translate a project for another warehouse, and report what changed.

Exit status: 0 when every model has SQL for the new side and the project was
written, 1 when a model could not be translated or Freshet could not plan the
result (nothing is written then), and 2 for a usage error.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional, Sequence

from .translate import CONTRACT, DIALECTS, Report, TranslateError, translate_project

#: The order a text report lists models in: what needs a look first.
_ORDER = {"rewritten": 0, "downstream": 1, "unchanged": 2}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="freshet-translate",
        description="Translate a Freshet project's SQL to another warehouse with sqlglot, "
        "and let Freshet name the models the translation changed in substance.",
    )
    parser.add_argument("--project", default=".", help="the project to translate: its *.sql models and JSON side files")
    parser.add_argument("--from", dest="read", required=True, choices=DIALECTS, metavar="DIALECT", help="the warehouse its SQL is written for")
    parser.add_argument("--to", dest="write", required=True, choices=DIALECTS, metavar="DIALECT", help="the warehouse to translate it for")
    parser.add_argument("--out", required=True, help="where the translated project goes; replaced whole on every run")
    parser.add_argument("--overrides", metavar="DIR", help="hand-written SQL for the new side, <model>.sql, used as written")
    parser.add_argument("--freshet", metavar="PATH", help="the freshet CLI (default: $FRESHET, then freshet on PATH)")
    parser.add_argument("--json", action="store_true", help=f"print the {CONTRACT} report instead of text")
    parser.add_argument("--diff", action="store_true", help="show what was rewritten in each rewritten model")
    args = parser.parse_args(argv)
    try:
        report = translate_project(
            args.project, args.out, args.read, args.write, overrides=args.overrides, freshet=args.freshet
        )
    except TranslateError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(json.dumps(report.to_json(), indent=2) if args.json else render(report, diff=args.diff))
    return 0 if report.ok else 1


def render(report: Report, *, diff: bool = False) -> str:
    """The report as text."""
    head = f"{report.read} -> {report.write} ({report.translator})"
    if not report.ok:
        failed = [m for m in report.models if m.status == "failed"]
        lines = [
            f"{head}: {len(failed)} of {len(report.models)} model(s) could not be translated, "
            f"so nothing was written to {report.out}"
        ]
        lines += [f"  {m.name:<24} {m.error}" for m in failed]
        lines.append(f"Write each one for {report.write} as <dir>/<model>.sql, and run again with --overrides <dir>.")
        return "\n".join(lines)

    counts = ", ".join(
        f"{sum(m.status == s for m in report.models)} {s}" for s in _ORDER if any(m.status == s for m in report.models)
    )
    lines = [f"{head}: {len(report.models)} model(s) into {report.out}: {counts}"]
    for m in sorted(report.models, key=lambda m: (_ORDER[m.status], m.name)):
        lines.append(f"  {m.status:<11} {m.name:<24} {_note(m, report.write)}".rstrip())
    unknown = [(m.name, f) for m in report.models for f in m.untranslated_functions]
    if unknown:
        lines.append(f"passed through as written, so {report.write} must have them with the same meaning:")
        lines += [f"  {name:<24} {f}" for name, f in unknown]
    if diff:
        for m in report.models:
            if m.status == "rewritten":
                lines += ["", m.diff(report.read, report.write).rstrip("\n")]
    return "\n".join(lines)


def _note(m, write: str) -> str:
    notes: List[str] = []
    if m.status == "rewritten":
        notes.append(f"rebuild it on {write} and check it")
    elif m.status == "downstream":
        notes.append(f"check it: it reads {m.because_of}")
    if m.override:
        notes.append("written by hand")
    return "; ".join(notes)
