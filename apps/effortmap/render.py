"""Choose how an effort map is rendered, from Python.

`effortmap.py` turns a plan into rows. This turns rows into something a person looks
at, and it is where the choice of *renderer* lives:

    python render.py --to text                 a terminal / notebook / CI log
    python render.py --to html --out map.html  one self-contained file, no server
    python render.py --to json                 for a machine
    python render.py --to streamlit            the interactive app (app.py)

The first three are **standard library only**. Streamlit is one option among them, not
the price of admission — installing it is what buying the interactive version costs,
and an analyst who wants a rendered answer in CI never pays it. That is the whole point
of the seam: before it, importing anything here meant importing pandas and altair.

Adding a renderer is registering a function:

    @renderer("md", "GitHub-flavoured Markdown, for a PR comment")
    def _markdown(plan, out, **opts): ...

`RENDERERS` is the registry the CLI's `--to` reads, so a new entry is documented,
listed in `--help` and selectable without touching the argument parser.
"""

from __future__ import annotations

import argparse
import html as html_mod
import json
import os
import sys
from typing import Callable, Dict, List, NamedTuple, Optional, TextIO

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "sdks", "python"))

import effortmap  # noqa: E402
from freshet_sdk import Client, Plan, PlannerError, load_project  # noqa: E402

__all__ = ["RENDERERS", "renderer", "render", "Renderer", "main"]


class Renderer(NamedTuple):
    name: str
    help: str
    fn: Callable[..., None]
    #: False when the renderer needs a package that is not in the standard library.
    stdlib: bool


RENDERERS: Dict[str, Renderer] = {}


def renderer(name: str, help: str, stdlib: bool = True):
    """Register a renderer under `name`."""

    def wrap(fn):
        RENDERERS[name] = Renderer(name=name, help=help, fn=fn, stdlib=stdlib)
        return fn

    return wrap


def render(plan: Plan, to: str = "text", out: Optional[TextIO] = None, **opts) -> None:
    """Render `plan` with the named renderer. Raises `KeyError` for an unknown name."""
    if to not in RENDERERS:
        known = ", ".join(sorted(RENDERERS))
        raise KeyError(f"unknown renderer {to!r}; known: {known}")
    RENDERERS[to].fn(plan, out or sys.stdout, **opts)


# ── shared bits ───────────────────────────────────────────────────────────────


def _fmt_cu(value: Optional[float]) -> str:
    return "—" if value is None else f"{value:.2f}"


def _headline(plan: Plan) -> List[tuple]:
    s = effortmap.summary(plan)
    verb = "restated" if plan.token is None else "changed directly"
    return [
        ("Models rebuilding", str(s["models"])),
        ("Predicted cost", f"{_fmt_cu(s['total_cu'])} CU"),
        ("Work items", "—" if s["work_items"] is None else str(s["work_items"])),
        ("Critical path", "—" if s["depth"] is None else str(s["depth"])),
        ("Widest wave", "—" if s["width"] is None else str(s["width"])),
        ("Split", f"{s['directly_changed']} {verb}, {s['downstream']} downstream"),
    ]


#: Repeated by every renderer that shows a number, on purpose. A cost figure that
#: travels without it eventually gets read as minutes.
UNIT_NOTE = (
    "Cost is a rate-card prediction in compute units — 1 CU is one full rebuild of one "
    "`table` model. No benchmark stands behind it and nothing here is measured in "
    "seconds. mCU is the exact integer; CU is that divided by 1000, for display."
)


# ── text ──────────────────────────────────────────────────────────────────────


@renderer("text", "plain text for a terminal, a notebook or a CI log")
def _text(plan: Plan, out: TextIO, width: int = 92, **_) -> None:
    def rule(char="─"):
        print(char * width, file=out)

    print("EFFORT MAP", file=out)
    rule()
    if not plan:
        print("Nothing to rebuild — every fingerprint matches the last commit.", file=out)
        return

    for key, value in _headline(plan):
        print(f"  {key:<20} {value}", file=out)
    rule()

    rows = effortmap.effort_rows(plan)
    # Column widths from the data, so a long model name never wraps into the numbers.
    w_model = max(5, *(len(r["model"]) for r in rows))
    w_basis = max(10, *(len(r["basis_label"]) for r in rows))
    print(f"  {'MODEL':<{w_model}}  {'WHY':<10} {'CU':>7}  {'CHARGED AS':<{w_basis}}  REBUILDS", file=out)
    for r in rows:
        print(
            f"  {r['model']:<{w_model}}  {r['origin']:<10} {_fmt_cu(r['cu']):>7}  "
            f"{r['basis_label']:<{w_basis}}  {r['unit']}",
            file=out,
        )
        if r["because_of"]:
            print(f"  {'':<{w_model}}  └─ pulled in by {r['because_of']} via {r['via_columns']}", file=out)

    waves = effortmap.wave_rows(plan)
    if waves:
        rule()
        print("  QUEUE", file=out)
        for w in waves:
            print(f"    wave {w['wave']}  {w['items']:>3} item(s)  {_fmt_cu(w['cu']):>7} CU  "
                  f"{', '.join(w['models'])}", file=out)

    breakdown = effortmap.breakdown_rows(plan)
    if breakdown:
        rule()
        print("  PARTITION SCOPING", file=out)
        for b in breakdown:
            print(f"    {b['model']:<{w_model}}  {b['partitions']} partitions  "
                  f"{b['first']} … {b['last']}", file=out)

    rule()
    for line in _wrap(UNIT_NOTE, width - 2):
        print(f"  {line}", file=out)


def _wrap(text: str, width: int) -> List[str]:
    lines, line = [], ""
    for word in text.split():
        if line and len(line) + 1 + len(word) > width:
            lines.append(line)
            line = word
        else:
            line = f"{line} {word}".strip()
    if line:
        lines.append(line)
    return lines


# ── json ──────────────────────────────────────────────────────────────────────


@renderer("json", "the shaped rows, for a machine or another tool")
def _json(plan: Plan, out: TextIO, **_) -> None:
    json.dump(
        {
            "summary": effortmap.summary(plan),
            "models": effortmap.effort_rows(plan),
            "waves": effortmap.wave_rows(plan),
            "breakdown": effortmap.breakdown_rows(plan),
            "queue": effortmap.queue_rows(plan),
            "unit_note": UNIT_NOTE,
        },
        out,
        indent=2,
        sort_keys=True,
    )
    print(file=out)


@renderer("ndjson", "the work queue only, one item per line, for a runner")
def _ndjson(plan: Plan, out: TextIO, **_) -> None:
    print(effortmap.queue_ndjson(plan), file=out)


# ── html ──────────────────────────────────────────────────────────────────────

_CSS = """
:root{color-scheme:light;--bg:#fcfcfb;--card:#fff;--bd:#e2e1dc;--ink:#16150f;
--ink2:#52514e;--mute:#7c7b74;--direct:#2a78d6;--down:#eb6834}
@media(prefers-color-scheme:dark){:root{--bg:#161614;--card:#1f1f1c;--bd:#32322d;
--ink:#f4f4ef;--ink2:#c3c2b7;--mute:#918f85;--direct:#3987e5;--down:#d95926}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.55 ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
.wrap{max-width:1080px;margin:0 auto;padding:32px 24px 64px}
h1{margin:0 0 4px;font-size:32px;letter-spacing:-.02em}
p.lede{margin:0 0 24px;color:var(--ink2)}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin:20px 0}
.stat{background:var(--card);border:1px solid var(--bd);border-radius:8px;padding:13px 15px}
.stat .k{font-size:12px;color:var(--mute)}
.stat .v{font-size:25px;font-weight:600;margin-top:2px;letter-spacing:-.02em}
h2{font-size:15px;margin:32px 0 6px}
p.hint{margin:0 0 14px;color:var(--mute);font-size:13px;max-width:76ch}
.scroll{overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:13.5px}
th,td{text-align:left;padding:7px 11px;border-bottom:1px solid var(--bd);white-space:nowrap}
th{font-size:12px;color:var(--mute)}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums;
font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12.5px}
.pill{display:inline-block;padding:1px 8px;border-radius:99px;font-size:11.5px;font-weight:600}
.pill.changed{background:color-mix(in srgb,var(--direct) 16%,transparent);color:var(--direct)}
.pill.downstream{background:color-mix(in srgb,var(--down) 18%,transparent);color:var(--down)}
.chart{margin:4px 0 16px}
.g{stroke:var(--bd)}
.t,.lg{fill:var(--mute);font-size:11px}
.t{text-anchor:middle}
.rl{fill:var(--ink2);font-size:12px;text-anchor:end}
.vl{fill:var(--ink);font-size:11.5px;font-family:ui-monospace,Menlo,monospace}
footer{margin-top:40px;padding-top:18px;border-top:1px solid var(--bd);color:var(--mute);font-size:12.5px;max-width:88ch}
code{font-family:ui-monospace,Menlo,monospace;font-size:.92em}
"""


def _svg_bars(rows, colors, axis_label, row_h=28) -> str:
    """A horizontal bar chart as inline SVG — no script, no library, no request.

    The same mark specs the other two front ends use: 4px rounded data-ends, a gap
    between fills, recessive axes, direct value labels, a legend when there are two
    series. Static, because a rendered report has nothing to be interactive about —
    the table below carries every number the bars encode.
    """
    if not rows:
        return ""
    pad_l = min(220, 8 * max(len(r[0]) for r in rows) + 14)
    pad_r, pad_t, pad_b, w = 70, 28 if colors else 8, 32, 880
    plot_w = w - pad_l - pad_r
    h = pad_t + len(rows) * row_h + pad_b
    top = max((r[1] for r in rows), default=0) or 1
    # Round the axis up to a readable bound rather than ending on the data.
    mag = 10 ** len(str(int(top)).split(".")[0][:-1] or "0") if top >= 10 else 1
    nice = next((s * mag for s in (1, 2, 2.5, 5, 10) if top <= s * mag), top)

    parts = [f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}" role="img" '
             f'aria-label="{html_mod.escape(axis_label)}">']
    for i in range(5):
        v = nice / 4 * i
        x = pad_l + (v / nice) * plot_w
        parts.append(f'<line class="g" x1="{x:.1f}" x2="{x:.1f}" y1="{pad_t}" y2="{pad_t + len(rows)*row_h}"/>')
        parts.append(f'<text class="t" x="{x:.1f}" y="{pad_t + len(rows)*row_h + 15}">{v:.2f}</text>')
    parts.append(f'<text class="t" x="{pad_l + plot_w/2}" y="{h - 4}">{html_mod.escape(axis_label)}</text>')

    if colors:
        cursor = pad_l
        for key, color in colors.items():
            parts.append(f'<rect x="{cursor}" y="6" width="11" height="11" rx="2" fill="{color}"/>')
            parts.append(f'<text class="lg" x="{cursor + 16}" y="16">{html_mod.escape(key)}</text>')
            cursor += 16 + len(key) * 7.2 + 18

    bar_h = row_h - 10
    for i, (label, value, series) in enumerate(rows):
        y = pad_t + i * row_h + 5
        length = max(3 if value > 0 else 0, (value / nice) * plot_w)
        fill = (colors or {}).get(series, "#2a78d6")
        parts.append(f'<text class="rl" x="{pad_l - 9}" y="{y + bar_h/2 + 4:.0f}">{html_mod.escape(label)}</text>')
        parts.append(f'<rect x="{pad_l}" y="{y}" width="{length:.1f}" height="{bar_h}" rx="4" fill="{fill}">'
                     f'<title>{html_mod.escape(label)}: {value:.2f}</title></rect>')
        parts.append(f'<text class="vl" x="{pad_l + length + 7:.1f}" y="{y + bar_h/2 + 4:.0f}">{value:.2f}</text>')
    parts.append("</svg>")
    return "".join(parts)


def _table(headers, rows) -> str:
    def cell(value, tag="td"):
        if isinstance(value, tuple):  # (text, "num")
            return f'<{tag} class="num">{value[0]}</{tag}>'
        return f"<{tag}>{value}</{tag}>"

    head = "".join(cell(h, "th") for h in headers)
    body = "".join("<tr>" + "".join(cell(c) for c in r) + "</tr>" for r in rows)
    return f'<div class="scroll"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


@renderer("html", "one self-contained page — no server, no script, nothing fetched")
def _html(plan: Plan, out: TextIO, title: str = "Effort map", **_) -> None:
    e = html_mod.escape
    body: List[str] = [f"<h1>{e(title)}</h1>",
                       '<p class="lede">What rebuilding the data layer costs, and the queue that would do it.</p>']

    if not plan:
        body.append('<p class="hint">Nothing to rebuild — every fingerprint matches the last commit.</p>')
    else:
        body.append('<div class="stats">' + "".join(
            f'<div class="stat"><div class="k">{e(k)}</div><div class="v">{e(v)}</div></div>'
            for k, v in _headline(plan)[:5]) + "</div>")

        rows = effortmap.effort_rows(plan)
        colors = {"changed": "var(--direct)", "downstream": "var(--down)"}
        body.append("<h2>Effort map</h2>")
        body.append('<div class="chart">' + _svg_bars(
            [(r["model"], r["cu"] or 0.0, r["origin"]) for r in rows],
            colors, "compute units (CU)") + "</div>")
        body.append(_table(
            ["Model", "Why", ("CU", "num"), ("mCU (exact)", "num"), "Charged as", "Rebuilds", "Because of", "Via columns"],
            [[e(r["model"]),
              f'<span class="pill {r["origin"]}">{r["origin"]}</span>',
              (_fmt_cu(r["cu"]), "num"),
              (str(r["millicu"] if r["millicu"] is not None else "—"), "num"),
              e(r["basis_label"]), e(r["unit"]), e(r["because_of"]), e(r["via_columns"])] for r in rows]))

        breakdown = effortmap.breakdown_rows(plan)
        if breakdown:
            body.append("<h2>Partition scoping</h2>")
            body.append('<p class="hint">Each partition becomes its own work item — this is where a '
                        "rebuild stops being all-or-nothing.</p>")
            body.append(_table(["Model", ("Partitions", "num"), "First", "Last"],
                               [[e(b["model"]), (str(b["partitions"]), "num"), e(b["first"]), e(b["last"])]
                                for b in breakdown]))

        waves = effortmap.wave_rows(plan)
        if waves:
            body.append("<h2>Concurrency waves</h2>")
            body.append('<p class="hint">Every item in a wave can run at the same time. This is the shape '
                        "of the rebuild, not its duration — nothing here is measured in time.</p>")
            body.append('<div class="chart">' + _svg_bars(
                [(f"wave {w['wave']}", w["cu"], "changed") for w in waves], None, "compute units (CU)") + "</div>")
            body.append(_table(["Wave", ("Items", "num"), ("CU", "num"), "Models"],
                               [[str(w["wave"]), (str(w["items"]), "num"), (_fmt_cu(w["cu"]), "num"),
                                 e(", ".join(w["models"]))] for w in waves]))

        queue = effortmap.queue_rows(plan)
        if queue:
            body.append("<h2>Queue</h2>")
            body.append('<p class="hint">Execution order. Draining in <code>#</code> order is always '
                        "dependency-safe; a whole wave at a time is the parallel form.</p>")
            body.append(_table([("#", "num"), "Work item", ("Wave", "num"), "Waits on", ("CU", "num"), ("mCU", "num")],
                               [[(str(q["seq"]), "num"), e(q["target"]), (str(q["wave"]), "num"),
                                 e(q["depends_on"]), (_fmt_cu(q["cu"]), "num"), (str(q["millicu"]), "num")]
                                for q in queue]))

    out.write(
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">"
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{e(title)}</title><style>{_CSS}</style></head><body><div class=\"wrap\">"
        + "".join(body)
        + f'<footer><p>{e(UNIT_NOTE)}</p></footer></div></body></html>\n'
    )


# ── streamlit ─────────────────────────────────────────────────────────────────


@renderer("streamlit", "the interactive app (needs `pip install -r requirements.txt`)", stdlib=False)
def _streamlit(plan: Plan, out: TextIO, **_) -> None:
    """Hand off to `app.py` under a real Streamlit server.

    Streamlit owns its own process — it re-executes its script on every interaction —
    so this cannot render into `out` the way the others do. It execs the server
    instead, which is the honest thing for a renderer whose output is a session rather
    than a document.
    """
    import shutil
    import subprocess

    if shutil.which("streamlit") is None:
        raise PlannerError(
            "the streamlit renderer needs streamlit installed: "
            "pip install -r requirements.txt (the other renderers need nothing)"
        )
    app = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app.py")
    print(f"launching the interactive app: streamlit run {app}", file=out)
    raise SystemExit(subprocess.call(["streamlit", "run", app]))


# ── the CLI ───────────────────────────────────────────────────────────────────


def main(argv: Optional[List[str]] = None) -> int:
    choices = sorted(RENDERERS)
    epilog = "renderers:\n" + "\n".join(
        f"  {r.name:<10} {r.help}{'' if r.stdlib else '   [needs extra packages]'}"
        for r in sorted(RENDERERS.values(), key=lambda r: r.name)
    )
    parser = argparse.ArgumentParser(
        prog="render.py",
        description="Render an effort map. Ask the planner what a rebuild costs, then draw it.",
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--to", default="text", choices=choices, help="which renderer (default: text)")
    parser.add_argument("--daemon", default="http://127.0.0.1:8788", help="where freshet-serve is listening")
    parser.add_argument("--project", help="a directory of .sql models to load into the daemon first")
    parser.add_argument("--sources", help="a JSON file of {table: [columns]}")
    parser.add_argument("--materializations", help="a JSON file of {model: table|view|incremental}")
    parser.add_argument("--restate", metavar="MODEL", help="plan a data restatement of this model instead")
    parser.add_argument("--partitions", default="", help="comma-separated partitions, with --restate")
    parser.add_argument("--out", help="write here instead of stdout")
    parser.add_argument("--fail-over", type=float, metavar="CU",
                        help="exit 2 when the plan costs more than this many CU (for a CI gate)")
    args = parser.parse_args(argv)

    client = Client(args.daemon)
    try:
        if args.project:
            client.reload(
                load_project(args.project),
                sources=_read_json(args.sources),
                materializations=_read_json(args.materializations),
            )
        if args.restate:
            partitions = [p.strip() for p in args.partitions.split(",") if p.strip()]
            if not partitions:
                parser.error("--restate needs --partitions")
            plan = client.restate(args.restate, partitions, include_cost=True, include_queue=True)
        else:
            plan = client.plan(include_cost=True, include_queue=True)
    except (PlannerError, OSError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    stream = open(args.out, "w", encoding="utf-8") if args.out else sys.stdout
    try:
        render(plan, to=args.to, out=stream)
    finally:
        if args.out:
            stream.close()
            print(f"wrote {args.out}", file=sys.stderr)

    if args.fail_over is not None and plan.cost and plan.cost.total_cu > args.fail_over:
        print(f"error: plan costs {plan.cost.total_cu:.2f} CU, over the {args.fail_over:.2f} CU limit",
              file=sys.stderr)
        return 2
    return 0


def _read_json(path: Optional[str]):
    if not path:
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


if __name__ == "__main__":
    raise SystemExit(main())
