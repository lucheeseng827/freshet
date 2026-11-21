"""A sample project, and the freshet CLI for the tests that classify.

The project is three models over one source: ``events`` (incremental, by ``dt``),
``daily`` (a table) and ``recent`` (a view of ``daily``). A test changes the SQL it
is about and leaves the rest.
"""

import json
import os
import shutil
from pathlib import Path

import pytest

MODELS = {
    "events": "select dt, user_id, amount, note from raw_events",
    "daily": "select dt, sum(amount) as total from events group by dt",
    "recent": "select dt, total from daily where total > 0",
}

SIDE = {
    "sources.json": {"raw_events": ["dt", "user_id", "amount", "note"]},
    "materializations.json": {"events": "incremental", "daily": "table", "recent": "view"},
    "incremental.json": {
        "models": {
            "events": {"replace": {"strategy": "insert_overwrite", "partition_column": "dt", "partition_type": "date"}}
        }
    },
}


@pytest.fixture
def project(tmp_path):
    """Write the sample project, with any model's SQL replaced; returns its directory."""

    def write(**sql):
        directory = tmp_path / "models"
        directory.mkdir(exist_ok=True)
        for name, text in {**MODELS, **sql}.items():
            path = directory / f"{name}.sql"
            if text is None:
                path.unlink(missing_ok=True)
            else:
                path.write_text(text + "\n")
        for name, body in SIDE.items():
            (directory / name).write_text(json.dumps(body))
        return directory

    return write


@pytest.fixture
def freshet():
    """The freshet CLI: ``$FRESHET``, else ``freshet`` on PATH, else the test skips."""
    found = os.environ.get("FRESHET") or shutil.which("freshet")
    if not found:
        pytest.skip("set FRESHET to the freshet CLI (cargo build -p freshet-cli: target/debug/freshet)")
    return str(Path(found).resolve())
