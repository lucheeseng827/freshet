"""Keep Airflow-backed tests away from any real Airflow install.

Importing Airflow creates ``$AIRFLOW_HOME`` (``~/airflow`` by default) and writes into
it. A test suite has no business doing that to the machine it runs on, so unless the
caller chose an ``AIRFLOW_HOME`` explicitly, point it at a throwaway directory before
any test module imports Airflow, and remove that directory when the session ends.
"""

import atexit
import os
import shutil
import tempfile

if "AIRFLOW_HOME" not in os.environ:
    os.environ["AIRFLOW_HOME"] = tempfile.mkdtemp(prefix="freshet-airflow-tests-")
    atexit.register(shutil.rmtree, os.environ["AIRFLOW_HOME"], ignore_errors=True)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
