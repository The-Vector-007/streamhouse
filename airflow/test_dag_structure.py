"""DAG validation. Runs in the Airflow venv, not the project venv.

Catches the failure that costs the most time in practice: a DAG that does not
import, which Airflow reports as a vague parse error in the UI long after you
have moved on.
"""

import os
import sys

os.environ.setdefault("AIRFLOW_HOME", os.path.join(os.path.dirname(__file__), "home"))

from airflow.models import DagBag  # noqa: E402

bag = DagBag(dag_folder=os.path.join(os.path.dirname(__file__), "dags"), include_examples=False)

failures = []
if bag.import_errors:
    failures.append(f"import errors: {bag.import_errors}")

dag = bag.get_dag("streamhouse_daily")
if dag is None:
    failures.append("streamhouse_daily not found")
else:
    tasks = {t.task_id for t in dag.tasks}
    if tasks != {"refine", "optimize", "vacuum"}:
        failures.append(f"unexpected tasks: {tasks}")
    downstream = {t.task_id: sorted(d.task_id for d in t.downstream_list) for t in dag.tasks}
    if downstream != {"refine": ["optimize"], "optimize": ["vacuum"], "vacuum": []}:
        failures.append(f"unexpected dependencies: {downstream}")
    if dag.max_active_runs != 1:
        failures.append("concurrent runs would let two refines overwrite silver at once")

if failures:
    for f in failures:
        print("FAIL:", f)
    sys.exit(1)

print(f"OK: streamhouse_daily parsed, {len(dag.tasks)} tasks, refine >> optimize >> vacuum")
print(f"    schedule={dag.schedule_interval} max_active_runs={dag.max_active_runs}")
