"""P8 - the daily pipeline: refine, compact, expire.

Airflow orchestrates; it does not compute. Every task shells out to the project's
own venv, because Spark needs Java 21 and a pinned pyspark that Airflow's
dependency set would fight with. Keeping them in separate environments means an
Airflow upgrade cannot break the pipeline and vice versa, and it matches the
project's existing rule that Spark runs on the host rather than in a container.

Why these three tasks, in this order:

  refine    bronze -> silver -> gold, recomputed from whatever bronze holds
  optimize  compact the small files streaming ingestion leaves behind
  vacuum    drop files no retained version references any more

`refine` is idempotent by construction: silver and gold are pure functions of
bronze, so a retry produces the same tables rather than doubled ones. That is what
makes the whole DAG safe to backfill. `optimize` on a compacted table is a no-op,
and `vacuum` only removes unreferenced files, so both are safe to repeat too.

Backfill: every task takes the logical date as its run id, so
`airflow dags backfill` over a date range labels the data-quality rows per day
instead of collapsing them into one run.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.environ.get("STREAMHOUSE_HOME", "/home/ajay/job-switch/projects/streamhouse")

# `mise exec` supplies JAVA_HOME. Without it PySpark cannot start a JVM, and the
# failure message blames Spark rather than the missing toolchain.
RUN = f"cd {PROJECT_DIR} && mise exec -- uv run python -m streamhouse"

default_args = {
    "owner": "streamhouse",
    # Retries are free precisely because every task is idempotent. If that stops
    # being true, this line becomes a data-corruption bug.
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="streamhouse_daily",
    description="Refine bronze into silver and gold, then compact and expire files.",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule="0 2 * * *",
    # Backfills run one day at a time. Concurrent runs would have two refines
    # overwriting silver simultaneously, and last-writer-wins is not a plan.
    max_active_runs=1,
    catchup=False,
    tags=["streamhouse", "medallion"],
) as dag:
    refine = BashOperator(
        task_id="refine",
        bash_command=f"{RUN}.pipeline refine --run-id {{{{ ds }}}}",
    )

    optimize = BashOperator(
        task_id="optimize",
        bash_command=f"{RUN}.maintenance optimize --tables all",
    )

    vacuum = BashOperator(
        task_id="vacuum",
        # Default retention, deliberately. A shorter window would make the table
        # cheaper and time travel useless, and the whole point of Delta here is
        # that history is queryable.
        bash_command=f"{RUN}.maintenance vacuum --tables all",
    )

    # Compact before expiring: OPTIMIZE creates the new files and tombstones the
    # old ones, so running VACUUM first would have nothing to collect and the
    # small files would survive another day.
    refine >> optimize >> vacuum
