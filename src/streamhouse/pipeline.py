"""End-to-end medallion runner.

Two entry points, matching the two shapes of work:

    python -m streamhouse.pipeline ingest --seconds 30
    python -m streamhouse.pipeline refine

`ingest` is a streaming job: Kafka into bronze, running until you stop it.
`refine` is a batch job: bronze into silver and gold, plus the data-quality table.
Airflow (P8) drives `refine`; a human or a supervisor drives `ingest`.

The split is deliberate. Bronze has to be always-on to keep up with the log, while
silver and gold are recomputed from whatever bronze holds, which is what makes them
safe to retry.
"""

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import functions as F

from streamhouse.bronze.ingest import ingest_stream
from streamhouse.gold.marts import rebuild
from streamhouse.silver.refine import (
    QUARANTINE_RULES,
    DataQualityBreach,
    breaches,
    dq_results,
    to_silver,
)
from streamhouse.spark import build_session

DEFAULT_WAREHOUSE = Path(__file__).resolve().parents[2] / "warehouse"


class Layout:
    """Where each table lives. One place, so nothing reinvents a path."""

    def __init__(self, root: Path | str):
        self.root = Path(root)

    @property
    def bronze(self) -> str:
        return str(self.root / "bronze")

    @property
    def bronze_dlq(self) -> str:
        return str(self.root / "bronze_dlq")

    @property
    def silver(self) -> str:
        return str(self.root / "silver")

    @property
    def quarantine(self) -> str:
        return str(self.root / "quarantine")

    @property
    def dq_results(self) -> str:
        return str(self.root / "dq_results")

    @property
    def gold(self) -> str:
        return str(self.root / "gold_daily_account_totals")

    @property
    def checkpoints(self) -> str:
        return str(self.root / "_checkpoints" / "bronze")


def refine(
    spark,
    layout: Layout,
    *,
    run_id: str | None = None,
    thresholds: dict[str, float] | None = None,
) -> dict:
    """Bronze to silver to gold, plus the DQ table. Idempotent end to end.

    Silver and quarantine are overwritten rather than appended: they are a pure
    function of bronze, so rewriting them is both correct and the only way a rerun
    after a bug fix produces the right answer instead of a doubled one.

    The DQ table is the exception. It appends, because it is a time series: the
    point is to see the failure rate move.
    """
    run = run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    bronze = spark.read.format("delta").load(layout.bronze)

    silver, quarantined = to_silver(bronze)
    silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        layout.silver
    )
    quarantined.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        layout.quarantine
    )

    # Re-read so the counts below describe what actually landed, not what the
    # planner thinks it would have written.
    silver_written = spark.read.format("delta").load(layout.silver)
    quarantine_written = spark.read.format("delta").load(layout.quarantine)

    results = dq_results(
        source=bronze, quarantined=quarantine_written, run_id=run, thresholds=thresholds
    )
    # mergeSchema because this table is a long-lived time series: checks get added
    # and gain columns (threshold arrived after the first runs), and history
    # written under the old shape has to stay readable alongside the new.
    results.write.format("delta").mode("append").option("mergeSchema", "true").save(
        layout.dq_results
    )

    # Write the DQ rows first, then decide. If the gate trips, the evidence of why
    # must already be on disk: an operator woken by a failed task needs the failure
    # rates, and a run that raises before recording them tells them nothing. The
    # check re-reads the table so it runs against what actually landed.
    written_results = spark.read.format("delta").load(layout.dq_results)
    failed_checks = breaches(written_results.where(F.col("run_id") == run))

    dates = rebuild(silver_written, layout.gold)

    summary = {
        "run_id": run,
        "bronze_rows": bronze.count(),
        "silver_rows": silver_written.count(),
        "quarantined_rows": quarantine_written.count(),
        "gold_dates": [str(d) for d in dates],
        "dq_breaches": failed_checks,
    }

    if failed_checks:
        # Gold is rebuilt before this raises, deliberately. The rows that reached
        # silver are valid; what is in question is whether enough of them arrived.
        # Leaving gold stale would turn a quality alert into a second outage.
        raise DataQualityBreach(
            f"run {run}: {len(failed_checks)} check(s) over threshold: {failed_checks}"
        )
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the streamhouse medallion.")
    parser.add_argument("command", choices=["ingest", "refine"])
    parser.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    parser.add_argument("--seconds", type=int, default=30, help="ingest: how long to run")
    parser.add_argument("--run-id", default=None, help="refine: label for the DQ rows")
    parser.add_argument(
        "--dq-threshold",
        type=float,
        default=None,
        help="refine: max failure rate per check before the run fails. Lower it to "
        "tighten the gate, raise it to push a known-bad backfill through deliberately.",
    )
    args = parser.parse_args(argv)

    layout = Layout(args.warehouse)
    spark = build_session(f"streamhouse-{args.command}")

    try:
        if args.command == "ingest":
            query = ingest_stream(
                spark,
                bronze_path=layout.bronze,
                dlq_path=layout.bronze_dlq,
                checkpoint_path=layout.checkpoints,
            )
            query.awaitTermination(timeout=args.seconds)
            query.stop()
            print(f"ingest ran for {args.seconds}s into {layout.bronze}")
        else:
            try:
                override = (
                    {name: args.dq_threshold for name, _ in QUARANTINE_RULES}
                    if args.dq_threshold is not None
                    else None
                )
                for key, value in refine(
                    spark, layout, run_id=args.run_id, thresholds=override
                ).items():
                    print(f"{key}: {value}")
            except DataQualityBreach as exc:
                # Non-zero exit, and a readable one. Airflow's BashOperator fails on
                # the exit code, and whoever opens the task log should see the rates
                # rather than a traceback through Spark internals.
                print(f"DATA QUALITY GATE FAILED: {exc}", file=sys.stderr)
                return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
