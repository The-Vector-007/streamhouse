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

from streamhouse.bronze.ingest import ingest_stream
from streamhouse.gold.marts import rebuild
from streamhouse.silver.refine import dq_results, to_silver
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


def refine(spark, layout: Layout, *, run_id: str | None = None) -> dict:
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

    dq_results(source=bronze, quarantined=quarantine_written, run_id=run).write.format(
        "delta"
    ).mode("append").save(layout.dq_results)

    dates = rebuild(silver_written, layout.gold)

    return {
        "run_id": run,
        "bronze_rows": bronze.count(),
        "silver_rows": silver_written.count(),
        "quarantined_rows": quarantine_written.count(),
        "gold_dates": [str(d) for d in dates],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the streamhouse medallion.")
    parser.add_argument("command", choices=["ingest", "refine"])
    parser.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    parser.add_argument("--seconds", type=int, default=30, help="ingest: how long to run")
    parser.add_argument("--run-id", default=None, help="refine: label for the DQ rows")
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
            for key, value in refine(spark, layout, run_id=args.run_id).items():
                print(f"{key}: {value}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
