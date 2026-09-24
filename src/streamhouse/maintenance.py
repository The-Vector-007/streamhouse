"""P8 - table maintenance: compaction and file expiry.

Streaming writes produce many small files. One micro-batch every few seconds means
thousands of parquet files a day, and a query then spends its time opening files
rather than reading rows. This is the chore that keeps a streaming lakehouse
queryable, and it is the reason a pipeline needs an orchestrator at all: it is
periodic work that is not part of any single batch.

Both operations are safe to repeat. OPTIMIZE on an already-compacted table is a
no-op, and VACUUM only removes files no live version references. That is what lets
Airflow retry either one without anyone reasoning about it.
"""

import argparse
import sys

from delta.tables import DeltaTable

from streamhouse.pipeline import DEFAULT_WAREHOUSE, Layout
from streamhouse.spark import build_session

# Delta's default. Seven days of tombstoned files exist so that a reader which
# started before a rewrite can still finish, and so time travel has somewhere to
# travel to. Shortening it trades recoverability for disk.
DEFAULT_RETAIN_HOURS = 168


def optimize(spark, path: str) -> dict:
    """Compact small files into fewer, larger ones."""
    table = DeltaTable.forPath(spark, path)
    before = len(spark.read.format("delta").load(path).inputFiles())
    table.optimize().executeCompaction()
    after = len(spark.read.format("delta").load(path).inputFiles())
    return {"path": path, "files_before": before, "files_after": after}


def vacuum(spark, path: str, *, retain_hours: int = DEFAULT_RETAIN_HOURS) -> dict:
    """Delete files no longer referenced by any retained version.

    Below the default retention Delta refuses, because deleting files a live
    reader still needs corrupts that reader. The check is only disabled when a
    caller explicitly asks for a shorter window, and that is a decision worth
    making deliberately rather than by default.
    """
    if retain_hours < DEFAULT_RETAIN_HOURS:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    DeltaTable.forPath(spark, path).vacuum(retain_hours)
    return {"path": path, "retained_hours": retain_hours}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compact and expire Delta files.")
    parser.add_argument("command", choices=["optimize", "vacuum"])
    parser.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    parser.add_argument("--retain-hours", type=int, default=DEFAULT_RETAIN_HOURS)
    parser.add_argument(
        "--tables",
        default="bronze,silver,gold",
        help="Comma-separated layer names, or 'all'.",
    )
    args = parser.parse_args(argv)

    layout = Layout(args.warehouse)
    targets = {
        "bronze": layout.bronze,
        "bronze_dlq": layout.bronze_dlq,
        "silver": layout.silver,
        "quarantine": layout.quarantine,
        "gold": layout.gold,
        "dq_results": layout.dq_results,
    }
    chosen = list(targets) if args.tables == "all" else args.tables.split(",")

    spark = build_session(f"streamhouse-{args.command}")
    try:
        for name in chosen:
            path = targets.get(name.strip())
            if not path:
                print(f"skipping unknown table: {name}")
                continue
            action = optimize if args.command == "optimize" else vacuum
            kwargs = {} if args.command == "optimize" else {"retain_hours": args.retain_hours}
            try:
                print(action(spark, path, **kwargs))
            except Exception as exc:  # noqa: BLE001 - one missing table must not fail the run
                print(f"{name}: skipped ({type(exc).__name__}: {exc})")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
