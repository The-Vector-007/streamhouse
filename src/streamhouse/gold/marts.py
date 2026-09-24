"""P5 - gold: aggregate marts, recomputed rather than accumulated.

The interview line for this layer: **most "incremental" pipelines are secretly not
incremental.** They append a delta per run and assume nothing that already landed
will ever change. Then a late event arrives for last Tuesday, the append adds a row
Tuesday's total never accounted for, and the mart drifts away from its source with
nothing to signal it.

This layer recomputes whole date partitions and replaces them. Running it twice for
the same date produces the same table, which is what makes a backfill safe and what
lets Airflow retry a task without anyone thinking about it.

That is where the restatement cost of late data lands, and it is the cheapest place
to put it: recomputing one day of aggregates is seconds, while trying to patch an
aggregate in place is a correctness problem forever.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PARTITION_COLUMN = "event_date"


def daily_account_totals(silver: DataFrame) -> DataFrame:
    """Per account, per currency, per event date.

    Grouped by event date, not ingest date. A mart answers business questions, and
    "how much did this account move on Tuesday" means Tuesday's events, wherever
    they happened to arrive. Bronze partitions by ingest date for write mechanics;
    gold groups by event date for meaning. Those are different jobs.
    """
    return (
        silver.withColumn(PARTITION_COLUMN, F.to_date("event_time"))
        .groupBy(PARTITION_COLUMN, "account_id", "currency")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount_minor").alias("total_amount_minor"),
            F.sum(F.when(F.col("status") == "SETTLED", 1).otherwise(0)).alias("settled_count"),
            # Carried through so a consumer can tell a restated day from a quiet
            # one without joining back to silver.
            F.sum(F.when(F.col("is_late"), 1).otherwise(0)).alias("late_count"),
            F.max("ingested_at").alias("last_ingested_at"),
        )
    )


def write_mart(df: DataFrame, path: str, *, dates: list | None = None) -> None:
    """Write a mart, replacing whole date partitions rather than appending.

    `replaceWhere` makes the write atomic per partition: the old rows for those
    dates and the new ones swap in a single Delta commit, so a reader never sees a
    half-rebuilt day. Appending here would double every total on the second run.

    With no `dates`, this is a full overwrite, which is correct but expensive.
    Callers that know which days changed should say so.
    """
    writer = df.write.format("delta").mode("overwrite").partitionBy(PARTITION_COLUMN)

    if dates:
        predicate = " OR ".join(
            f"{PARTITION_COLUMN} = '{d}'" for d in sorted({str(d) for d in dates})
        )
        writer = writer.option("replaceWhere", predicate)

    writer.save(path)


def affected_dates(silver: DataFrame) -> list:
    """Which event dates this batch touched, so a caller can scope the rebuild.

    Late data is exactly why this is not just "today": a batch ingested this
    morning can carry events from any day the watermark still allows.
    """
    rows = (
        silver.select(F.to_date("event_time").alias(PARTITION_COLUMN))
        .distinct()
        .orderBy(PARTITION_COLUMN)
        .collect()
    )
    return [r[PARTITION_COLUMN] for r in rows]


def rebuild(silver: DataFrame, path: str, *, dates: list | None = None) -> list:
    """Recompute the mart for every date this batch touched. Returns those dates.

    The whole point: call it once or call it five times, the table is the same
    afterwards.
    """
    targets = dates if dates is not None else affected_dates(silver)
    if not targets:
        return []

    scoped = silver.where(F.to_date("event_time").isin(targets))
    write_mart(daily_account_totals(scoped), path, dates=targets)
    return targets
