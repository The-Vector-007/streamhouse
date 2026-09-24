"""P4 - silver: validation, dedup, quarantine, and data-quality results.

Bronze asked "is this parseable?". Silver asks "is this true?".

Three jobs, in this order:

1. **Deduplicate.** A client retry after a timeout puts the same transaction_id on
   the log twice. Both copies are individually valid, so nothing upstream can see
   the problem. Dedup is a transformation, not a quality check: there is no defect
   to report, just a row to collapse.
2. **Validate.** Amount, currency and status are checked against the domain rules.
   Failures are *quarantined*, never dropped, because a row you deleted is a row
   you cannot explain to whoever asks about it next week.
3. **Report.** Every run writes a data-quality row per check, so the failure rate
   is queryable history rather than something you re-derive by hand during an
   incident.

Lateness is a property, not a defect. A late event is flagged and kept; the
restatement cost lands in gold, which is where recomputing an aggregate is cheap.
"""

from datetime import UTC, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from streamhouse.domain.transaction import VALID_CURRENCIES, TransactionStatus

# How far behind its ingest time an event may be before it is flagged late. Chosen
# to match the generator's late band, which starts at 5 minutes.
DEFAULT_LATENESS_SECONDS = 300


def _sql_list(values) -> str:
    return ", ".join(f"'{v}'" for v in sorted(values))


# Business rules, as SQL strings rather than Column objects. Two reasons: a Column
# cannot be built before a SparkContext exists, so a module-level tuple of them
# blows up at import; and a string is the same expression the quarantine reason and
# the DQ check both use, so the two can never drift apart.
#
# Each predicate is true when the row is BAD. Note the explicit IS NULL arms: SQL
# three-valued logic makes `currency NOT IN (...)` evaluate to NULL, not true, when
# currency is null, so a null would slip through as clean without them.
QUARANTINE_RULES: tuple[tuple[str, str], ...] = (
    ("non_positive_amount", "amount_minor IS NULL OR amount_minor <= 0"),
    (
        "unknown_currency",
        f"currency IS NULL OR currency NOT IN ({_sql_list(VALID_CURRENCIES)})",
    ),
    (
        "unknown_status",
        f"status IS NULL OR status NOT IN ({_sql_list(s.value for s in TransactionStatus)})",
    ),
    ("missing_counterparty", "counterparty_id IS NULL"),
)


def deduplicate(bronze: DataFrame) -> DataFrame:
    """Collapse retries, keeping the copy that reached the log first.

    Ordering by (kafka_partition, kafka_offset) rather than by a timestamp is
    deliberate: offsets are a total order within a partition and never tie, while
    two retries can share a millisecond and leave the winner up to chance. A
    non-deterministic dedup makes every downstream count irreproducible.
    """
    first_arrival = Window.partitionBy("transaction_id").orderBy(
        F.col("kafka_partition").asc(), F.col("kafka_offset").asc()
    )
    return (
        bronze.withColumn("_arrival", F.row_number().over(first_arrival))
        .where(F.col("_arrival") == 1)
        .drop("_arrival")
    )


def flag_lateness(df: DataFrame, *, lateness_seconds: int = DEFAULT_LATENESS_SECONDS) -> DataFrame:
    """Mark how far behind ingest each event was, and whether that crosses the bound.

    Kept, not dropped. Dropping late data is how a pipeline reports numbers that
    are clean, stable and quietly wrong.
    """
    lag = F.unix_timestamp("ingested_at") - F.unix_timestamp("event_time")
    return df.withColumn("lateness_seconds", lag.cast("long")).withColumn(
        "is_late", F.col("lateness_seconds") > F.lit(lateness_seconds)
    )


def validate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split on the business rules. Returns (clean, quarantined).

    A quarantined row carries every reason it failed, not just the first, because
    fixing one upstream bug should not reveal a second one a week later.
    """
    reasons = F.concat_ws(
        ",", *[F.when(F.expr(predicate), F.lit(name)) for name, predicate in QUARANTINE_RULES]
    )
    tagged = df.withColumn("quarantine_reason", reasons)

    clean = tagged.where(F.col("quarantine_reason") == "").drop("quarantine_reason")
    quarantined = tagged.where(F.col("quarantine_reason") != "")
    return clean, quarantined


def to_silver(
    bronze: DataFrame, *, lateness_seconds: int = DEFAULT_LATENESS_SECONDS
) -> tuple[DataFrame, DataFrame]:
    """The whole silver transform. Returns (silver, quarantine).

    Dedup runs before validation so that a retry of an invalid record is
    quarantined once rather than three times, which would make the failure rate
    depend on how flaky the client's network was.
    """
    deduped = flag_lateness(deduplicate(bronze), lateness_seconds=lateness_seconds)
    return validate(deduped)


def dq_results(
    *,
    source: DataFrame,
    quarantined: DataFrame,
    table: str = "silver.transactions",
    run_id: str | None = None,
    checked_at: datetime | None = None,
) -> DataFrame:
    """One row per rule per run: how many were checked, how many failed.

    This is the artifact that turns "data quality" from a claim into a time series.
    It is also what lakehouse-mcp's dq_results tool reads, so an agent can answer
    "did last night's load degrade?" without anyone opening a notebook.
    """
    spark = source.sparkSession
    run = run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    stamp = checked_at or datetime.now(UTC)
    total = source.count()

    rows = []
    for name, predicate in QUARANTINE_RULES:
        failed = quarantined.where(F.expr(predicate)).count()
        rows.append(
            (
                run,
                table,
                name,
                stamp,
                total,
                failed,
                # Guard the divide: an empty batch is not a 100% failure rate, and
                # a NaN here would poison every downstream average.
                float(failed) / total if total else 0.0,
                failed == 0,
            )
        )

    return spark.createDataFrame(
        rows,
        "run_id string, table_name string, check_name string, checked_at timestamp, "
        "rows_checked long, rows_failed long, failure_rate double, passed boolean",
    )
