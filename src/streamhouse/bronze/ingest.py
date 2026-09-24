"""P3.2 - bronze ingestion from Kafka into Delta.

Bronze enforces *structure*, not *meaning*.

A negative amount and an unknown currency parse cleanly, so they land in bronze
untouched and silver rejects them later. A payload that is not JSON, or that is
missing a field the pipeline needs to identify a row, cannot be reasoned about at
all, so it goes to a dead-letter table instead. Filtering business-invalid rows
here would destroy the audit trail, which is the entire reason bronze exists:
when someone asks "did we receive it?", bronze has to be able to answer yes even
when the record was garbage.

The exactly-once mechanism lives in write_batch_idempotent(). Kafka gives
at-least-once delivery and Delta gives idempotent writes; together that is
effectively-once processing. See docs/BRIEFS/p3-exactly-once.md.
"""

from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streamhouse.generator.topics import BOOTSTRAP_SERVERS

RAW_TOPIC = "txn.raw"

# The wire contract, mirroring Transaction.to_bytes(). Declaring it explicitly
# rather than inferring it is the schema enforcement: a producer that starts
# sending a new shape does not silently widen the bronze table, and a field that
# stops arriving shows up as null instead of vanishing.
WIRE_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("account_id", StringType()),
        StructField("counterparty_id", StringType()),
        StructField("amount_minor", LongType()),
        StructField("currency", StringType()),
        StructField("status", StringType()),
        StructField("event_time", TimestampType()),
        StructField("ingest_time", TimestampType()),
    ]
)

# Without these a row cannot be identified, deduplicated or windowed, so silver
# has nothing to work with. Note what is absent: amount and currency are *not*
# structural. A wrong amount is still a row we received.
REQUIRED_FIELDS = ("transaction_id", "account_id", "event_time")

PARTITION_COLUMN = "ingest_date"


def read_kafka_stream(
    spark: SparkSession,
    *,
    topic: str = RAW_TOPIC,
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
    starting_offsets: str = "earliest",
    max_offsets_per_trigger: int | None = None,
) -> DataFrame:
    """The raw Kafka stream, before any parsing.

    Note there is no consumer group here. Spark tracks offsets in its own
    checkpoint rather than committing them back to Kafka, because the offset and
    the data it produced have to advance together or exactly-once is impossible.
    """
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        # Spark should not die because a topic was recreated under it; that is an
        # operational event, not a data error.
        .option("failOnDataLoss", "false")
    )
    if max_offsets_per_trigger is not None:
        # Caps how much of the backlog one micro-batch swallows. Also the knob that
        # decides where batch boundaries fall, which matters more than it looks:
        # see test_deleting_the_checkpoint_can_silently_lose_data.
        reader = reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)
    return reader.load()


def to_bronze(raw: DataFrame, *, ingested_at: datetime | None = None) -> DataFrame:
    """Parse the payload and attach provenance, keeping the original bytes.

    `ingested_at` is stamped once per batch by the caller rather than with
    current_timestamp() per row. Two reasons: a batch that straddles midnight
    would otherwise write into two date partitions, and a row-level clock call is
    a non-deterministic transformation, which is exactly the thing that quietly
    breaks replay reasoning.
    """
    stamp = F.lit(ingested_at or datetime.now(UTC)).cast(TimestampType())
    payload = F.from_json(F.col("value").cast(StringType()), WIRE_SCHEMA)

    return (
        raw.withColumn("payload", payload)
        .select(
            F.col("payload.transaction_id").alias("transaction_id"),
            F.col("payload.account_id").alias("account_id"),
            F.col("payload.counterparty_id").alias("counterparty_id"),
            F.col("payload.amount_minor").alias("amount_minor"),
            F.col("payload.currency").alias("currency"),
            F.col("payload.status").alias("status"),
            F.col("payload.event_time").alias("event_time"),
            # The producer's own stamp, kept distinct from ours. The gap between
            # them is the pipeline's ingestion lag.
            F.col("payload.ingest_time").alias("produced_at"),
            # The raw bytes, unchanged. This column is why bronze can answer
            # "what exactly did we receive?" after the parse rules change.
            F.col("value").cast(StringType()).alias("raw_payload"),
            F.col("key").cast(StringType()).alias("kafka_key"),
            # Provenance. Without partition and offset in the table you cannot
            # trace a row back to its position in the log during an incident.
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            stamp.alias("ingested_at"),
        )
        # Partition by ingest date, never event date. Late data would reopen old
        # event-date partitions, turning every append into a rewrite of history.
        .withColumn(PARTITION_COLUMN, F.to_date(F.col("ingested_at")))
    )


def split_parseable(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separate rows bronze can stand behind from rows it cannot.

    Returns (good, dead_letters). Nothing is dropped: every input row leaves in
    exactly one of the two frames, so a malformed payload neither kills the
    stream nor disappears.
    """
    missing = F.lit(False)
    for field in REQUIRED_FIELDS:
        missing = missing | F.col(field).isNull()

    good = bronze.where(~missing)
    dead = bronze.where(missing).withColumn(
        "dlq_reason",
        F.concat_ws(
            ",",
            *[
                F.when(F.col(field).isNull(), F.lit(f"missing:{field}"))
                for field in REQUIRED_FIELDS
            ],
        ),
    )
    return good, dead


def write_batch_idempotent(
    df: DataFrame,
    path: str,
    *,
    app_id: str,
    version: int,
    partition_by: tuple[str, ...] = (),
) -> None:
    """Append `df` to a Delta table, at most once for a given (app_id, version).

    This is the whole exactly-once story in four lines.

    Delta records the highest version it has committed per txnAppId in the
    transaction log. A replay of the same batch carries the same version, Delta
    sees it is not greater than what is already committed, and skips the write
    entirely. That is why at-least-once delivery from Kafka still produces
    effectively-once results.

    Omitting these two options is the classic foreachBatch bug: everything looks
    correct until a driver dies mid-batch, and then the replay silently writes a
    second copy.
    """
    writer = df.write.format("delta").mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.option("txnAppId", app_id).option("txnVersion", version).save(path)


def ingest_stream(
    spark: SparkSession,
    *,
    bronze_path: str,
    dlq_path: str,
    checkpoint_path: str,
    topic: str = RAW_TOPIC,
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
    starting_offsets: str = "earliest",
    app_id: str = "bronze",
    trigger: dict | None = None,
) -> StreamingQuery:
    """Start the bronze ingest query. Returns immediately with the handle.

    foreachBatch is used because one micro-batch has to fan out to two tables,
    bronze and the DLQ, and a plain sink writes one. That choice is what makes
    the txnVersion discipline in write_batch_idempotent() load-bearing.
    """

    def process(batch: DataFrame, batch_id: int) -> None:
        # One clock reading per batch, taken on the driver. See to_bronze().
        bronze = to_bronze(batch, ingested_at=datetime.now(UTC))
        good, dead = split_parseable(bronze)

        # batch_id is Spark's monotonic micro-batch number and is stable across a
        # replay of that batch, which is exactly the property txnVersion needs.
        write_batch_idempotent(
            good, bronze_path, app_id=app_id, version=batch_id, partition_by=(PARTITION_COLUMN,)
        )
        # A separate app_id, so a DLQ write never advances the bronze table's
        # committed version, and vice versa.
        write_batch_idempotent(
            dead,
            dlq_path,
            app_id=f"{app_id}.dlq",
            version=batch_id,
            partition_by=(PARTITION_COLUMN,),
        )

    raw = read_kafka_stream(
        spark,
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        starting_offsets=starting_offsets,
    )
    writer = raw.writeStream.foreachBatch(process).option("checkpointLocation", checkpoint_path)
    if trigger:
        writer = writer.trigger(**trigger)
    return writer.start()
