"""P3.2 — streaming ingest into bronze.

Implemented in: src/streamhouse/bronze/ingest.py

Bronze enforces *structure*, not *meaning*. A negative amount belongs in bronze
untouched; silver is where it gets rejected. Bronze only asks "is this parseable?"
Filtering too early destroys the audit trail, which is the whole reason bronze
exists.

These run against a Kafka-shaped DataFrame rather than a live broker, so the
parsing and provenance rules are covered without `make up`. The end-to-end path
is exercised in test_bronze_exactly_once.py.
"""

import json
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streamhouse.bronze.ingest import (
    PARTITION_COLUMN,
    split_parseable,
    to_bronze,
    write_batch_idempotent,
)
from streamhouse.generator.produce import DefectRates, generate_payloads

# The column shape Spark's Kafka source produces. Building it by hand keeps these
# tests broker-free while still feeding to_bronze() exactly what it sees in prod.
KAFKA_SOURCE_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


def _kafka_frame(spark, payloads: list[bytes]):
    now = datetime.now(UTC)
    rows = [(b"acc_0001", payload, "txn.raw", i % 6, i, now) for i, payload in enumerate(payloads)]
    return spark.createDataFrame(rows, KAFKA_SOURCE_SCHEMA)


@pytest.mark.spark
def test_writes_raw_records_unchanged(spark, warehouse, checkpoints):
    """Business-invalid records must survive bronze. That is the point."""
    payloads = generate_payloads(
        200, seed=5, defects=DefectRates(negative_amount=0.1, unknown_currency=0.1)
    )
    good, dead = split_parseable(to_bronze(_kafka_frame(spark, payloads)))

    rows = good.collect()
    # The offending records are business-invalid, not structurally broken, so
    # every one of them has to be here rather than in the DLQ.
    assert any(r.amount_minor < 0 for r in rows), "negative amounts were filtered out of bronze"
    assert any(r.currency == "XYZ" for r in rows), "unknown currency was filtered out of bronze"
    assert dead.count() == 0, "nothing here is structurally broken, so the DLQ should be empty"

    # And the original bytes survive verbatim, which is what makes bronze an
    # audit trail rather than a first pass at cleaning.
    sent = {json.dumps(json.loads(p), sort_keys=True) for p in payloads}
    stored = {json.dumps(json.loads(r.raw_payload), sort_keys=True) for r in rows}
    assert stored == sent


@pytest.mark.spark
def test_adds_ingest_metadata_columns(spark, warehouse, checkpoints):
    """Bronze adds provenance: kafka partition, offset, ingest timestamp.

    Without offsets in the table you cannot answer "where did this row come
    from?" during an incident, and that question always gets asked.
    """
    payloads = generate_payloads(50, seed=6, defects=DefectRates())
    stamp = datetime(2026, 9, 24, 12, 0, tzinfo=UTC)
    bronze = to_bronze(_kafka_frame(spark, payloads), ingested_at=stamp)

    for column in (
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "ingested_at",
    ):
        assert column in bronze.columns, f"lost provenance column {column}"

    rows = sorted(bronze.collect(), key=lambda r: r.kafka_offset)
    assert [r.kafka_offset for r in rows] == list(range(50))
    assert {r.kafka_partition for r in rows} == set(range(6))
    assert all(r.kafka_topic == "txn.raw" for r in rows)
    # One stamp for the whole batch, not one per row. A per-row clock call would
    # scatter a midnight-straddling batch across two date partitions.
    assert len({r.ingested_at for r in rows}) == 1
    # The producer's stamp is kept separately; the gap between them is ingest lag.
    assert all(r.produced_at is not None for r in rows)


@pytest.mark.spark
def test_unparseable_payload_goes_to_dlq_not_the_stream_floor(spark, warehouse, checkpoints):
    """Malformed JSON must not kill the stream, and must not vanish silently."""
    clean = generate_payloads(20, seed=7, defects=DefectRates())
    broken = [
        b"{not json at all",
        b"",
        json.dumps(
            {
                "transaction_id": None,
                "account_id": "acc_1",
                "event_time": "2026-09-24T00:00:00+00:00",
            }
        ).encode(),
        json.dumps({"account_id": "acc_2"}).encode(),
    ]

    good, dead = split_parseable(to_bronze(_kafka_frame(spark, clean + broken)))

    assert good.count() == 20
    assert dead.count() == len(broken), "a malformed payload was dropped instead of dead-lettered"
    # Nothing vanishes: every input row leaves in exactly one of the two frames.
    assert good.count() + dead.count() == len(clean) + len(broken)

    # The DLQ has to say why, or triage means re-deriving it by hand.
    reasons = [r.dlq_reason for r in dead.collect()]
    assert all("missing:" in reason for reason in reasons)
    # And it keeps the bytes, so the record can be replayed once the bug is fixed.
    assert all(r.raw_payload is not None for r in dead.collect())


@pytest.mark.spark
def test_partitioned_by_ingest_date(spark, warehouse, checkpoints):
    """Partition by ingest date, not event date.

    Event date would rewrite old partitions when late data arrives, which turns
    an append into a rewrite. Be ready to explain that trade-off.
    """
    # Lateness of 40 days guarantees event date and ingest date disagree, which is
    # the only way to tell the two partitioning choices apart.
    payloads = generate_payloads(30, seed=8, defects=DefectRates())
    ingested_at = datetime.now(UTC)
    bronze = to_bronze(_kafka_frame(spark, payloads), ingested_at=ingested_at)

    stale = bronze.withColumn(
        "event_time",
        bronze.event_time - F.expr("INTERVAL 40 DAYS"),
    )
    path = str(warehouse / "bronze")
    write_batch_idempotent(stale, path, app_id="test", version=1, partition_by=(PARTITION_COLUMN,))

    written = spark.read.format("delta").load(path)
    partitions = {
        r[PARTITION_COLUMN] for r in written.select(PARTITION_COLUMN).distinct().collect()
    }
    event_dates = {r.event_time.date() for r in written.select("event_time").collect()}

    assert partitions == {ingested_at.date()}, "partitioned by something other than ingest date"
    assert partitions.isdisjoint(event_dates), (
        "partition value tracks event date; late data would rewrite old partitions"
    )
    # 40 days back really is a different date, so the assertion above has teeth.
    assert min(event_dates) < ingested_at.date() - timedelta(days=30)
