"""Bronze-shaped test rows.

Not named test_* on purpose, so pytest does not try to collect it.

Silver and gold tests need precise control over amounts, currencies, offsets and
event times, which is awkward to get out of the generator. This builds bronze rows
directly, with defaults for everything a test does not care about.
"""

from datetime import UTC, datetime, timedelta

BRONZE_SCHEMA = (
    "transaction_id string, account_id string, counterparty_id string, "
    "amount_minor long, currency string, status string, "
    "event_time timestamp, produced_at timestamp, raw_payload string, "
    "kafka_key string, kafka_topic string, kafka_partition int, kafka_offset long, "
    "kafka_timestamp timestamp, ingested_at timestamp, ingest_date date"
)

INGESTED_AT = datetime(2026, 9, 24, 12, 0, tzinfo=UTC)


def bronze_row(offset: int, **overrides):
    """One bronze row. Everything is overridable; the defaults are valid."""
    ingested_at = overrides.pop("ingested_at", INGESTED_AT)
    event_time = overrides.pop("event_time", ingested_at - timedelta(seconds=5))
    fields = {
        "transaction_id": f"txn_{offset:06d}",
        "account_id": "acc_0001",
        "counterparty_id": "acc_0002",
        "amount_minor": 12_500,
        "currency": "INR",
        "status": "SETTLED",
        "event_time": event_time,
        "produced_at": event_time,
        "raw_payload": "{}",
        "kafka_key": "acc_0001",
        "kafka_topic": "txn.raw",
        "kafka_partition": offset % 3,
        "kafka_offset": offset,
        "kafka_timestamp": ingested_at,
        "ingested_at": ingested_at,
        "ingest_date": ingested_at.date(),
    }
    fields.update(overrides)
    return tuple(fields[name.split()[0]] for name in BRONZE_SCHEMA.split(", "))


def bronze_frame(spark, rows):
    return spark.createDataFrame(rows, BRONZE_SCHEMA)
