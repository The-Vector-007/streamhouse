"""P3.2 — streaming ingest into bronze.

You write: src/streamhouse/bronze/ingest.py

Bronze enforces *structure*, not *meaning*. A negative amount belongs in bronze
untouched; silver is where it gets rejected. Bronze only asks "is this parseable?"
Filtering too early destroys the audit trail, which is the whole reason bronze
exists.
"""

import pytest


@pytest.mark.spark
def test_writes_raw_records_unchanged(spark, warehouse, checkpoints):
    """Business-invalid records must survive bronze. That is the point."""
    pytest.skip("implement in P3.2")


@pytest.mark.spark
def test_adds_ingest_metadata_columns(spark, warehouse, checkpoints):
    """Bronze adds provenance: kafka partition, offset, ingest timestamp.

    Without offsets in the table you cannot answer "where did this row come
    from?" during an incident, and that question always gets asked.
    """
    pytest.skip("implement in P3.2")


@pytest.mark.spark
def test_unparseable_payload_goes_to_dlq_not_the_stream_floor(spark, warehouse, checkpoints):
    """Malformed JSON must not kill the stream, and must not vanish silently."""
    pytest.skip("implement in P3.2")


@pytest.mark.spark
def test_partitioned_by_ingest_date(spark, warehouse, checkpoints):
    """Partition by ingest date, not event date.

    Event date would rewrite old partitions when late data arrives, which turns
    an append into a rewrite. Be ready to explain that trade-off.
    """
    pytest.skip("implement in P3.2")
