"""P3.3 — failure injection. The test that makes the README honest.

Anyone can set `checkpointLocation` and write "exactly-once" in a README. Almost
nobody kills the write mid-batch and asserts the row count. That difference is
this file, and it is the most interview-valuable artifact in the repo.

The claim, said precisely: **at-least-once delivery plus idempotent writes equals
effectively-once processing.** Kafka will hand the same records over twice after a
failure. Delta refuses to apply the same (txnAppId, txnVersion) twice. Neither
half is sufficient alone.

One test here departs from the original spec. The spec assumed that deleting the
checkpoint duplicates data; it does not, because Delta skips any version at or
below the highest it has committed for that app id. The real hazard is silent
data *loss*, which is worse and more interesting. See docs/DECISIONS.md.
"""

import shutil
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from pyspark.errors import StreamingQueryException
from pyspark.sql import functions as F

from streamhouse.bronze.ingest import (
    PARTITION_COLUMN,
    read_kafka_stream,
    split_parseable,
    to_bronze,
    write_batch_idempotent,
)
from streamhouse.generator.produce import DefectRates, generate_payloads, produce_batch
from streamhouse.generator.topics import create_admin_client, create_kafka_topics

RECORDS = 5_000
BATCH = 1_000


class SimulatedCrash(RuntimeError):
    """A driver death, shaped like the real thing: after the write, before the commit."""


@pytest.fixture
def kafka_topic():
    """A throwaway topic, so offsets never leak between tests."""
    name = f"test.txn.{uuid4().hex[:8]}"
    admin = create_admin_client()
    create_kafka_topics(admin, [(name, 3, 1)])
    yield name
    admin.delete_topics([name])


def _start(spark, *, topic, bronze_path, checkpoint, batch_size=BATCH, crash_on=None):
    """The bronze query, optionally rigged to die after a given batch's write."""

    def process(batch, batch_id):
        good, _ = split_parseable(to_bronze(batch, ingested_at=datetime.now(UTC)))
        write_batch_idempotent(
            good,
            bronze_path,
            app_id="bronze",
            version=batch_id,
            partition_by=(PARTITION_COLUMN,),
        )
        if crash_on is not None and batch_id == crash_on:
            # The data is committed to Delta at this point. Spark has written
            # offsets/N but has not yet written commits/N. That gap is the entire
            # failure mode, and a graceful .stop() cannot reproduce it.
            raise SimulatedCrash(f"driver died after batch {batch_id} wrote")

    raw = read_kafka_stream(
        spark, topic=topic, starting_offsets="earliest", max_offsets_per_trigger=batch_size
    )
    return raw.writeStream.foreachBatch(process).option("checkpointLocation", checkpoint).start()


def _count(spark, path):
    return spark.read.format("delta").load(path).count()


@pytest.mark.spark
def test_replaying_the_same_batch_does_not_duplicate(spark, warehouse):
    """The core mechanism, tested directly and without any streaming machinery.

    If this fails, nothing else in this file can pass.
    """
    path = str(warehouse / "bronze")
    df = spark.range(500).toDF("id")

    write_batch_idempotent(df, path, app_id="bronze", version=1)
    write_batch_idempotent(df, path, app_id="bronze", version=1)  # replay

    assert _count(spark, path) == 500


@pytest.mark.spark
def test_a_later_version_still_applies(spark, warehouse):
    """Idempotence must not become a write block: batch N+1 has to land."""
    path = str(warehouse / "bronze")
    df = spark.range(100).toDF("id")

    write_batch_idempotent(df, path, app_id="bronze", version=0)
    write_batch_idempotent(df, path, app_id="bronze", version=1)

    assert _count(spark, path) == 200


@pytest.mark.spark
@pytest.mark.kafka
def test_kill_mid_batch_does_not_duplicate(spark, warehouse, checkpoints, kafka_topic):
    """The headline claim, end to end.

    Produce 5,000 records, kill the stream after the first batch has written but
    before Spark commits it, restart against the same checkpoint, drain, and
    assert the count is exactly 5,000 with no duplicate transaction_id.
    """
    produce_batch(generate_payloads(RECORDS, seed=99, defects=DefectRates()), topic=kafka_topic)

    bronze_path = str(warehouse / "bronze")
    checkpoint = str(checkpoints / "bronze")

    with pytest.raises(StreamingQueryException):
        _start(
            spark, topic=kafka_topic, bronze_path=bronze_path, checkpoint=checkpoint, crash_on=0
        ).awaitTermination(timeout=300)

    # The crash-shaped state on disk. This is the detail worth quoting in an
    # interview: offsets/0 says "batch 0 will read this range", commits/0 is
    # absent, so on restart Spark has no choice but to re-run batch 0.
    assert (Path(checkpoint) / "offsets" / "0").exists(), "no offset log; the batch never started"
    assert not (Path(checkpoint) / "commits" / "0").exists(), "batch committed; not a crash"
    # A partial write, not an exact count: Spark spreads maxOffsetsPerTrigger
    # across the topic's partitions, so the precise split is its business. What
    # matters is that batch 0's rows are already committed while its offsets are
    # uncommitted, which is the state the restart has to survive.
    after_crash = _count(spark, bronze_path)
    assert 0 < after_crash < RECORDS, f"expected a partial write, got {after_crash}"

    query = _start(
        spark, topic=kafka_topic, bronze_path=bronze_path, checkpoint=checkpoint, crash_on=None
    )
    query.processAllAvailable()
    query.stop()

    written = spark.read.format("delta").load(bronze_path)
    assert written.count() == RECORDS, "replayed batch 0 was written twice"
    assert written.select("transaction_id").distinct().count() == RECORDS


@pytest.mark.spark
@pytest.mark.kafka
def test_deleting_the_checkpoint_can_silently_lose_data(spark, warehouse, checkpoints, kafka_topic):
    """The negative case, and the one that teaches the most.

    The original spec expected duplication here. That is not what happens, and
    the truth is worse. Delta skips any version at or below the highest already
    committed for an app id, so after clearing the checkpoint the replayed
    batches are all skipped. If the new run draws its batch boundaries in a
    different place, the offsets that fall inside a skipped batch are never
    written at all, and nothing anywhere reports an error.

    This is the concrete cost of "just clear the checkpoint to unstick it".
    """
    produce_batch(generate_payloads(RECORDS, seed=101, defects=DefectRates()), topic=kafka_topic)

    bronze_path = str(warehouse / "bronze")
    checkpoint = str(checkpoints / "bronze")

    with pytest.raises(StreamingQueryException):
        _start(
            spark,
            topic=kafka_topic,
            bronze_path=bronze_path,
            checkpoint=checkpoint,
            batch_size=BATCH,
            crash_on=0,
        ).awaitTermination(timeout=300)
    partial = _count(spark, bronze_path)
    assert 0 < partial < RECORDS, f"expected a partial write, got {partial}"

    # Someone clears the checkpoint to get the stream moving again.
    shutil.rmtree(checkpoint)

    # It restarts from earliest, but with a different batch size, so batch 0 now
    # covers offsets the old batch 0 never did.
    query = _start(
        spark,
        topic=kafka_topic,
        bronze_path=bronze_path,
        checkpoint=checkpoint,
        batch_size=BATCH * 3,
        crash_on=None,
    )
    query.processAllAvailable()
    query.stop()

    written = spark.read.format("delta").load(bronze_path)
    final = written.count()

    assert final < RECORDS, (
        "expected silent loss from skipped versions; got everything, so the "
        "idempotence scope must have changed"
    )
    # And it is loss, not duplication. Both matter: the guarantee held, the
    # operator's mental model did not.
    assert written.select("transaction_id").distinct().count() == final


@pytest.mark.spark
def test_a_fresh_app_id_does_duplicate(spark, warehouse):
    """Where duplication actually comes from.

    The guarantee is scoped to (txnAppId, txnVersion). Change the app id and the
    idempotence marker no longer matches, so the same batch lands twice. This is
    what rewriting a job with a new name, or templating the app id off something
    volatile, silently does.
    """
    path = str(warehouse / "bronze")
    df = spark.range(300).toDF("id")

    write_batch_idempotent(df, path, app_id="bronze", version=0)
    write_batch_idempotent(df, path, app_id="bronze-restarted", version=0)

    assert _count(spark, path) == 600


@pytest.mark.spark
def test_non_deterministic_transform_breaks_the_guarantee(spark, warehouse):
    """Optional, and a genuinely deep cut.

    Delta's idempotence is by version, not by content, so a clock call mid-pipeline
    is survivable while the version marker holds. The moment it does not (see the
    fresh-app-id case above), content is all you have left, and a
    current_timestamp() column means the two copies are not even byte-identical.
    You cannot dedupe your way out afterwards.
    """
    path = str(warehouse / "bronze")
    stamped = spark.range(100).toDF("id").withColumn("processed_at", F.current_timestamp())

    write_batch_idempotent(stamped, path, app_id="a", version=0)
    write_batch_idempotent(stamped, path, app_id="b", version=0)

    rows = spark.read.format("delta").load(path)
    assert rows.count() == 200
    # Same logical row, two different stamps: no content-based dedup can collapse
    # these back without knowing which column to ignore.
    per_id = rows.groupBy("id").agg(F.countDistinct("processed_at").alias("stamps"))
    assert per_id.where("stamps > 1").count() > 0, (
        "the clock was evaluated once and cached; re-run with a materialisation "
        "boundary between the two writes"
    )
