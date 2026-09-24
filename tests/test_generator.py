"""P1.3 — the generator.

Implemented in: src/streamhouse/generator/produce.py

The generator emits defects on purpose. Every downstream layer exists to handle
them, so a generator that is too clean makes the rest of the project theatre.
"""

from uuid import uuid4

import pytest

from streamhouse.generator.produce import (
    DefectRates,
    generate_batch,
    generate_payloads,
    produce_batch,
)
from streamhouse.generator.topics import (
    BOOTSTRAP_SERVERS,
    create_admin_client,
    create_kafka_topics,
)


def test_generates_requested_count():
    batch = generate_batch(count=100, seed=42, defects=DefectRates())
    assert len(batch) == 100


def test_is_deterministic_for_a_seed():
    """You will be debugging a failing assertion at 11pm. Reproducibility is mercy."""
    a = generate_batch(count=50, seed=7, defects=DefectRates())
    b = generate_batch(count=50, seed=7, defects=DefectRates())
    assert [t.transaction_id for t in a] == [t.transaction_id for t in b]


def test_emits_duplicate_transaction_ids_when_asked():
    batch = generate_batch(count=1000, seed=1, defects=DefectRates(duplicate=0.05))
    ids = [t.transaction_id for t in batch]
    assert len(ids) != len(set(ids)), "no duplicates emitted; silver dedup is untestable"


def test_emits_late_events_when_asked():
    """Late == event_time far behind the batch's newest event_time."""
    batch = generate_batch(count=1000, seed=2, defects=DefectRates(late=0.02))
    newest = max(t.event_time for t in batch)
    lateness = [(newest - t.event_time).total_seconds() for t in batch]
    assert max(lateness) > 60, "nothing arrived late; P4 watermarks are untestable"


def test_clean_run_emits_no_defects():
    """Defect rates of zero must mean zero. Otherwise you cannot isolate a bug."""
    batch = generate_batch(count=500, seed=3, defects=DefectRates())
    ids = [t.transaction_id for t in batch]
    assert len(ids) == len(set(ids))


@pytest.mark.kafka
def test_produces_to_kafka():
    """Needs the broker: make up PROFILE=ingest

    Publish a batch, consume it back, assert the count. Keying is asserted in
    test_domain.py, so here you only care that it lands.
    """
    from confluent_kafka import Consumer

    topic = f"test.produce.{uuid4().hex[:8]}"
    admin = create_admin_client()
    create_kafka_topics(admin, [(topic, 3, 1)])

    try:
        payloads = generate_payloads(250, seed=77, defects=DefectRates())
        delivered = produce_batch(payloads, topic=topic)
        assert delivered == 250, "producer reported fewer deliveries than it was given"

        consumer = Consumer(
            {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
                "group.id": f"test-{uuid4().hex[:8]}",
                # Read the topic from the start; this consumer has no committed
                # offsets and the default would skip everything already produced.
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([topic])
        try:
            seen = []
            # Poll until the batch is accounted for, with a deadline rather than a
            # fixed sleep: broker assignment latency is not something to guess at.
            while len(seen) < 250:
                message = consumer.poll(timeout=10.0)
                if message is None:
                    break
                if message.error():
                    continue
                seen.append(message.value())
        finally:
            consumer.close()

        assert len(seen) == 250
        # The bytes survive the round trip intact, which is what bronze relies on.
        assert set(seen) == set(payloads)
    finally:
        admin.delete_topics([topic])
