"""P1.3 - the transaction generator.

Emits deliberately dirty data. Every layer downstream (the bronze DLQ, silver
dedup, the silver watermark, the quarantine table) exists to handle a defect that
starts here, so a generator that is too clean makes the rest of the project
theatre.

Two output shapes, and the split is forced by the domain model:

    generate_batch()    -> list[Transaction]   clean, duplicated and late records
    generate_payloads() -> list[bytes]         the same, plus records so invalid
                                               that Transaction refuses them

`Transaction._validate` rejects zero and negative amounts, unknown currencies and
future event times. That validation is right and worth keeping, so the defects it
refuses never travel through the model: they are built directly as wire-format
JSON. See docs/DECISIONS.md.

Which layer catches what (docs/BRIEFS/p1-domain-and-topics.md):

    duplicate id        client retry after timeout   -> silver dedup
    late event_time     mobile buffering, bad network -> silver watermark
    null/missing field  upstream schema drift        -> bronze schema enforcement
    negative amount     refund modelled wrong        -> silver validation, quarantine
    unknown currency    new market, nobody told you  -> silver validation, quarantine

Note the split: bronze asks "is this parseable?", silver asks "is this true?".
"""

import argparse
import json
import os
import random
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from streamhouse.domain.transaction import (
    VALID_CURRENCIES,
    Transaction,
    TransactionStatus,
)
from streamhouse.generator.topics import BOOTSTRAP_SERVERS

RAW_TOPIC = os.getenv("STREAMHOUSE_RAW_TOPIC", "txn.raw")

# A pool small enough that most accounts get several transactions. Partitioning is
# by account, so a pool of unique accounts would give every partition one row each
# and hide every ordering bug this project is meant to expose.
ACCOUNT_POOL_SIZE = 200

# On-time events land within this many seconds of now.
FRESH_WINDOW_S = 30.0

# Late events land between 5 and 60 minutes back. The floor matters: lateness is
# measured against the newest event in the batch, not against now, so it has to
# clear FRESH_WINDOW_S by a wide margin before a watermark test means anything.
LATE_MIN_S = 300.0
LATE_MAX_S = 3600.0

# Minor-unit exponent per currency. Not always 2: JPY has no minor unit at all, so
# a "cents" assumption would inflate every yen amount by 100x.
CURRENCY_EXPONENT = {"INR": 2, "USD": 2, "JPY": 0}

_CURRENCIES = sorted(VALID_CURRENCIES)
_STATUSES = list(TransactionStatus)


@dataclass(frozen=True)
class DefectRates:
    """Fraction of records carrying each deliberate defect. Zero means clean."""

    duplicate: float = 0.0
    late: float = 0.0
    null_field: float = 0.0
    negative_amount: float = 0.0
    unknown_currency: float = 0.0


def _defect_count(count: int, rate: float) -> int:
    """How many records carry a defect at this rate.

    Rounds, but never rounds a requested defect away entirely: asking for a 0.1%
    duplicate rate on 100 records should still give you one duplicate, otherwise a
    caller who asked for dirty data silently gets clean data.
    """
    if rate <= 0.0 or count < 2:
        return 0
    return max(1, min(count - 1, round(count * rate)))


def _account_id(rng: random.Random) -> str:
    return f"acc_{rng.randrange(ACCOUNT_POOL_SIZE):04d}"


def _amount_minor(rng: random.Random, currency: str) -> int:
    """A plausible amount, in this currency's minor units.

    The exponent is doing real work here. A flat "multiply by 100" would make
    every JPY amount a hundred times too large, which is the classic bug this
    project's integer-minor-units decision exists to avoid.
    """
    exponent = CURRENCY_EXPONENT[currency]
    major = rng.randint(1, 5_000)
    sub_unit = rng.randrange(10**exponent) if exponent else 0
    return major * 10**exponent + sub_unit


def generate_batch(count: int, seed: int, defects: DefectRates) -> list:
    """`count` transactions, reproducible from `seed` alone.

    Determinism comes from a local Random instance, never the module-level RNG:
    two calls with the same seed must agree even if something else in the process
    drew random numbers in between.

    Only the defects that a Transaction can legally represent are applied here,
    which is duplicates and lateness. The rest live in generate_payloads().
    """
    rng = random.Random(seed)

    # One clock reading for the whole batch, so lateness is measured against a
    # fixed point rather than drifting as the loop runs.
    now = datetime.now(UTC)

    batch: list[Transaction] = []
    for _ in range(count):
        currency = rng.choice(_CURRENCIES)
        batch.append(
            Transaction(
                transaction_id=f"txn_{rng.getrandbits(48):012x}",
                account_id=_account_id(rng),
                counterparty_id=_account_id(rng),
                amount_minor=_amount_minor(rng, currency),
                currency=currency,
                status=rng.choice(_STATUSES),
                # Always in the past. The model rejects future event times, so
                # clock-skew-forward is deliberately not modelled here.
                event_time=now - timedelta(seconds=rng.uniform(0.0, FRESH_WINDOW_S)),
            )
        )

    _apply_lateness(batch, rng, now, _defect_count(count, defects.late))
    _apply_duplicates(batch, rng, _defect_count(count, defects.duplicate))
    return batch


def _apply_lateness(
    batch: list[Transaction],
    rng: random.Random,
    now: datetime,
    n: int,
) -> None:
    """Push `n` records further into the past, in place.

    Backwards only. A client whose clock runs fast is a real defect, but the model
    refuses a future event_time, so it cannot be represented. See docs/DECISIONS.md.
    """
    if n <= 0:
        return

    for i in rng.sample(range(len(batch)), n):
        # Mutating in place skips _validate, which is fine only because we move
        # event_time backwards. Forwards would produce a record the model would
        # have refused to construct.
        batch[i].event_time = now - timedelta(seconds=rng.uniform(LATE_MIN_S, LATE_MAX_S))


def _apply_duplicates(batch: list[Transaction], rng: random.Random, n: int) -> None:
    """Replace `n` records with retries of other records, in place.

    Replace rather than append: a retry storm does not change how many messages the
    broker saw, and callers asked for `count` records. Each duplicate is a fresh
    object carrying an earlier record's id and fields, which is what a client retry
    after a timeout actually looks like on the wire.
    """
    if n <= 0:
        return

    victims = rng.sample(range(len(batch)), n)
    # Sources must not themselves be replaced, or a duplicate can point at a record
    # that no longer exists in the batch.
    sources = [i for i in range(len(batch)) if i not in set(victims)]

    for victim in victims:
        original = batch[rng.choice(sources)]
        batch[victim] = Transaction(
            transaction_id=original.transaction_id,
            account_id=original.account_id,
            counterparty_id=original.counterparty_id,
            amount_minor=original.amount_minor,
            currency=original.currency,
            status=original.status,
            event_time=original.event_time,
        )


def _corrupt(payload: dict, kind: str, rng: random.Random) -> dict:
    """Break one field of a decoded payload, in the way `kind` names.

    These are the defects Transaction refuses to construct, so they are applied to
    the decoded dict on its way to the wire rather than to a model instance.
    """
    broken = dict(payload)
    if kind == "null_field":
        # Upstream schema drift: a field that used to be there stops arriving.
        # Structurally broken, so bronze catches it and DLQs it.
        broken[rng.choice(["account_id", "amount_minor", "currency"])] = None
    elif kind == "negative_amount":
        # Parses fine, so bronze must pass it through untouched. Silver quarantines
        # it. Filtering here would destroy the audit trail bronze exists to keep.
        broken["amount_minor"] = -abs(broken["amount_minor"])
    elif kind == "unknown_currency":
        broken["currency"] = "XYZ"
    else:
        raise ValueError(f"unknown defect kind: {kind}")
    return broken


def generate_payloads(count: int, seed: int, defects: DefectRates) -> list[bytes]:
    """`count` wire-format JSON payloads, reproducible from `seed`.

    This is what actually goes to Kafka. It is generate_batch() plus the three
    defects that cannot exist as a Transaction, applied by replacement so the
    count still holds.
    """
    rng = random.Random(seed)
    payloads = [json.loads(t.to_bytes()) for t in generate_batch(count, seed, defects)]

    corruptions = [
        ("null_field", _defect_count(count, defects.null_field)),
        ("negative_amount", _defect_count(count, defects.negative_amount)),
        ("unknown_currency", _defect_count(count, defects.unknown_currency)),
    ]

    # Draw victims from one pool so two defects never land on the same record. A
    # record that is both negative and unknown-currency would make it ambiguous
    # which silver rule actually rejected it.
    available = list(range(count))
    rng.shuffle(available)
    for kind, n in corruptions:
        for _ in range(n):
            if not available:
                break
            victim = available.pop()
            payloads[victim] = _corrupt(payloads[victim], kind, rng)

    return [json.dumps(p, sort_keys=True).encode("utf-8") for p in payloads]


def produce_batch(
    records: Iterable[Transaction | bytes],
    *,
    topic: str = RAW_TOPIC,
    bootstrap_servers: str = BOOTSTRAP_SERVERS,
    flush_timeout: float = 30.0,
) -> int:
    """Publish records to Kafka. Returns the number confirmed delivered.

    Accepts Transactions or raw bytes, because the corrupt fraction has no valid
    model instance to key off.
    """
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    delivered = 0
    failures: list[str] = []

    def _on_delivery(err, _msg) -> None:
        nonlocal delivered
        if err is None:
            delivered += 1
        else:
            failures.append(str(err))

    for record in records:
        if isinstance(record, Transaction):
            key, value = record.kafka_key(), record.to_bytes()
        else:
            # A corrupt payload gets no key. Keying it by a field we already know
            # is untrustworthy would route garbage into a real account's partition
            # and break the per-account ordering the key exists to guarantee.
            key, value = None, record
        producer.produce(topic=topic, key=key, value=value, on_delivery=_on_delivery)
        producer.poll(0)

    remaining = producer.flush(flush_timeout)
    if remaining:
        raise RuntimeError(f"{remaining} messages still queued after {flush_timeout}s")
    if failures:
        raise RuntimeError(f"{len(failures)} deliveries failed, first: {failures[0]}")
    return delivered


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Produce synthetic transactions to Kafka.")
    parser.add_argument("--rate", type=int, default=100, help="messages per second")
    parser.add_argument("--seconds", type=int, default=5, help="how long to run")
    parser.add_argument("--seed", type=int, default=0, help="base seed, for reproducible runs")
    parser.add_argument("--topic", default=RAW_TOPIC)
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--duplicate", type=float, default=0.05)
    parser.add_argument("--late", type=float, default=0.02)
    parser.add_argument("--null-field", type=float, default=0.01)
    parser.add_argument("--negative-amount", type=float, default=0.01)
    parser.add_argument("--unknown-currency", type=float, default=0.01)
    args = parser.parse_args(argv)

    defects = DefectRates(
        duplicate=args.duplicate,
        late=args.late,
        null_field=args.null_field,
        negative_amount=args.negative_amount,
        unknown_currency=args.unknown_currency,
    )

    total = 0
    for tick in range(args.seconds):
        started = time.monotonic()
        # A new seed per tick, derived from the base, so a run is reproducible as a
        # whole while each second still carries different transactions.
        payloads = generate_payloads(args.rate, args.seed + tick, defects)
        total += produce_batch(payloads, topic=args.topic, bootstrap_servers=args.bootstrap_servers)
        time.sleep(max(0.0, 1.0 - (time.monotonic() - started)))

    print(f"delivered {total} messages to {args.topic}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
