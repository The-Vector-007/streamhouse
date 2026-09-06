"""P1.1 — the transaction model.

You write: src/streamhouse/domain/transaction.py

These tests pin the interface, not the implementation. If you want a different
shape, change the test and record why in docs/DECISIONS.md.
"""

from datetime import UTC, datetime

import pytest

from streamhouse.domain.transaction import Transaction, TransactionStatus


def _valid(**overrides):
    base = dict(
        transaction_id="txn_01HQ8X",
        account_id="acc_1001",
        counterparty_id="acc_2002",
        amount_minor=125_00,
        currency="INR",
        status=TransactionStatus.SETTLED,
        event_time=datetime(2026, 9, 1, 10, 30, tzinfo=UTC),
    )
    base.update(overrides)
    return base


def test_constructs_from_valid_fields():
    txn = Transaction(**_valid())
    assert txn.transaction_id == "txn_01HQ8X"
    assert txn.amount_minor == 12500


def test_amount_is_integer_minor_units_not_float():
    """Money is never a float. See docs/BRIEFS/p1-domain-and-topics.md."""
    txn = Transaction(**_valid())
    assert isinstance(txn.amount_minor, int)
    assert not hasattr(txn, "amount")  # no float amount field, deliberately


def test_rejects_negative_amount():
    with pytest.raises(ValueError):
        Transaction(**_valid(amount_minor=-1))


def test_rejects_unknown_currency():
    with pytest.raises(ValueError):
        Transaction(**_valid(currency="XYZ"))


def test_event_time_must_be_timezone_aware():
    """A naive datetime is a bug waiting for a DST boundary."""
    with pytest.raises(ValueError):
        Transaction(**_valid(event_time=datetime(2026, 9, 1, 10, 30)))


def test_roundtrips_through_kafka_bytes():
    txn = Transaction(**_valid())
    assert Transaction.from_bytes(txn.to_bytes()) == txn


def test_kafka_key_groups_by_account_not_transaction():
    """The key decides partition, and therefore ordering.

    Per-account ordering is what downstream dedup and balance logic need.
    Keying by transaction_id would scatter one account across every partition.
    """
    a = Transaction(**_valid(transaction_id="txn_A", account_id="acc_1"))
    b = Transaction(**_valid(transaction_id="txn_B", account_id="acc_1"))
    c = Transaction(**_valid(transaction_id="txn_C", account_id="acc_9"))

    assert a.kafka_key() == b.kafka_key()
    assert a.kafka_key() != c.kafka_key()


def test_ingest_time_defaults_to_now_and_is_separate_from_event_time():
    """event_time is when it happened. ingest_time is when we heard about it.

    Conflating them makes late data invisible, which breaks P4's watermark.
    """
    txn = Transaction(**_valid())
    assert txn.ingest_time is not None
    assert txn.ingest_time != txn.event_time
