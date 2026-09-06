"""P1.1 — the transaction model.

>>> DELETE EVERYTHING BELOW AND WRITE THE REAL THING. <<<

These stubs exist only so `make test` collects and reports honest failures
instead of an import error. They are the interface the tests expect, not a
starting point to fill in. See TASKS.md P1.1 and docs/BRIEFS/p1-domain-and-topics.md.
"""

from enum import StrEnum

_TODO = "P1.1 not implemented — see TASKS.md"


class TransactionStatus(StrEnum):
    """Replace with the statuses a payment actually moves through."""

    SETTLED = "SETTLED"


class Transaction:
    def __init__(self, **kwargs):
        raise NotImplementedError(_TODO)

    def to_bytes(self) -> bytes:
        raise NotImplementedError(_TODO)

    @classmethod
    def from_bytes(cls, raw: bytes) -> "Transaction":
        raise NotImplementedError(_TODO)

    def kafka_key(self) -> bytes:
        raise NotImplementedError(_TODO)
