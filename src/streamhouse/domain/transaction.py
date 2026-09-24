import json
from datetime import UTC, datetime
from enum import StrEnum

VALID_CURRENCIES = frozenset({"INR", "JPY", "USD"})


class TransactionStatus(StrEnum):
    """Lifecycle statuses for payment processing."""

    PENDING = "PENDING"
    SETTLED = "SETTLED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    REFUNDED = "REFUNDED"


class Transaction:
    def __init__(
        self,
        transaction_id: str,
        account_id: str,
        counterparty_id: str,
        amount_minor: int,
        currency: str,
        status: TransactionStatus,
        event_time: datetime,
        ingest_time: datetime | None = None,
    ) -> None:
        self._validate(amount_minor, currency, event_time)

        self.transaction_id = transaction_id
        self.account_id = account_id
        self.counterparty_id = counterparty_id
        self.amount_minor = amount_minor
        self.currency = currency
        self.status = TransactionStatus(status) if isinstance(status, str) else status
        self.event_time = event_time
        self.ingest_time: datetime = ingest_time or datetime.now(UTC)

    @staticmethod
    def _validate(amount_minor: int, currency: str, event_time: datetime) -> None:
        if amount_minor <= 0:
            raise ValueError(f"Transaction amount must be positive, got: {amount_minor}")
        if currency not in VALID_CURRENCIES:
            raise ValueError(
                f"Invalid currency: '{currency}'. Must be one of {set(VALID_CURRENCIES)}"
            )

        # Strictly enforce timezone awareness
        if event_time.tzinfo is None or event_time.tzinfo.utcoffset(event_time) is None:
            raise ValueError("event_time must be timezone-aware")

        if event_time > datetime.now(UTC):
            raise ValueError(f"Event time cannot be in the future: {event_time}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Transaction):
            return NotImplemented
        return self.transaction_id == other.transaction_id

    def __hash__(self) -> int:
        return hash(self.transaction_id)

    def to_bytes(self) -> bytes:
        message = {
            "transaction_id": self.transaction_id,
            "account_id": self.account_id,
            "counterparty_id": self.counterparty_id,
            "amount_minor": self.amount_minor,
            "currency": self.currency,
            "status": str(self.status),
            "event_time": self.event_time.isoformat(),
            "ingest_time": self.ingest_time.isoformat(),
        }
        return json.dumps(message, sort_keys=True).encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> "Transaction":
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ValueError(f"Failed to decode transaction bytes: {e}") from e

        try:
            event_time = datetime.fromisoformat(data["event_time"])
            ingest_time = (
                datetime.fromisoformat(data["ingest_time"]) if "ingest_time" in data else None
            )
            status = TransactionStatus(data["status"])
        except (KeyError, ValueError) as e:
            raise ValueError(f"Invalid or missing payload field: {e}") from e

        return cls(
            transaction_id=data["transaction_id"],
            account_id=data["account_id"],
            counterparty_id=data["counterparty_id"],
            amount_minor=data["amount_minor"],
            currency=data["currency"],
            status=status,
            event_time=event_time,
            ingest_time=ingest_time,
        )

    def kafka_key(self) -> bytes:
        if not self.account_id:
            raise ValueError("Cannot generate Kafka key: account_id is empty or None")
        return str(self.account_id).encode("utf-8")
