"""P1.3 — the generator.

>>> DELETE EVERYTHING BELOW AND WRITE THE REAL THING. <<<

Interface only, so the suite collects. See TASKS.md P1.3.
"""

from dataclasses import dataclass

_TODO = "P1.3 not implemented — see TASKS.md"


@dataclass(frozen=True)
class DefectRates:
    """Fraction of records carrying each deliberate defect. Zero means clean."""

    duplicate: float = 0.0
    late: float = 0.0
    null_field: float = 0.0
    negative_amount: float = 0.0
    unknown_currency: float = 0.0


def generate_batch(count: int, seed: int, defects: DefectRates) -> list:
    raise NotImplementedError(_TODO)
