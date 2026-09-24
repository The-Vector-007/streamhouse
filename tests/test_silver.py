"""P4 — silver: dedup, validation, quarantine, data quality.

Bronze asked "is this parseable?". Silver asks "is this true?".
"""

from datetime import timedelta

import pytest

from _builders import INGESTED_AT, bronze_frame, bronze_row
from streamhouse.silver.refine import (
    QUARANTINE_RULES,
    breaches,
    deduplicate,
    dq_results,
    flag_lateness,
    to_silver,
    validate,
)


@pytest.mark.spark
def test_dedup_collapses_retries_keeping_the_first_arrival(spark):
    """A client retry puts the same id on the log twice. Both copies are valid."""
    rows = [
        bronze_row(0, transaction_id="txn_dup", amount_minor=100),
        bronze_row(1, transaction_id="txn_unique"),
        # The retry: same id, later offset, and a different amount so we can tell
        # which copy survived.
        bronze_row(2, transaction_id="txn_dup", amount_minor=999),
    ]
    deduped = deduplicate(bronze_frame(spark, rows))

    assert deduped.count() == 2
    survivor = deduped.where("transaction_id = 'txn_dup'").collect()[0]
    assert survivor.amount_minor == 100, "kept the retry instead of the original"


@pytest.mark.spark
def test_dedup_is_deterministic_across_runs(spark):
    """Ordering by offset, not timestamp: two retries can share a millisecond."""
    rows = [
        bronze_row(i, transaction_id="txn_same", amount_minor=100 + i, kafka_partition=0)
        for i in range(5)
    ]
    frame = bronze_frame(spark, rows)

    first = deduplicate(frame).collect()[0].amount_minor
    second = deduplicate(frame).collect()[0].amount_minor
    assert first == second == 100


@pytest.mark.spark
def test_invalid_rows_are_quarantined_not_dropped(spark):
    rows = [
        bronze_row(0),
        bronze_row(1, amount_minor=-500),
        bronze_row(2, currency="XYZ"),
        bronze_row(3, status="TELEPORTED"),
        bronze_row(4, counterparty_id=None),
    ]
    clean, quarantined = validate(bronze_frame(spark, rows))

    assert clean.count() == 1
    assert quarantined.count() == 4
    # Nothing vanishes.
    assert clean.count() + quarantined.count() == len(rows)

    reasons = {r.transaction_id: r.quarantine_reason for r in quarantined.collect()}
    assert reasons["txn_000001"] == "non_positive_amount"
    assert reasons["txn_000002"] == "unknown_currency"
    assert reasons["txn_000003"] == "unknown_status"
    assert reasons["txn_000004"] == "missing_counterparty"


@pytest.mark.spark
def test_a_row_reports_every_reason_it_failed(spark):
    """Fixing one upstream bug should not reveal a second one next week."""
    rows = [bronze_row(0, amount_minor=-1, currency="XYZ")]
    _, quarantined = validate(bronze_frame(spark, rows))

    reason = quarantined.collect()[0].quarantine_reason
    assert "non_positive_amount" in reason
    assert "unknown_currency" in reason


@pytest.mark.spark
def test_late_events_are_flagged_and_kept(spark):
    """Dropping late data is how a pipeline reports numbers that are quietly wrong."""
    rows = [
        bronze_row(0),
        bronze_row(1, event_time=INGESTED_AT - timedelta(hours=2)),
    ]
    flagged = flag_lateness(bronze_frame(spark, rows), lateness_seconds=300)

    assert flagged.count() == 2, "a late row was dropped"
    by_id = {r.transaction_id: r for r in flagged.collect()}
    assert by_id["txn_000000"].is_late is False
    assert by_id["txn_000001"].is_late is True
    assert by_id["txn_000001"].lateness_seconds == 7200


@pytest.mark.spark
def test_dedup_runs_before_validation(spark):
    """A retry of an invalid record is one quarantined row, not three.

    Otherwise the failure rate depends on how flaky the client's network was,
    which makes the DQ time series meaningless.
    """
    rows = [bronze_row(i, transaction_id="txn_bad", amount_minor=-5) for i in range(3)]
    _, quarantined = to_silver(bronze_frame(spark, rows))

    assert quarantined.count() == 1


@pytest.mark.spark
def test_dq_results_report_one_row_per_check(spark):
    rows = [bronze_row(0), bronze_row(1, amount_minor=-500), bronze_row(2, currency="XYZ")]
    source = bronze_frame(spark, rows)
    _, quarantined = to_silver(source)

    results = dq_results(source=source, quarantined=quarantined, run_id="run-1")
    collected = {r.check_name: r for r in results.collect()}

    assert set(collected) == {name for name, _ in QUARANTINE_RULES}
    assert collected["non_positive_amount"].rows_failed == 1
    assert collected["unknown_currency"].rows_failed == 1
    assert collected["unknown_status"].rows_failed == 0
    assert collected["unknown_status"].passed is True
    assert collected["non_positive_amount"].passed is False
    assert all(r.rows_checked == 3 for r in collected.values())
    assert collected["non_positive_amount"].failure_rate == pytest.approx(1 / 3)


@pytest.mark.spark
def test_dq_results_survive_an_empty_batch(spark):
    """An empty batch is not a 100% failure rate, and a NaN would poison averages."""
    empty = bronze_frame(spark, [])
    _, quarantined = to_silver(empty)

    results = dq_results(source=empty, quarantined=quarantined, run_id="run-empty")
    assert results.count() == len(QUARANTINE_RULES)
    assert all(r.failure_rate == 0.0 and r.passed for r in results.collect())


@pytest.mark.spark
def test_the_gate_opens_when_failure_rates_are_under_threshold(spark):
    """Routine bad data must not fail the run.

    The live warehouse sits around 2% on two checks. A gate that trips there would
    be red permanently, and a permanently red check is one nobody reads.
    """
    rows = [bronze_row(i) for i in range(100)] + [bronze_row(900, amount_minor=-1)]
    source = bronze_frame(spark, rows)
    _, quarantined = to_silver(source)

    results = dq_results(source=source, quarantined=quarantined, run_id="under")
    assert breaches(results) == []


@pytest.mark.spark
def test_the_gate_trips_when_a_rate_crosses_its_threshold(spark):
    """A step change in failure rate is the thing worth waking someone for."""
    clean = [bronze_row(i) for i in range(80)]
    broken = [bronze_row(500 + i, currency="XYZ") for i in range(20)]
    source = bronze_frame(spark, clean + broken)
    _, quarantined = to_silver(source)

    results = dq_results(source=source, quarantined=quarantined, run_id="over")
    failed = breaches(results)

    assert [f["check"] for f in failed] == ["unknown_currency"]
    assert failed[0]["failure_rate"] == pytest.approx(0.2)
    assert failed[0]["threshold"] == 0.05
    assert failed[0]["rows_failed"] == 20


@pytest.mark.spark
def test_thresholds_are_overridable_per_check(spark):
    """So one noisy rule can be loosened without blunting the rest."""
    rows = [bronze_row(i) for i in range(80)] + [
        bronze_row(500 + i, currency="XYZ") for i in range(20)
    ]
    source = bronze_frame(spark, rows)
    _, quarantined = to_silver(source)

    relaxed = dq_results(
        source=source,
        quarantined=quarantined,
        run_id="relaxed",
        thresholds={"unknown_currency": 0.5},
    )
    assert breaches(relaxed) == []


@pytest.mark.spark
def test_quarantine_is_unaffected_by_the_gate(spark):
    """The gate decides whether the run fails. It never decides what is kept."""
    rows = [bronze_row(i) for i in range(80)] + [
        bronze_row(500 + i, currency="XYZ") for i in range(20)
    ]
    source = bronze_frame(spark, rows)
    clean, quarantined = to_silver(source)

    assert quarantined.count() == 20
    assert clean.count() + quarantined.count() == 100, "the gate dropped rows"
