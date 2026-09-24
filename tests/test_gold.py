"""P5 — gold: aggregate marts that can be rebuilt.

The claim this layer has to earn: running it twice produces the same table. Most
"incremental" pipelines append a delta per run and drift the moment late data
arrives for a day that is already closed.
"""

from datetime import timedelta

import pytest

from _builders import INGESTED_AT, bronze_frame, bronze_row
from streamhouse.gold.marts import affected_dates, daily_account_totals, rebuild
from streamhouse.silver.refine import to_silver

DAY = INGESTED_AT.date()
YESTERDAY = DAY - timedelta(days=1)


def _silver(spark, rows):
    silver, _ = to_silver(bronze_frame(spark, rows), lateness_seconds=300)
    return silver


@pytest.mark.spark
def test_totals_group_by_account_currency_and_event_date(spark):
    rows = [
        bronze_row(0, account_id="acc_A", currency="INR", amount_minor=100),
        bronze_row(1, account_id="acc_A", currency="INR", amount_minor=250),
        bronze_row(2, account_id="acc_A", currency="USD", amount_minor=700),
        bronze_row(3, account_id="acc_B", currency="INR", amount_minor=50),
    ]
    mart = {
        (r.account_id, r.currency): r for r in daily_account_totals(_silver(spark, rows)).collect()
    }

    assert mart[("acc_A", "INR")].transaction_count == 2
    assert mart[("acc_A", "INR")].total_amount_minor == 350
    assert mart[("acc_A", "USD")].total_amount_minor == 700
    assert mart[("acc_B", "INR")].transaction_count == 1
    # Currencies are never summed together. 100 INR + 700 USD is not 800 anything.
    assert len(mart) == 3


@pytest.mark.spark
def test_grouping_uses_event_date_not_ingest_date(spark):
    """A mart answers business questions: Tuesday means Tuesday's events."""
    rows = [
        bronze_row(0),
        # Ingested today, happened yesterday.
        bronze_row(1, event_time=INGESTED_AT - timedelta(days=1)),
    ]
    dates = {r.event_date for r in daily_account_totals(_silver(spark, rows)).collect()}
    assert dates == {DAY, YESTERDAY}


@pytest.mark.spark
def test_rebuild_is_idempotent(spark, warehouse):
    """The whole point of the layer. Run it five times, get the same table."""
    rows = [bronze_row(i, amount_minor=100) for i in range(10)]
    silver = _silver(spark, rows)
    path = str(warehouse / "gold")

    rebuild(silver, path)
    first = spark.read.format("delta").load(path).collect()

    rebuild(silver, path)
    rebuild(silver, path)
    third = spark.read.format("delta").load(path).collect()

    assert len(first) == len(third) == 1
    assert first[0].transaction_count == third[0].transaction_count == 10
    assert first[0].total_amount_minor == third[0].total_amount_minor == 1000


@pytest.mark.spark
def test_late_data_restates_only_its_own_date(spark, warehouse):
    """Where the restatement cost of late data lands, and what it must not touch."""
    path = str(warehouse / "gold")

    today = [bronze_row(i, amount_minor=100) for i in range(3)]
    yesterday = [
        bronze_row(i + 10, amount_minor=100, event_time=INGESTED_AT - timedelta(days=1))
        for i in range(2)
    ]
    rebuild(_silver(spark, today + yesterday), path)

    before = {
        r.event_date: r.transaction_count for r in spark.read.format("delta").load(path).collect()
    }
    assert before == {DAY: 3, YESTERDAY: 2}

    # A late event for yesterday shows up in a batch ingested today.
    catch_up = yesterday + [
        bronze_row(99, amount_minor=100, event_time=INGESTED_AT - timedelta(days=1))
    ]
    late_silver = _silver(spark, catch_up)
    touched = rebuild(late_silver, path)

    assert touched == [YESTERDAY], "rebuilt a date the batch never touched"
    after = {
        r.event_date: r.transaction_count for r in spark.read.format("delta").load(path).collect()
    }
    assert after[YESTERDAY] == 3, "yesterday was not restated"
    assert after[DAY] == 3, "today was clobbered by a rebuild scoped to yesterday"


@pytest.mark.spark
def test_affected_dates_reports_what_the_batch_touched(spark):
    rows = [
        bronze_row(0),
        bronze_row(1, event_time=INGESTED_AT - timedelta(days=1)),
        bronze_row(2, event_time=INGESTED_AT - timedelta(days=1)),
    ]
    assert affected_dates(_silver(spark, rows)) == [YESTERDAY, DAY]


@pytest.mark.spark
def test_late_count_is_carried_into_the_mart(spark, warehouse):
    """So a consumer can tell a restated day from a quiet one without joining back."""
    rows = [
        bronze_row(0),
        bronze_row(1, event_time=INGESTED_AT - timedelta(hours=3)),
    ]
    mart = daily_account_totals(_silver(spark, rows)).collect()
    assert sum(r.late_count for r in mart) == 1
