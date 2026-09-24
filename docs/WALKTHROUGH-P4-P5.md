# Walkthrough: silver, gold, and the pipeline runner

Stage 2. Bronze answered "did we receive it?". These two layers answer "is it
true?" and "what does it add up to?".

---

## Silver (`src/streamhouse/silver/refine.py`)

### The order matters: dedup, then validate

`to_silver` deduplicates before it validates, and that is not arbitrary. A client
retries an invalid record three times, and if validation ran first you would
quarantine three rows for one bad transaction. Your data-quality failure rate would
then depend on how flaky that client's network was, which makes the whole time
series meaningless.

### Why dedup orders by offset, not by timestamp

```python
Window.partitionBy("transaction_id").orderBy("kafka_partition", "kafka_offset")
```

Offsets are a total order within a partition and never tie. Two retries can easily
share a millisecond, and ordering by a timestamp would leave the winner to chance.
A non-deterministic dedup makes every downstream count irreproducible, which you
discover the first time two runs of the same backfill disagree.

Dedup is a transformation, not a quality check. There is no defect to report, just
a row to collapse. Worth saying out loud in an interview, because it is a common
place people conflate the two.

### Why the rules are SQL strings

An early version built `F.col(...)` predicates at module import. That crashes: a
Column cannot exist before a SparkContext does. Strings also mean the quarantine
reason and the DQ check share one definition and cannot drift apart.

The `IS NULL` arms are load-bearing. SQL three-valued logic makes
`currency NOT IN ('INR','JPY','USD')` evaluate to NULL, not true, when currency is
null, so a null currency would have slipped through as clean. That is the kind of
bug that survives code review and shows up as a rounding error six months later.

### Quarantine, never drop

A row you deleted is a row you cannot explain to whoever asks about it next week.
Quarantined rows carry *every* reason they failed, not just the first, so fixing
one upstream bug does not reveal a second one a week later.

### Lateness is a property, not a defect

Late events are flagged with `is_late` and `lateness_seconds`, and kept. Dropping
them is how a pipeline reports numbers that are clean, stable and quietly wrong.
The restatement cost lands in gold, which is the cheap place to pay it.

---

## Gold (`src/streamhouse/gold/marts.py`)

### The line to lead with

**Most "incremental" pipelines are secretly not incremental.** They append a delta
per run and assume nothing already written will change. Then a late event arrives
for last Tuesday, the append adds a row Tuesday's total never accounted for, and
the mart drifts from its source with nothing to signal it.

### Recompute and replace

`rebuild` works out which event dates a batch touched, recomputes those whole
partitions, and swaps them in with Delta's `replaceWhere`. Running it once or five
times leaves the same table. That is what makes a backfill safe and what lets
Airflow retry a task without anyone reasoning about it.

`replaceWhere` also makes the swap atomic per partition, so a reader never sees a
half-rebuilt day.

### Event date here, ingest date in bronze

Bronze partitions by ingest date because that is a write-mechanics decision: it
keeps appends append-only. Gold groups by event date because that is a meaning
decision: "how much did this account move on Tuesday" means Tuesday's events,
whenever they arrived. Same data, two different questions, two different keys.

`test_late_data_restates_only_its_own_date` is the one to point at: a late event
for yesterday restates yesterday and leaves today untouched.

---

## The runner (`src/streamhouse/pipeline.py`)

Two commands, because there are two shapes of work:

```
python -m streamhouse.pipeline ingest --seconds 30    # streaming, always-on
python -m streamhouse.pipeline refine                  # batch, retryable
```

Silver and quarantine are **overwritten** on every refine, because they are a pure
function of bronze. Rewriting them is the only way a rerun after a bug fix produces
the right answer instead of a doubled one.

The DQ table **appends**, because it is a time series. The point is to watch the
failure rate move.

### A real run, for reference

3,000 messages produced with defects injected, then ingest, then refine:

```
bronze_rows:      2987     (13 unparseable went to the DLQ)
silver_rows:      2709
quarantined_rows:  128
gold_dates:       ['2026-09-24']
```

Those reconcile: 2987 − 150 duplicates = 2837 = 2709 + 128. The 150 is the 5%
duplicate rate the generator was asked for. The DQ table showed 2.08% non-positive
amounts and 2.21% unknown currencies against 2% injected for each, which is the
sampling noise you would expect.

Being able to walk that arithmetic is worth more in an interview than any
architecture diagram.
