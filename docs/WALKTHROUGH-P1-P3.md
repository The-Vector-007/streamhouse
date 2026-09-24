# Walkthrough: the generator, the session, and bronze

Read this alongside the code. It covers what landed, why it was built that way, and
the interview answer each piece supports.

---

## 1. The generator (`src/streamhouse/generator/produce.py`)

### What it does

Two entry points, and the split is the interesting part.

- `generate_batch(count, seed, defects) -> list[Transaction]` returns model objects.
- `generate_payloads(count, seed, defects) -> list[bytes]` returns wire JSON.

### Why two

`Transaction._validate` refuses zero and negative amounts, unknown currencies and
future event times. `DefectRates` asks for exactly three defects that validation
forbids. Something had to give, and there were three options:

1. Relax the model so it can hold broken data.
2. Bypass the constructor with `object.__new__`.
3. Build the corrupt fraction as raw JSON that never touches the model.

Option 3 won. A domain model whose job is to refuse invalid transactions should not
be weakened so a test fixture can be lazier, and the corrupt bytes are exactly what
bronze's dead-letter path needs to have something real to catch.

### The details worth knowing

**Determinism comes from a local `Random(seed)`**, never the module-level RNG. If it
used `random.shuffle` directly, an unrelated library drawing a number between your
two calls would break reproducibility, and you would spend an evening not believing
your own test.

**Duplicates replace, they do not append.** A retry storm does not change how many
messages the broker saw. If duplicates appended, `count=1000, duplicate=0.05` would
return 1050 records, and every downstream count assertion would quietly drift.

**Lateness only moves backwards.** A client whose clock runs fast is a real defect,
but the model rejects a future `event_time`, so it cannot be represented here. The
floor is 300 seconds because lateness is measured against the newest record in the
batch, not against now, and on-time records already spread across a 30-second
window. A 60-second floor would produce a test that passes or fails on timing luck.

**Currency exponent is not always 2.** JPY has no minor unit. `_amount_minor` uses
`10**exponent`, so yen amounts come out as whole yen. A flat "multiply by 100" would
make every JPY transaction a hundred times too large, which is the exact bug that
storing integer minor units is supposed to prevent.

### Interview answer this supports

> *"Your generator emits 5% duplicates and 2% late events. Why would a pipeline that
> passes on clean data fail here, and which layer catches each defect?"*

Clean data hides the fact that a pipeline makes assumptions. Duplicates break
row-uniqueness, and nothing upstream can see them because each copy is individually
valid, so they die in silver dedup. Late events break window completeness, bounded
by a watermark in silver, with the restatement cost landing in gold. The real
tension is completeness against latency: a longer watermark catches more late data
and delays every downstream number.

Bronze enforces structure, silver enforces meaning. Bronze asks "is this
parseable?", silver asks "is this true?"

---

## 2. The Spark session (`src/streamhouse/spark.py`)

### Why the configs are on the builder, not set afterwards

Spark freezes its `SparkConf` when the JVM starts. `spark.conf.set(...)` afterwards
changes the *session* config for things that read it at query time, but the Delta
extension is installed during session construction. Set it late and the extension
never loads, `sparkContext.getConf()` never shows it, and Delta writes fail with an
error that does not mention any of this.

### Why `local[2]` and 8 shuffle partitions

The driver is also the executor in local mode, and it shares an 8 GB WSL VM with a
Kafka container. More threads buys more concurrent task memory, not more speed.
Spark's default of 200 shuffle partitions is right on a cluster and absurd here: it
produces 200 near-empty files per shuffle and the scheduling overhead dwarfs the
work.

### The one that will bite you later

`spark.sql.session.timeZone` is pinned to UTC. Payloads carry tz-aware UTC
timestamps. If the session ran in the host's local zone, Spark would reinterpret
them, and every event-time window in silver would silently shift by the UTC offset.
Nothing errors. The numbers are just wrong.

### The Kafka connector

PySpark does not bundle it, and `configure_spark_with_delta_pip` only adds Delta.
Without it, `readStream.format("kafka")` fails with *"Failed to find data source:
kafka"*, which reads like a typo. It is pinned to `pyspark.__version__` rather than
hardcoded, because a connector built against a different Spark minor fails later,
deeper, and with a worse message.

---

## 3. Bronze (`src/streamhouse/bronze/ingest.py`)

### The structure/meaning split

This is the design decision to lead with if someone asks about the medallion layers.

A negative amount parses fine. An unknown currency parses fine. Both are *wrong*,
and both land in bronze untouched, because bronze's job is to be able to answer
"did we receive it?" even when the answer is "yes, and it was garbage". Filtering
them here would destroy the audit trail that is the entire reason bronze exists.

Only records that cannot be identified at all get dead-lettered: unparseable JSON,
or a null `transaction_id` / `account_id` / `event_time`. Note what is *not* in
`REQUIRED_FIELDS`: amount and currency. A wrong amount is still a row we received.

Nothing is dropped. `split_parseable` returns two frames and every input row leaves
in exactly one of them.

### Why ingest date, not event date

Partitioning by event date looks more natural and is a trap. Late data would reopen
old event-date partitions, turning every append into a rewrite of history, and the
whole point of a streaming append is that it does not rewrite. The test pushes
`event_time` back 40 days specifically so the two choices produce visibly different
partition values.

### Why the ingest stamp is taken once per batch

`current_timestamp()` per row would scatter a batch that straddles midnight across
two date partitions. It is also a non-deterministic transformation in the middle of
a pipeline, which is the thing that quietly breaks replay reasoning. The stamp is
taken on the driver, once, and applied as a literal.

---

## 4. Exactly-once (`write_batch_idempotent`, and the tests)

### The sentence to say

**At-least-once delivery plus idempotent writes equals effectively-once
processing.** Kafka will hand you the same records twice after a failure. Delta
refuses to apply the same `(txnAppId, txnVersion)` twice. Neither half is sufficient
alone, and saying "exactly-once" without naming both halves is the tell that someone
has only read the marketing.

### The mechanism, concretely

```
checkpoints/bronze/
  offsets/N     written BEFORE batch N runs   "batch N will read this range"
  commits/N     written AFTER batch N finishes "batch N is done"
```

Crash after the Delta write but before `commits/N`, and on restart Spark sees
`offsets/N` with no `commits/N`. It has no choice but to re-run batch N against the
identical offset range. The write carries the same `txnVersion`, Delta sees it is
not greater than what it already committed for that app id, and skips it.

### What the test actually does

`test_kill_mid_batch_does_not_duplicate` raises inside `foreachBatch` after
`write_batch_idempotent` returns. It then asserts *on disk* that `offsets/0` exists
and `commits/0` does not, which is the proof that the failure was crash-shaped
rather than graceful. Then it restarts against the same checkpoint, drains, and
asserts exactly 5,000 rows with no duplicate `transaction_id`.

Without the `txnAppId`/`txnVersion` options, the replay writes a second copy and the
count comes back over 5,000. That is the classic `foreachBatch` bug, and it looks
completely correct until the first driver death in production.

### The finding that contradicted the original spec

The spec assumed deleting the checkpoint duplicates data. **It does not.** Verified
directly:

```
after v0              : 100
replay same v0        : 100
new v1                : 200
REPLAY OLD v0 after v1: 200   <- skipped, not duplicated
different app_id v0   : 300   <- this is where duplication comes from
```

Delta tracks the *highest* version committed per app id and skips anything at or
below it. So after clearing a checkpoint, every replayed batch is skipped. If the
restart draws its batch boundaries somewhere else, the offsets that fall inside a
skipped batch are **never written at all**.

That is silent data loss, and it is worse than duplication because nothing errors
and no count looks obviously wrong. It is also the honest answer to "what happens if
someone clears the checkpoint to unstick a stream", which is a thing people do.

Duplication comes from changing the `txnAppId`, because that resets the idempotence
scope. Renaming a job does it. So does templating the app id off anything volatile.
