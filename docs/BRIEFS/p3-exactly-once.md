# P3 brief — exactly-once, and why it is a lie told carefully

## 1. The claim you are actually making

"Exactly-once delivery" is impossible in a distributed system. The message may be
delivered twice; you cannot prevent that. What you *can* do is make the **effect**
happen once.

The honest phrasing, and the one to use in an interview:

> **At-least-once delivery + idempotent writes = effectively-once processing.**

If you say "exactly-once" without that unpacking, a good interviewer will push, and
the push is the whole question.

## 2. The three moving parts

**Kafka offsets.** Where the reader is. Spark Structured Streaming does *not* use
Kafka consumer groups to track this — it stores offsets in its own checkpoint. So
"my consumer group lag is zero" means nothing here, which surprises people.

**Spark checkpoint.** A directory holding, per micro-batch: the offset range read,
and whether the batch committed. Structure:

```
checkpoints/bronze/
  offsets/     <- batch N will read this range   (written BEFORE the batch)
  commits/     <- batch N finished               (written AFTER the write)
```

The order is the entire mechanism. Offsets are written first, so a crash between
the two is *detectable*: an offset file with no matching commit file means "batch N
was attempted, outcome unknown."

**Delta transaction log.** `_delta_log/` holds an ordered set of JSON commits.
Version N either exists or does not — there is no partial version. That atomicity
is what makes the replay safe.

## 3. Walk the failure

Driver is killed after the Delta write but before the commit file:

1. Restart. Spark reads `offsets/N`, sees no `commits/N`, so it re-runs batch N.
2. It re-reads **the same offset range** — the offsets were recorded before the
   attempt, so the replay is byte-identical, not "wherever the consumer got to".
3. It writes to Delta again, using the same `txnAppId` and `txnVersion`.
4. Delta sees version N already applied for that app id and **skips the write**.
5. `commits/N` is written. Batch N+1 proceeds.

Net effect: the data landed once. The write was attempted twice. Both statements
are true simultaneously, and holding both is the actual understanding.

Read:
- Delta Lake docs, *Idempotent table writes* (`txnAppId` / `txnVersion`)
- Spark docs, *Structured Streaming — Fault Tolerance Semantics*
- Databricks, *Diving into Delta Lake: unpacking the transaction log*

## 4. What breaks it

- **Deleting the checkpoint.** Now offsets are unknown and you reprocess from
  `startingOffsets`. This is the number one cause of duplicated data in real
  pipelines, usually via "let me just clear the checkpoint to fix it."
- **Changing the query shape** while keeping the checkpoint. Spark may not be able
  to resume; some changes are legal, most are not.
- **`foreachBatch` without `txnAppId`/`txnVersion`.** Now the replay in step 4 writes
  a second copy. This is exactly the bug your failure-injection test exists to catch.
- **A non-deterministic transformation.** Replay must produce identical output. A
  `current_timestamp()` mid-pipeline quietly breaks the guarantee.

## 5. Why the test matters more than the code

Anyone can add `.option("checkpointLocation", ...)` and write "exactly-once" in a
README. Almost nobody kills the driver mid-batch and asserts the row count.

`test_kill_mid_batch_does_not_duplicate` is the single most valuable artifact in
this repository. It converts a claim into evidence, and in an interview it turns
"I believe it works" into "here's the test."

## Answer these before you write code

1. A micro-batch writes 10k rows to Delta, then the driver is killed before the
   offset commit. What happens on restart, and why once rather than twice?
2. Why does Spark keep its own offsets instead of using Kafka consumer groups?
3. Someone deletes `checkpoints/bronze/` to "fix a stuck stream". What happens?
4. Where does `foreachBatch` break the guarantee, and what restores it?
