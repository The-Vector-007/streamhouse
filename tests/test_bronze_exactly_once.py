"""P3.3 — failure injection. The test that makes the README honest.

Anyone can set `checkpointLocation` and write "exactly-once" in a README. Almost
nobody kills the write mid-batch and asserts the row count. That difference is
this file, and it is the most interview-valuable artifact in the repo.

Read docs/BRIEFS/p3-exactly-once.md first. Specifically: at-least-once delivery
plus idempotent writes equals effectively-once processing. Say it that way.
"""

import pytest


@pytest.mark.spark
def test_replaying_the_same_batch_does_not_duplicate(spark, warehouse):
    """The core mechanism, tested directly and without any streaming machinery.

    Write the same batch twice with the same (txnAppId, txnVersion). Delta must
    apply it once and skip the second. If this fails, nothing else in this file
    can pass.

        from streamhouse.bronze.ingest import write_batch_idempotent

        write_batch_idempotent(df, path, app_id="bronze", version=1)
        write_batch_idempotent(df, path, app_id="bronze", version=1)  # replay
        assert spark.read.format("delta").load(path).count() == len(df)
    """
    pytest.skip("implement in P3.3")


@pytest.mark.spark
def test_kill_mid_batch_does_not_duplicate(spark, warehouse, checkpoints):
    """The headline claim, end to end.

    Shape:
      1. produce a known number of records (say 5,000) to a source
      2. start the stream writing to bronze
      3. kill it mid-batch, without a graceful stop, leaving offsets/N written
         and commits/N absent
      4. restart against the SAME checkpoint
      5. let it drain
      6. assert the bronze row count == 5,000 exactly, and that
         transaction_id has no duplicates

    Step 3 is the whole test. A graceful `.stop()` proves nothing, because it
    commits cleanly. You need the crash-shaped failure.

    Recording the observed offsets/commits state at step 3 in docs/ANSWERS.md is
    worth doing: it is the concrete detail that makes the interview answer land.
    """
    pytest.skip("implement in P3.3")


@pytest.mark.spark
def test_deleting_the_checkpoint_does_duplicate(spark, warehouse, checkpoints):
    """The negative case, and the one that teaches the most.

    Delete the checkpoint, restart from earliest, and assert the data IS
    duplicated. Two reasons this earns its place:

      1. it proves the guarantee comes from the checkpoint, not from luck or
         from something Delta does for free
      2. it documents the single most common real-world cause of duplicated
         data: someone "just clearing the checkpoint" to unstick a stream

    A test that asserts the failure mode is worth more than a comment warning
    about it.
    """
    pytest.skip("implement in P3.3")


@pytest.mark.spark
def test_non_deterministic_transform_breaks_the_guarantee(spark, warehouse, checkpoints):
    """Optional, and a genuinely deep cut.

    Put current_timestamp() mid-pipeline and show that a replayed batch produces
    different rows, so the idempotence check no longer protects you. If you get
    this one working you will be ahead of most senior candidates on this topic.
    """
    pytest.skip("optional, P3.3")
