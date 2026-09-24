# Open questions

Places where this code takes a position an interviewer could push on. Each one has
a defensible answer; none of them is the only possible answer. Knowing which is
which is the point.

---

### Why is equality on `Transaction` only `transaction_id`?

Two records with the same id and different amounts compare equal. That is arguably
right for a payments domain (the id *is* the identity) and arguably a bug waiting to
happen (a corrupted retry would compare equal to the original and hide the
corruption). Currently unresolved. If asked, the honest answer is that identity is
the id and field-level comparison belongs in a reconciliation check, not in `__eq__`.

### Why is `Transaction` mutable?

`txn.amount_minor = -500` is accepted after construction, bypassing `_validate`.
The generator relies on this to push `event_time` backwards. A frozen dataclass
would be safer and would force the generator to rebuild rather than mutate. The
trade was made for the generator's convenience, which is the weaker reason.

### Should `amount_minor == 0` be valid?

The model rejects it. A zero-value authorisation is a real thing in card payments.
Currently a deliberate choice, not an oversight, but it is a choice.

### Why is the DLQ a Delta table and not the `txn.dlq` Kafka topic?

The topic exists and is provisioned, but bronze writes dead letters to Delta. The
reasoning: a parse failure inside the pipeline is durable state you want to query
and replay from, not a message to hand to another consumer. The Kafka topic is
intended for the Java ingest gateway (P2) to reject on, before anything reaches the
log. Both can coexist; only one is built.

### Is `failOnDataLoss=false` hiding a real problem?

It stops the stream dying when a topic is recreated underneath it, which is an
operational event. It would also silently mask genuine retention-driven data loss.
In production this should probably be `true` with alerting, and it is `false` here
because tests create and delete topics constantly.

### Why is lateness only simulated backwards?

Because the model refuses a future `event_time`. Clock-skew-forward is a real
production defect, arguably more common than network lateness on mobile, and it is
not exercised anywhere in this project.

### What happens when a batch straddles a partition boundary under load?

The ingest stamp is taken once per batch on the driver, so a single batch lands in
exactly one date partition. Under a long backlog drain, a batch could cover hours of
event time while being stamped with a single ingest instant. That is correct by
design (ingest date means ingest date), but it makes "rows per ingest date" a
misleading operational metric during a catch-up.

### Is `maxOffsetsPerTrigger` part of the contract?

`test_deleting_the_checkpoint_can_silently_lose_data` shows that changing it between
runs, after clearing a checkpoint, causes silent loss. That implies batch sizing is
not purely a tuning knob: it is coupled to the idempotence scheme. Nothing in the
code enforces or records the value that a given table was written with.

### How would you detect the silent-loss case in production?

Currently: you would not. There is no reconciliation between Kafka offsets consumed
and rows landed. The provenance columns (`kafka_partition`, `kafka_offset`) make
such a check possible, and nothing performs it yet. This is the single most useful
thing to build next if someone asks "how would you harden this?".
