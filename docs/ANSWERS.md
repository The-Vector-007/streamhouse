# Interview answers

For every task marked **Answer first**, write your answer here **before** you write
the code. Then come back after the task is done and write a second answer.

Do not edit the first answer. The gap between the two is what you learned, and
re-reading these before an interview is worth more than re-reading the code.

Format:

```markdown
## P1.1 — Partitioning a payments topic by transaction_id

**Before** (2026-09-02)
> ...what you actually think right now, even if unsure. Guessing is fine.
> Being vague to avoid being wrong is not.

**After** (2026-09-03)
> ...what you know having built it. Name the thing that surprised you.
```

---

## P1.1 — Partitioning a payments topic by transaction_id

Question - You partition a payments topic by transaction_id. A consumer needs to detect duplicate charges per account. What breaks, and what should the key have been?

**Before** (2026-09-06)
> data per transaction id will be stored in each partition
> one user can make multiple transactions and there can be multiple users
> per account duplicate detection - need to get data per account/user which can be distributed across multiple partitions
> what breaks? - need to collect data across all partitions and then filter out data per account and then detect
> key should have been - account id

## P1.2 — Number of partitions

Question - Why 6 partitions and not 1? What would force you to change it, and why is that change expensive?

**Before** (2026-09-06)
> as data grows, a single partition will have too much data to store and process and eventually the consumers will face performance bottleneck while consuming messages, so to increase throughput no of paritions increased from 1 to 6.
> Increasing number of paritions is expensive as ordering of messages gets affected as messages for the same key can go in different parition
> Other issues 
    > No of partition increase - Requires more open file handles | partition - directory with two files : index file and data file. both needs to be managed via file handle
    > Increases unavailability - partitions are replicated, if they are increases replication increases as well leading to less availability
    > More paritions increase end to end latency - messages are commited and only after that they are visible to consumers. commiting a messages means it should be present acorss all the in sync replicas(ISR), and it takes time.
    > produces buffer and consumer buffer will have to increase leading to increase in client memory

## P1.3 — Dirty Data

Question - Your generator emits 5% duplicates and 2% late events. Why would a pipeline that passes on clean data fail here, and which layer catches each defect?

**Before** (2026-09-13)
> duplicate events and late events will be addressed in the silver layer.
> bronze will allow them as these events are still parseable - structure is present
>pipeline is written in such a way that it assumes whatever event is coming for it to process are valid due to which it runs on clean data but when duplicate or late events come which has a valid structure the same pipeline fails due to which dq checks come into picture!

**After** (2026-09-13)
>first of all dedup is a transformation not dq
>duplicates break row-uniqueness and die in silver dedup because nothing upstream can see them; late events break window completeness, bounded by a watermark in silver with the restatement cost landing in gold; and the real tension is completeness against latency. Then let them pick which thread to pull