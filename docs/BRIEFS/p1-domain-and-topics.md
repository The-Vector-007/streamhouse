# P1 brief — domain model, Kafka keys, and dirty data

## What you need to understand before coding

### 1. Key vs value, and what the key actually controls

A Kafka record has a key and a value. The key is not metadata — it decides the
partition, via `hash(key) % numPartitions`.

That gives you exactly one guarantee, and it is narrow: **records with the same key
land on the same partition, and within a partition order is preserved.** Across
partitions there is no ordering at all.

So the key is an ordering decision disguised as an identifier. Choose it by asking
"what must stay in order relative to each other?", never "what uniquely identifies
this row?".

Read:
- Kafka docs, *Design → Message Delivery Semantics*
- Confluent, *Choosing the number of partitions*

### 2. Why partition count is hard to change later

Adding partitions rehashes future keys but not existing ones. A key that lived on
partition 2 for six months may now route to partition 5, while its history sits on
2. Any consumer keeping per-key state now has that state split across partitions,
and per-key ordering is broken across the boundary.

That is why you pick a partition count with headroom and treat increases as a
migration, not a config tweak. Be ready to say this out loud.

### 3. Money is not a float

`0.1 + 0.2 != 0.3` in IEEE 754. Payments systems store **minor units as integers**
(₹12.34 → `1234`). The currency's exponent tells you where the point goes, and it
is not always 2 — JPY has 0.

If you ever see a `double` amount in a payments codebase, that is a bug someone has
not hit yet.

### 4. Dirty data on purpose

Your generator emits defects deliberately. A pipeline that only works on clean data
is a pipeline that has not been tested.

| Defect | Realistic cause | Which layer should catch it |
|---|---|---|
| duplicate id | client retry after timeout | silver dedup (and the P2.4 idempotency key) |
| out-of-order `event_time` | mobile client with a bad clock, network delay | silver watermark |
| null required field | upstream schema drift | bronze schema enforcement |
| negative amount | refund modelled wrong | silver validation → quarantine |
| unknown currency | new market launched without telling you | silver validation → quarantine |

Note the pattern: **bronze enforces structure, silver enforces meaning.** Bronze
asks "is this parseable?", silver asks "is this true?".

## Answer these before you write code

1. You partition a payments topic by `transaction_id`. A consumer needs to detect
   duplicate charges per account. What breaks, and what should the key have been?
2. Why 6 partitions and not 1? What would force you to change it, and why is that
   change expensive?
3. Your generator emits 5% duplicates and 2% late events. Why would a pipeline that
   passes on clean data fail here, and which layer catches each defect?

Write your answers in `docs/ANSWERS.md` **before** implementing. Guessing is fine.
Being deliberately vague to avoid being wrong is not — that habit shows up in
interviews.
