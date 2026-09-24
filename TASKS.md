# Streamhouse build tasks

You implement everything under `src/` and `services/`. This file, the tests, and
`docs/BRIEFS/` are the scaffolding around that.

## How a task works

```
Read first      concepts you need, with links. Do this before opening an editor.
Answer first    an interview question. Write your answer COLD in docs/ANSWERS.md
                before you write code. Revisit it after. The gap between the two
                answers is the thing you actually learned.
Given           a failing test. It pins the interface, not the implementation.
You write       the file(s). Every line.
Done when       the named test passes.
```

**The stuck rule.** Stuck for more than 45 minutes: ask. But ask about the
*concept* ("why does Spark commit offsets after the write?"), never for the code
("write my ingest function"). Code you did not write is code you cannot defend in
an interview, and defending it is the entire point of this project.

**The interface is yours.** The tests encode one reasonable design. If you think
of a better one, change the test and write down why in `docs/DECISIONS.md`.
Interviewers push hardest on design choices, and "I changed it because..." is a
much stronger answer than "that's how it was set up."

---

## P0 — Environment

- [x] **P0.1 Raise WSL memory.** See `docs/RUNBOOK.md`. Do this first and read the
      commit-limit warning; you have crashed this host before by skipping it.
- [x] **P0.2 `make setup`.** Installs Java 21, Maven, Python 3.12, and the venv.
      Done when `java -version` says 21 and `python -V` says 3.12.
- [x] **P0.3 `make up PROFILE=ingest && make topics`.** Kafka starts, `topics`
      returns empty (no topics yet — that is P1).

---

## P1 — Domain and generator

### P1.1 The transaction model

> **Read first** (~20 min) — `docs/BRIEFS/p1-domain-and-topics.md`
> Kafka keys vs values, partitioning, and why key choice determines ordering.

> **Answer first** — in `docs/ANSWERS.md`:
> *"You partition a payments topic by `transaction_id`. A downstream consumer needs
> to detect duplicate charges per account. What breaks, and what should the key
> have been?"*

- **You write:** `src/streamhouse/domain/transaction.py`
- **Given:** `tests/test_domain.py`
- **Done when:** `uv run pytest tests/test_domain.py` passes.

A UPI-style transaction: id, account, counterparty, amount (minor units — never
floats for money, and be ready to say why), currency, status, `event_time`,
`ingest_time`. Serialises to and from JSON bytes for Kafka.

### P1.2 Topic creation

> **Answer first:** *"Why 6 partitions and not 1? What would force you to change it
> later, and why is that change expensive?"*

- **You write:** `src/streamhouse/generator/topics.py`, plus a `make topics-create` target
- **Done when:** `make topics` lists `txn.raw` and `txn.dlq`.

### P1.3 The generator

> **Read first** — `docs/BRIEFS/p1-domain-and-topics.md`, the "dirty data on
> purpose" section.

> **Answer first:** *"Your generator emits 5% duplicates and 2% late events. Why
> would a pipeline that passes tests on clean data fail on this, and which layer
> should catch each defect?"*

- **You write:** `src/streamhouse/generator/produce.py`
- **Given:** `tests/test_generator.py`
- **Done when:** tests pass and `python -m streamhouse.generator.produce --rate 100 --seconds 5`
  puts ~500 messages on `txn.raw`.

Emit deliberate defects: duplicate ids, out-of-order `event_time`, null required
fields, negative amounts, unknown currencies. Everything downstream exists to
handle these, so if the generator is too clean the rest of the project is theatre.

---

## P2 — Java ingest gateway *(new language — take your time here)*

This is the phase that opens doors. Adyen, N26 and most mid-level backend reqs
you looked at want exactly this: an HTTP service under concurrency talking to a
broker.

### P2.0 Java, coming from Python

> **Read first** (~45 min) — `docs/BRIEFS/p2-java-from-python.md`
> Compile step, no truthiness, `==` vs `.equals()`, checked exceptions, records,
> and the big one: **there is no GIL**. Threads genuinely run in parallel, so
> shared mutable state is a real bug rather than a theoretical one.

> **Answer first:** *"In Python I could increment a counter from two threads and
> usually get away with it. Why is that not true in Java, and what would I use
> instead?"*

No code for this task. If you can answer that cold, the rest of P2 is mechanics.

### P2.1 Project setup

> **Read first** (~40 min) — `docs/BRIEFS/p2-java-concurrency.md`
> Java 21 virtual threads, structured concurrency, and why blocking IO stopped
> being expensive.

- **You write:** `services/ingest-gateway/pom.xml` and a hello-world HTTP server
- **Done when:** `cd services/ingest-gateway && mvn test` runs green.

Use the JDK's built-in `HttpServer` or Javalin. Not Spring Boot — you want to see
the machinery, and "I used Spring" answers nothing in an interview.

### P2.2 Produce to Kafka

> **Answer first:** *"Your gateway returns 200 to the client. Where exactly is the
> message at that moment, and what does `acks=all` change about your answer?"*

- **You write:** the producer path, `POST /v1/transactions`
- **Given:** `services/ingest-gateway/src/test/java/.../IngestHandlerTest.java`
- **Done when:** a POST lands on `txn.raw`.

### P2.3 Virtual threads and backpressure

> **Read first** — the backpressure section of the P2 brief.

> **Answer first:** *"10,000 concurrent requests arrive and Kafka slows to 500/s.
> Describe what your service does. Now describe what it should do."*

- **You write:** virtual-thread executor, a bounded queue, 429 on overflow
- **Done when:** the load test shows rejections instead of unbounded memory growth.

This is the single most interview-valuable task in the project. "I used virtual
threads" is a weak answer. "I bounded the queue because unbounded buffering turns
a latency problem into an OOM" is a strong one.

### P2.4 Idempotency keys

> **Answer first:** *"A client retries a payment POST after a timeout. How do you
> avoid double-charging, and where does that state live?"*

- **Done when:** the same `Idempotency-Key` twice produces one Kafka message.

### P2.5 Graceful shutdown

> **Answer first:** *"SIGTERM arrives with 200 requests in flight. What must happen
> before the process exits, and what is the failure mode if you skip it?"*

- **Done when:** SIGTERM drains in-flight requests and flushes the producer, losing nothing.

---

## P3 — Bronze: exactly-once *(the headline claim)*

### P3.1 Local Spark + Delta

> **Read first** (~30 min) — `docs/BRIEFS/p3-exactly-once.md`

- **You write:** `src/streamhouse/spark.py` — the SparkSession builder with Delta configured
- **Given:** `tests/test_spark_session.py`

### P3.2 Streaming ingest

> **Answer first:** *"A micro-batch writes 10k rows to Delta, then the driver is
> killed before the offset commit. What happens on restart, and why is the result
> exactly once rather than twice?"*

- **You write:** `src/streamhouse/bronze/ingest.py`
- **Given:** `tests/test_bronze_ingest.py`

### P3.3 Failure injection — prove it

> This is the test that makes the README claim honest. Without it, "exactly-once"
> is marketing.

- **Given:** `tests/test_bronze_exactly_once.py`
- **Done when:** `test_kill_mid_batch_does_not_duplicate` passes.

---

## P4 — Silver: validation, dedup, quarantine

Tasks written when you get here. Interview payload: watermarks and late data, why
quarantine beats dropping, and how you pick a dedup window.

## P5 — Gold: aggregate marts
Idempotent recompute vs append. Why most "incremental" pipelines are secretly not.

## P6 — Java serving API
REST over Delta/DuckDB. Connection pooling, caching, p99 under concurrency.

## P7 — Load test and measurement ← **the one your resume needs**
Throughput, p99, and **what broke first when you pushed past it**. Record results
in `docs/BENCHMARKS.md`. That last question is what separates you from candidates
quoting a happy-path number they cannot explain.

## P8 — Airflow *(optional)*
Backfills, OPTIMIZE, VACUUM. Idempotent backfill design.

## P9 — CI, observability, architecture docs *(optional)*

---

## Ground rules

1. **Never push code you cannot explain line by line.** If you pasted it, either
   understand it or delete it.
2. **The README never claims what the code does not do.** Update the roadmap when a
   phase lands, not when you start it.
3. **Commit per task**, Conventional Commits, body explains *why*.
4. **`docs/ANSWERS.md` is the real deliverable.** The code proves you can build it.
   That file proves you can explain it, which is what actually gets you hired.
