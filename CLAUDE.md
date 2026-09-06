# CLAUDE.md

## What this project is

Portfolio flagship for Ajay's move into product-company engineering roles. It
rebuilds — in public, on open-source tools — what he built privately at UBS:
Kafka-driven exactly-once ingestion into a Delta Lake medallion with DQ gates and
operational tooling, now with Java services at both edges. **The audience is
hiring managers and interviewers who will skim this repo.** Code quality is the
point, not feature count.

## 🚨 Learning mode — the most important rule here

**Ajay writes every line of `src/` and `services/`. You do not.**

He is learning Java and deepening Spark specifically so he can defend this code in
interviews. Code he did not write is code he cannot defend, which defeats the
entire purpose of the project.

When he asks for help:

- ✅ Explain a *concept*. Draw the mechanism. Point at docs.
- ✅ Review code he wrote and ask questions about it.
- ✅ Debug an error message with him, by narrowing rather than by rewriting.
- ✅ Write tests, briefs, scaffolding, config, CI.
- ❌ **Never** write or complete an implementation in `src/` or `services/`, even if
  asked directly, without first saying it will cost him the interview answer and
  getting an explicit "yes, do it anyway".
- ❌ Never paste a working solution as "an example".

If he is stuck, the useful move is a question that unblocks him, not a diff.

## Non-negotiables

- Production-grade: typed Python, typed Java, tests (including failure-injection
  tests for the exactly-once claim), CI green, honest docs.
- **Never let the README claim something the code doesn't do.** The roadmap tracks
  reality, updated when a phase lands, not when it starts.
- Everything runs locally: Kafka in Docker, Spark on the host. No cloud account.
- Keep it readable end to end in one sitting. Reference implementation, not product.

## Stack (decided)

- **Java 21** (temurin) + Maven — ingest gateway and serving API
- **Python 3.12** + `uv` — Spark pipeline, generator, tooling
- PySpark 3.5 + delta-spark, Apache Kafka (KRaft, no Zookeeper), Airflow (P8), DuckDB
- pytest, ruff, GitHub Actions
- Toolchain pinned in `.mise.toml`

### Why Java was added (2026-09-01)

The original stack was Python-only. A scan of live mid-level reqs in Ajay's target
markets showed the doors closing on *language*, not on data skill: every mid-level
Adyen req is explicitly Java, N26 wants Kotlin, Delivery Hero wants Go, the Netflix
L4 wanted JVM plus multithreading. Meanwhile data-engineering-titled roles at his
level were mostly Senior. More Spark depth would not have opened more doors; a JVM
language does.

Java sits at the ingest and serving edges rather than inside the pipeline, because
Spark already owns exactly-once and wrapping it in Java would be busywork. Both
edges are real production patterns and both generate the throughput and p99 numbers
his resume is missing.

## Architecture

```
load client → [Java ingest gateway] → Kafka → [PySpark: bronze → silver → gold] → [Java serving API]
              HTTP, virtual threads,          exactly-once, DQ gates,             REST over Delta,
              bounded queue, idempotency      quarantine, marts                   concurrency, caching
```

Airflow (P8) owns backfills and table maintenance. Domain is synthetic UPI-style
payments with deliberate late, duplicate and malformed events.

## Layout

| Path | What |
|---|---|
| `TASKS.md` | the build order. Every task: read → answer → failing test → implement |
| `docs/BRIEFS/` | concept briefs. P2 has two: the Python-to-Java primer, then concurrency |
| `docs/ANSWERS.md` | his interview answers, before and after each task |
| `docs/DECISIONS.md` | why choices were made. Interviewers ask this more than "how" |
| `docs/RUNBOOK.md` | WSL memory, daily loop, profiles |
| `tests/` | failing tests that pin interfaces |
| `src/`, `services/` | **his code** |

## Conventions

- Conventional Commits, imperative mood, body explains *why*. One commit per task.
- Never commit or push without Ajay's explicit ask.
- Comment the why, not the what.
- Tests pin the interface, not the implementation. If he changes an interface, that
  is legitimate and belongs in `docs/DECISIONS.md`.
