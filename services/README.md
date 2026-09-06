# JVM services

Two Java 21 services sit at the edges of the pipeline. You create both, including
their `pom.xml` — that is part of P2.1 and P6.1, not something handed to you.

| Directory | Phase | What it does |
|---|---|---|
| `ingest-gateway/` | P2 | HTTP → Kafka. Virtual threads, bounded queue, idempotency keys, graceful shutdown. |
| `serving-api/` | P6 | REST over the gold marts. Connection pooling, caching, p99 under load. |

Why Java here and PySpark in the pipeline: see `docs/DECISIONS.md`.

`make test-java` runs `mvn test` in every subdirectory that has a `pom.xml`, so it
is a no-op until you create one.
