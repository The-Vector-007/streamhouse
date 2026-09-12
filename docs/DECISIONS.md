# Decisions

One entry per non-obvious choice. Interviewers ask "why did you do it that way?"
far more often than "how does it work?", and this file is where you win that.

Keep entries short: what you chose, what you rejected, and the trade-off you
accepted. If you changed a test because you preferred a different interface, that
belongs here.

```markdown
## Money is stored in minor units, as integers
**Rejected:** float/double amounts.
**Why:** binary floats cannot represent 0.10 exactly; summing a million of them
drifts. Every payments system stores minor units. Cost: every display path has to
divide, and currency exponent is not always 2 (JPY is 0).
```

---

## Java at the edges, Python in the pipeline
**Rejected:** all-Python, and all-Scala.
**Why:** the Spark work mirrors what I already do at UBS, so PySpark keeps the
resume honest and lets me go deep instead of re-learning syntax. Java goes on the
ingest and serving edges, which is where real systems put it anyway, and it is the
language that was blocking mid-level roles I wanted (Adyen, N26-class fintech).
**Cost:** two build systems in one repo, and a slower start on P2.

## DLQ parition size
A DLQ needs neither throughput nor per-account ordering, and one partition means dead letters stay in arrival order
