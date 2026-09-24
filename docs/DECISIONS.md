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

## The Python pipeline is built for me, not by me (2026-09-24)
**Rejected:** writing every line myself, which was the original rule in `CLAUDE.md`.
**Why:** the referral applications to Amazon, Google, Apple and Microsoft need a
working repo now, and I cannot write P3 through P8 at the speed that needs. I took
the trade knowingly.
**Cost:** the original rule existed because code I did not write is code I cannot
defend in an interview, and that cost is real. Mitigation is that I read the code
daily, and each phase ships with `docs/WALKTHROUGH-*.md` plus an entry in
`docs/OPEN-QUESTIONS.md` marking where an interviewer would push. Java (`services/`)
stays out of scope and stays mine.

## Defects that the domain model refuses are emitted as raw bytes, not Transactions
**Rejected:** relaxing `Transaction`'s validation so the generator can build
defective instances; and bypassing the constructor with `object.__new__`.
**Why:** `DefectRates` asks for `null_field`, `negative_amount` and
`unknown_currency`, but `Transaction.__init__` raises `ValueError` for exactly
those, plus zero amounts and future `event_time`. That validation is correct and
worth keeping: the model should refuse to represent an invalid transaction. So the
corrupt fraction is emitted as raw JSON bytes that never go through the model,
which also gives bronze's DLQ path something real to catch. `generate_batch`
returns `Transaction` objects for the clean, duplicate and late fractions only.
**Cost:** the generator has two output shapes, and callers must handle both.

## Lateness is simulated backwards, never forwards
**Rejected:** modelling a client with a clock running fast.
**Why:** `Transaction._validate` rejects any `event_time` in the future, so a
forward-skewed event cannot exist as a `Transaction` at all. Late data is modelled
by pushing `event_time` further into the past, which is also the realistic case:
events arrive late because of network delay and mobile buffering, not because
clocks run ahead.
**Cost:** clock-skew-forward, a real production defect, is untested here.

## Data-quality checks gate the run on a rate, not on a single bad row (2026-09-24)
**Rejected:** failing the run on any failed check, and the previous behaviour of
computing `passed` and having nothing read it.
**Why:** the second was the real problem. `dq_results` emitted a `passed` flag that
no code consumed, so the pipeline reported quality without ever acting on it, and
"data-quality gates" was a word the code did not earn. Failing on a single bad row
would have been worse: upstream emits bad records continuously, the live warehouse
sits at ~2% on two checks, and a gate that is red every night is one nobody reads.
The threshold catches the thing that matters, which is the **rate moving**: a new
country code nobody announced, or an upstream schema change, arrives as a step
change well clear of 5%.
**Cost:** a number that has to be tuned per check, and a `--dq-threshold` escape
hatch so a known-bad backfill can be pushed through deliberately rather than by
commenting the check out. The DQ rows are written **before** the gate raises, so a
failed run still leaves the evidence of why on disk. Gold is rebuilt before the
raise too: the rows that reached silver are valid, and leaving gold stale would
turn a quality alert into a second outage.
