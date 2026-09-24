# Walkthrough: Airflow, maintenance, and why they exist

Stage 4. The pipeline already worked without this. What it did not have was
anything to run the work that belongs to *no particular batch*.

---

## Why an orchestrator at all

Two of the three tasks in the DAG are not part of any ingest:

- **Compaction.** Streaming writes leave small files. One micro-batch every few
  seconds is thousands of parquet files a day, and a query then spends its time
  opening files rather than reading rows. Nobody's batch owns fixing that.
- **File expiry.** Delta keeps tombstoned files so that a reader which started
  before a rewrite can finish, and so time travel has somewhere to travel to.
  Someone has to eventually delete them.

That is the honest answer to "why do you need Airflow here?", and it is better
than "because pipelines have Airflow".

## The DAG

```
refine  >>  optimize  >>  vacuum
```

**Order matters between the last two.** OPTIMIZE creates the compacted files and
tombstones the old ones. Running VACUUM first would find nothing to collect, and
the small files would survive another day.

**Every task is idempotent**, which is what makes `retries: 2` safe rather than a
data-corruption setting. `refine` recomputes silver and gold from whatever bronze
holds, so a retry produces the same tables rather than doubled ones. OPTIMIZE on a
compacted table is a no-op. VACUUM only removes files no retained version
references.

**`max_active_runs=1`** because two concurrent refines would both overwrite
silver, and last-writer-wins is not a plan. This matters specifically during a
backfill, which is exactly when you are most likely to have several runs queued.

**Backfill** works because each task takes the logical date as its run id, so the
data-quality rows are labelled per day instead of collapsing into one run.

## Why Airflow shells out instead of importing

Every task is a `BashOperator` calling the project's own venv:

```
cd $STREAMHOUSE_HOME && mise exec -- uv run python -m streamhouse.pipeline refine
```

Airflow's dependency set and a pinned `pyspark==3.5.3` do not want to live in the
same environment, and Spark needs Java 21 on PATH, which `mise exec` supplies.
Separate venvs mean an Airflow upgrade cannot break the pipeline and vice versa.
It also matches the project's existing rule that Spark runs on the host rather
than in a container, which is what keeps the memory budget survivable.

If a task fails with a JVM error, check that `mise exec` is still in the command.
Without it the failure blames Spark rather than the missing toolchain.

## VACUUM's safety check, and when to disable it

`vacuum()` only sets `retentionDurationCheck.enabled=false` when a caller
explicitly asks for a window shorter than the 168-hour default. Delta refuses
short retention by default because deleting files a live reader still needs
corrupts that reader. Turning the check off is a decision worth making
deliberately, not a line you copy from a tutorial because it made an error go
away.

## What actually ran

```
refine    SUCCESS
optimize  SUCCESS   bronze: 6 files -> 1
vacuum    SUCCESS   all six tables, 168h retention
DagRun    state=success
```

Being able to say "it compacted bronze from six files to one" is worth more than
saying "it has Airflow".
