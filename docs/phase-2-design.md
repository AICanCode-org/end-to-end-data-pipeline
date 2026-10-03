---
phase: 2
slug: phase-2-design
title: "Design: architecture, the data contract, layers and ADRs"
duration: 4 h
start_tag: phase-1-end
end_tag: phase-2-end
---
# Phase 2 · Design: architecture, the data contract, layers and ADRs

## 1. Why it matters
The old version of this project started with a tool list: Airbyte, Delta Lake, a Spark cluster, dbt, Airflow,
Metabase, Postgres. Together they need more than 16 GB of RAM, and none of them answers the questions that decide
whether the dashboard is right: where does each record land, can a day be re-run without duplicates, what happens
when a check fails, who may read a half-built table? Design is answering those questions **before** the code, and
choosing tools that fit the answers and the laptop. In this phase you draw the architecture, define the layers and
their contracts, read the data contract that pins every source and gold table, and record the five decisions that
shape the project as Architecture Decision Records (ADRs).

## 2. Objectives
By the end of this phase you can:
- Draw the pipeline: sources → ingestion → lake (bronze) → silver → gold → checks → publish → dashboard, and say what each step guarantees.
- Explain bronze / silver / gold (the "medallion" layers): grain, mutability, who writes and who reads each.
- Design idempotent ingestion: extraction windows, cursors, retries, partition replacement, manifests, PII handling.
- Design a publish gate so the dashboard never reads a half-built or failed build.
- Read `contract/data-contract.yaml` and say what each section enforces.
- Justify every tool choice against the requirements and an 8 GB RAM budget, and write five ADRs.

## 3. Step-by-step instructions

### Step 2.1 · Draw the architecture
Copy `spec/design-template.md` to `spec/design.md` and draw §1 (Mermaid renders on GitHub):
```text
flowchart LR
  API[orders/customers API] --> ING[ingest]
  DROPS[file drops] --> ING
  ING --> BRONZE[(lake: raw, immutable)]
  BRONZE --> SILVER[silver: one clean row per entity]
  SILVER --> GOLD[gold: facts, dims, 5 reports]
  GOLD --> DQ{checks}
  DQ -- pass --> PUB[(published warehouse)]
  DQ -- fail --> INC[incident report]
  PUB --> DASH[dashboard]
```
For each box write one paragraph: input, output, the guarantee it gives the next box.

### Step 2.2 · Layers and their contracts
Fill in §2. Think about each row: **bronze** keeps every delivery exactly as received (minus PII), partitioned by
delivery date, never edited; silver and gold are always rebuilt from it, so a fixed rule fixes history. **Silver**
has one clean row per business entity (the latest order version, one row per payment event). **Gold** has the
facts, dimensions and the five reports, with the exact columns of the contract. Who may read silver? (Only gold
models.) Who may read gold? (Only through the published warehouse.)

### Step 2.3 · Ingestion that can be re-run
Fill in §3. Decide and justify:
- The extraction window for day D: `[D 00:00 IST, D+1 00:00 IST)` on `updated_at`, half-open, so no record is in two
  windows of the same run and none is missed at midnight.
- Pagination (follow `next_cursor`), retries (429/503 with exponential backoff and jitter, honour `Retry-After`;
  never retry a 401), a timeout on every request.
- Writing: the day's partition is written to a temporary directory and swapped in whole; re-running D replaces it.
  A manifest records what was loaded (counts, sha256, contract version).
- Bad records: rows missing a required field go to `lake/quarantine/` with the reason, not into bronze, and are counted.
- PII: `email` and `phone` are dropped before the first byte is written.

### Step 2.4 · Read the data contract
Open `contract/data-contract.yaml` and annotate (in your design) what each section enforces:

| Section | Enforced by | When it fails |
|---|---|---|
| `sources` (required fields, primary key, `pii_drop`) | ingestion | the record is quarantined; PII never lands |
| `enums` | `accepted_values` checks, dbt tests | the build is not published |
| `gold` (columns, types, order) | dbt model contracts / PySpark `enforce()` | the build fails before it is checked |
| `checks` (severity `error`/`warn`) | `pipewright dq` | `error`: not published; `warn`: published, reported |

Then read `contract/transform-spec.md` once end to end and mark the three rules you think are easiest to get wrong.

### Step 2.5 · The publish gate
Fill in §5. The dashboard reads `warehouse/pipewright.duckdb`. Every run builds into a **separate build file**,
runs the checks on it, records the verdict in the build itself (`ops.build_info`), and only then swaps the build
into place with one atomic rename. A crash, a failed check or a half-written table can therefore never reach the
dashboard: it keeps the last good build, which knows its own business date.

### Step 2.6 · Tools and footprint
Fill in §7. For each tool of the old design, decide keep / replace and why, with the RAM it needs on a laptop.
Then compare with the reference decisions (after writing yours): `git show phase-2-end:spec/design.md`.

<!-- track:sql -->
**SQL · dbt track:** design the dbt project in §4: `models/staging` (one model per source, reading the lake files
through the `lake_glob()` macro, never the API; materialized as tables so the published file is self-contained), `models/gold` (tables, with `contract: {enforced: true}` so the
column names and types of `data-contract.yaml` are checked at build time), seeds (`city_map`), and where the tests
will go (generic tests in YAML, singular tests in `tests/`, unit tests for the trickiest rules). Note why the
models must filter `dt <= run_date` (a backfill must never see the future).
<!-- /track -->

<!-- track:pyspark -->
**Python · PySpark track:** design the package in §4: `bronze.py` (read the lake with explicit schemas, never
inferred), `parsing.py` (pure column expressions: time zones, paise), `staging.py` (silver), `gold.py` (facts,
dims, reports), `pipeline.py` (`enforce()` the contract's column types, write Parquet, register the tables in the
DuckDB warehouse). Note why local mode (`local[2]`) with a 1 GB driver is enough for this data and how you will keep Spark's
session time zone from changing your dates (`spark.sql.session.timeZone=UTC`, parse offsets yourself).
<!-- /track -->

### Step 2.7 · Write five ADRs
Use `spec/adr/0000-template.md`. One decision per file, 1/2 page each:
1. Where the data lives (a lake of files + an embedded warehouse vs a lakehouse table format on a cluster).
2. How data is ingested (a connector platform vs a small ingestion package you can read and test).
3. How many ways the transformations are written, and how you know they agree.
4. How a build becomes visible (the publish gate).
5. How the pipeline is scheduled (an orchestrator calling one CLI vs an orchestrator that *is* the code).
Commit `spec/design.md` and `spec/adr/000{1..5}-*.md`.

## 4. AI-assist prompts

### Prompt 1: Review my architecture for failure modes
```text
Here is the architecture of a daily batch pipeline: <paste spec/design.md §1-§3>. Sources: a paginated REST API
with updated_at windows and three daily file drops. Walk through these failures one by one and tell me what my
design does in each case, and where it is unclear: (1) the job crashes halfway through writing a partition,
(2) the same day is run twice, (3) an order changes status three days later, (4) a file arrives 3 hours late,
(5) a new enum value appears, (6) the API rate-limits for two minutes. Do not propose new tools.
```
**Verify the output by:** finding, for each failure, the exact sentence in your design that handles it (if there is
none, the AI found a gap; add it), and rejecting answers that rely on a tool or service you did not choose.

### Prompt 2: Argue the other side of a tool decision
```text
I am choosing between <Airbyte | a small Python ingestion package> for ingesting one REST API and three daily
files on an 8 GB laptop, for a teaching project that must run offline. Give the strongest case FOR the option I
rejected, then the costs of each option (RAM, moving parts, what a learner can read and test, what happens at 100x
the data). Be concrete; no marketing language.
```
**Verify the output by:** checking RAM and container-count claims against the tools' official docs (they change
between versions), and making sure your ADR's "Consequences" section lists the best counter-argument honestly.

### Prompt 3: Draft an ADR from my notes
```text
Context notes: <paste your bullet points for one decision>. Write an ADR with sections Context, Options considered
(at least 3), Decision, Consequences (positive and negative). Keep it under 300 words. Use only facts from my notes;
mark anything you had to assume with [ASSUMPTION].
```
**Verify the output by:** removing or confirming every [ASSUMPTION], checking each option was really considered,
and making sure the decision can be tested (e.g. "a crash during publish leaves the old warehouse readable" is a test
you will write in Phase 3).

## 5. Deliverables
- [ ] `spec/design.md` with the architecture diagram and all eight sections.
- [ ] A layer table (bronze / silver / gold) with grain, mutability, writers and readers.
- [ ] An ingestion design that explains windows, retries, idempotent partition writes, quarantine and PII handling.
- [ ] The publish gate described so that a crash at any point leaves the dashboard on the last good build.
- [ ] Five ADRs in `spec/adr/`.
- [ ] An `AI_LOG.md` entry, including one AI suggestion you rejected and why.

## 6. Self-check quiz

### Q1. Why is the bronze layer immutable and partitioned by delivery date?
- [ ] Because files are cheaper than tables
- [x] So silver and gold can always be rebuilt from exactly what was received; a fixed rule fixes history, and re-running a day replaces only that day's partition
- [ ] Because dbt cannot update files
- [ ] To save memory
> **Answer:** If bronze were edited in place you could never reproduce yesterday's numbers or replay a bug fix over history. Partitioning by delivery date makes a re-run replace one partition, never append duplicates.

### Q2. Why is the API extraction window half-open, `[D 00:00 IST, D+1 00:00 IST)`?
- [ ] It is faster
- [x] So every instant belongs to exactly one window: a record updated exactly at midnight is loaded once, not twice or never
- [ ] Because the API requires it
- [ ] To skip test orders
> **Answer:** Closed windows `[a, b]` double-load records at the boundary; open ones lose them. Half-open windows tile the time line exactly.

### Q3. What is the purpose of building into a separate file and swapping it in with one rename?
- [ ] Renames are faster than writes
- [ ] To keep a backup
- [x] The dashboard can only ever see a complete build that passed its checks; a crash or a failed check leaves the last good warehouse in place
- [ ] DuckDB cannot write to an open file
> **Answer:** `os.replace` on the same file system is atomic: readers see either the old file or the new one, never a half-written one. The checks run before the swap, so a failed build is never visible.

### Q4. Why did the design replace Airbyte with a small ingestion package?
- [ ] Airbyte cannot read REST APIs
- [x] For one API and three files, a platform needing several GB of RAM and many containers hides the very ideas the project teaches (windows, cursors, retries, idempotency); a few hundred lines of tested Python show them and run on any laptop
- [ ] Airbyte is not open source
- [ ] Python is faster than Airbyte
> **Answer:** At work, a connector platform is often the right call for dozens of sources. Here the ADR records the trade-off: fewer moving parts and full visibility, at the cost of writing (and testing) the connector yourself.

### Q5. What does an `error` severity on a contract check mean?
- [ ] The pipeline crashes
- [ ] The check is skipped
- [x] The build is not published: the dashboard keeps the last good data and an incident report is written
- [ ] The failing rows are deleted
> **Answer:** `warn` checks are published and reported; `error` checks block the publish gate. Deleting rows to make a check pass would hide the problem.

### Q6. Why must every model filter lake partitions with `dt <= run_date`?
- [ ] To make queries faster
- [x] So a build for a past day (a backfill or a re-run) sees only what had arrived by then and gives the same numbers as the original run
- [ ] Because future partitions are corrupt
- [ ] dbt requires it
> **Answer:** Without it, rebuilding 15 June after September's data has landed would mix in later status changes and give different numbers than the dashboard showed on 16 June. Reproducibility needs a time boundary.

## 7. What you learned
- An architecture expressed as guarantees between steps, not as a tool list.
- Medallion layers with clear grain, mutability and access rules.
- Idempotent ingestion: half-open windows, retries with backoff, partition replacement, manifests, quarantine, PII at the door.
- A publish gate built on an atomic rename.
- Tool choices justified against requirements and a RAM budget, recorded as ADRs.

## 8. Catch-up git commands
```bash
git fetch upstream --tags
git switch -c my-phase-3 phase-2-end
git show phase-2-end:spec/design.md | less
ls $(git rev-parse --show-toplevel)/spec/adr; git show phase-2-end --stat
```
