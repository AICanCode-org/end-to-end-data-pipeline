---
phase: 0
slug: phase-0-setup
title: Setup, the brief and the sources
duration: 2 h
start_ref: main
end_tag: phase-0-end
---
# Phase 0 · Setup, the brief and the sources

## 1. Why it matters
A dashboard nobody can rebuild is a screenshot with a refresh button. Data teams start where engineers start: a
repository that installs with one command, source data whose origin you can prove, tests that say what "right"
means, and a log of how AI helped. In this phase you set up the tools for **one** track, read the brief, meet
BazaarBox's sources (an API and three file drops) and their defects, run the starter, meet the pending tests and
the contract that will guide you, open your first pull request and start your `AI_LOG.md`. You will not write a
single transformation yet, on purpose: first you learn who needs which number, when, and what the sources can and
cannot tell you.

## 2. Objectives
By the end of this phase you can:
- Fork the repository **with all branches and tags**, clone it and add the `upstream` remote.
- Pick a track (SQL · dbt or Python · PySpark), install the toolchain on Windows (WSL2), macOS or Linux, and run `make lint test`.
- Explain the brief in two sentences: who needs which five numbers, by when, and what a wrong number costs.
- Generate the source data, prove it is exactly the published one (the manifest check), call the API stub with a token and list three defects you can see in the raw records, plus the two fields that must never reach the lake.
- Open a pull request that passes CI and write your first `AI_LOG.md` entry.

## 3. Step-by-step instructions

### Step 0.1 · Fork and clone (keep the tags!)
1. On GitHub open `AICanCode-org/end-to-end-data-pipeline` → **Fork**. **Untick "Copy the `main` branch only"**.
   The `solution` branch and the `phase-N-end` tags are your safety net in later phases.
2. Clone **your** fork and connect the original as `upstream`:
   ```bash
   git clone https://github.com/<you>/end-to-end-data-pipeline.git
   cd end-to-end-data-pipeline
   git remote add upstream https://github.com/AICanCode-org/end-to-end-data-pipeline.git
   git fetch upstream --tags
   git tag -l 'phase-*'      # phase-0-end … phase-8-end
   ```
3. **Windows:** do all of this inside **WSL2** (Ubuntu) and keep the repo in the Linux file system
   (`~/code/end-to-end-data-pipeline`), not under `/mnt/c`.

### Step 0.2 · Install the tools for ONE track
Every track needs Git, `make` and Python 3.12: ingestion, the checks, the runner, the DAG and the dashboard are
shared Python. Run the doctor script; it tells you exactly what is missing:
```bash
scripts/doctor.sh
make setup            # .venv with the shared package and both tracks' pinned tools
```
`make setup` installs both tracks so you can run the parity check later; you will only *write* code in one.

<!-- track:sql -->
**SQL · dbt track:** dbt-core 1.12 with the DuckDB adapter. DuckDB is an in-process analytical database: no
server, the warehouse is one file, and it reads the lake's JSONL and CSV files directly. Your silver and gold
models are SQL files in `tracks/sql/models`; dbt builds them in dependency order, runs their tests and enforces
the column contracts.
```bash
.venv/bin/dbt --version                         # Core 1.12.x, duckdb 1.11.x
cd tracks/sql && ../../.venv/bin/dbt parse --profiles-dir .   # the project loads (no data needed)
```
Optional: the DuckDB CLI (`brew install duckdb`, or the binary from duckdb.org) to explore files by hand.
<!-- /track -->

<!-- track:pyspark -->
**Python · PySpark track:** PySpark 4.2 in **local mode**: the Spark engine runs inside one JVM on your laptop, no
cluster. Your silver and gold models are DataFrame functions in `tracks/pyspark/pipewright_spark`; the gold
tables are written as Parquet and registered in the same DuckDB warehouse, so the checks and the dashboard do not
care which track built them. Spark needs Java 17 or 21:
```bash
java -version                                   # 17 or 21
.venv/bin/python -c "from pyspark.sql import SparkSession as S; s = S.builder.master('local[1]').getOrCreate(); print(s.version); s.stop()"
```
If Spark complains about the host name, set `export SPARK_LOCAL_IP=127.0.0.1`.
<!-- /track -->

### Step 0.3 · Read the brief
Read the top of `README.md` (the situation) and `contract/transform-spec.md` §1 (conventions). Write down, in your
own words: **who needs which number, how often, by what time, and what each kind of wrong number costs** (a revenue
figure that double-counts a duplicated payment? a CAC computed with test accounts? yesterday's dashboard shown as
today's?). Phase 1 turns your notes into requirements.

### Step 0.4 · Meet the sources
Generate the source data and check it is byte-for-byte the published one, then start the API stub:
```bash
make source            # generate → check against contract/source-manifest-1.0.json → serve on 127.0.0.1:18140
```
In a second terminal, look at the data the way an engineer does, from the outside:
```bash
curl -s http://127.0.0.1:18140/health
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:18140/v1/orders          # 401: a token is required
curl -s -H 'Authorization: Bearer bazaarbox-dev-token' \
  'http://127.0.0.1:18140/v1/orders?updated_since=2026-06-01T00:00:00%2B05:30&updated_before=2026-06-02T00:00:00%2B05:30&limit=3' \
  | python3 -m json.tool | head -60
head -3 source-data/drops/marketing_spend/spend_2026-06-01.csv
head -2 source-data/drops/payments/payments_2026-06-01.jsonl
cut -d, -f3 source-data/drops/products/products_2026-06-01.csv | sort | uniq -c
```
Read `source/README.md`. Then note three defects you can see without any code (two time-zone formats? amounts as
text with a rupee sign? the same payment event twice? a category with a leading space?), and find the two fields
the contract says must be dropped before anything is written (`contract/data-contract.yaml`, `pii_drop`).

### Step 0.5 · Run the checks and meet the pending tests and the contract
```bash
make lint test
```
All the tests are **skipped**. They are *pending*: the complete test suite is already written and switched off in
one `PENDING` list per suite:

| Suite | Pending list |
|---|---|
| shared package | `pipewright/tests/conftest.py` |
| SQL · dbt | `tracks/sql/pytests/conftest.py` |
| Python · PySpark | `tracks/pyspark/tests/conftest.py` |
| Airflow DAG | `orchestration/tests/conftest.py` |

Each entry names a milestone ("Phase 3 M2 - silver (staging) models"). In Phase 3 you delete one entry at a time,
watch the tests go red, and make them green.

Now open the files of `contract/` that make "right" precise (`contract/README.md` lists them all):
- `data-contract.yaml`: what each source must send, the enums, every gold table's columns and types, and the
  data-quality checks with their severities;
- `transform-spec.md`: every rule in words: parsing, silver, gold, the five reports;
- `fixture/source/` + `golden/fixture/`: three days of source data and the gold tables they must produce, small
  enough to check by hand; `golden/v1.0/`: the reference reports for the full three months.

### Step 0.6 · Your first pull request
1. Create a branch: `git switch -c chore/phase-0-setup`.
2. Fill in the first entry in `AI_LOG.md`, e.g. how an assistant explained a `java` / `dbt` / `pip` error during the
   install and how you checked its advice.
3. Commit and push, then open a PR **in your fork** against your `main`:
   ```bash
   git add AI_LOG.md
   git commit -m "docs: start AI_LOG for phase 0"
   git push -u origin chore/phase-0-setup
   ```
4. Open the **Actions** tab. Forks have Actions switched off until you click **"I understand my workflows, go ahead
   and enable them"**. CI runs lint + unit tests for the shared package and both tracks. Wait for green, fill in the
   PR template and merge.

## 4. AI-assist prompts

### Prompt 1: Fix my toolchain install
```text
I'm on <Windows 11 + WSL2 Ubuntu 24.04 | macOS 15 | Ubuntu 24.04>. I want to run a project that needs Python 3.12
with a virtual environment, <dbt-core 1.12 with dbt-duckdb | PySpark 4.2 in local mode with Java 21>, make and
shellcheck. Here is the output of scripts/doctor.sh: <paste output>. Give me the exact commands to install what is
missing, explain each one in one line, and tell me how to verify each install.
```
**Verify the output by:** running each verification command, re-running `scripts/doctor.sh` until every required
line is green, and checking any download URL against the official site (python.org, adoptium.net, docs.getdbt.com,
spark.apache.org, docs.docker.com). Never pipe an unknown URL into `bash`.

### Prompt 2: Restate the brief as requirements
```text
Here is a stakeholder brief: <paste the "situation" paragraph of README.md>. Restate it as: (1) who needs which
number, at which grain, how often and by what time, (2) for each of the five reports, the questions I must ask
before I can define it exactly (which orders count as revenue? which day does an order belong to?), (3) three ways
the dashboard could show a plausible but wrong number, (4) what "fresh" and "correct" should mean, measurably.
Do not invent numbers or facts that are not in the brief.
```
**Verify the output by:** checking every claim against the brief (assistants add details that are not there, such
as a revenue target or a data warehouse BazaarBox does not have), and keeping at least one failure mode about
*time* (a time zone, a late update, a late file). `contract/transform-spec.md` §1 answers several of the questions.

### Prompt 3: Read a source like a data engineer
```text
Here are 3 records from a REST API of an e-commerce company and the header plus 5 rows of two daily CSV files:
<paste the curl output and the head of the spend and products files>. List every data-quality problem you can see
or suspect, how you would detect each one in code, which fields look like personal data, and what you would ask
the source owner. Do not write any transformation yet.
```
**Verify the output by:** finding each problem in the real data with `grep`, `cut | sort | uniq -c` or a quick
DuckDB query (an assistant may "see" problems that are not in your sample), and comparing your list with
`contract/transform-spec.md` §2-§3. These records are synthetic; with real customer data you would paste schemas
and aggregates only, never rows with emails or phone numbers.

## 5. Deliverables
- [ ] Your fork contains the `solution` branch and the `phase-*` tags (`git tag -l` shows them).
- [ ] `make lint test` passes locally (all tests skipped).
- [ ] `make source` says the generated files are identical to the manifest, and you called the API with a token.
- [ ] A note (in your PR description or `AI_LOG.md`): the five reports and their deadline, three source defects, and the two PII fields.
- [ ] A merged PR with a green CI run.
- [ ] `AI_LOG.md` has its first entry.

## 6. Self-check quiz

### Q1. Why must you untick "Copy the main branch only" when forking?
- [ ] Otherwise GitHub Actions is disabled in the fork
- [x] Otherwise the `solution` branch and the phase tags are missing from your fork
- [ ] Otherwise you cannot open pull requests
- [ ] Otherwise the source data is not copied
> **Answer:** Forks copy only `main` by default. The tags (`phase-N-end`) live on the `solution` branch; without them you cannot compare your work or catch up. Fix it later with `git fetch upstream --tags`.

### Q2. Why does `make source` check the generated files against `contract/source-manifest-1.0.json`?
- [ ] To make generation faster
- [ ] To encrypt the source data
- [x] To prove your source data is byte-for-byte the data the golden tables were computed from, so a difference in your numbers is a bug in your code, not different input
- [ ] Because Git cannot store JSONL files
> **Answer:** The manifest lists the sha256 of every generated file. Same seed, same bytes. If the check fails, nothing downstream can be compared with the golden tables.

### Q3. The orders API returns an order on 1 June and the same order again on 4 June. What happened?
- [ ] The API has a bug and returns duplicates
- [x] The order changed (e.g. it was delivered or cancelled), so its `updated_at` moved into a later extraction window; the pipeline must keep the latest version
- [ ] A customer placed the same order twice
- [ ] The pagination cursor was reused
> **Answer:** Incremental extraction by `updated_at` sees every change of a record, so the same key appears in several daily partitions. Silver keeps the latest version per key (`transform-spec.md` §3.1). Counting both would double the revenue.

### Q4. Why are all tests skipped in the starter?
- [ ] They are flaky
- [ ] CI is too slow to run them
- [x] They describe behaviour you build in Phases 3-6 and are switched on one milestone at a time
- [ ] They only run on the solution branch
> **Answer:** The full suite is written up-front (test-first). Deleting a line from a `PENDING` list turns a group back on, so you always have a clear red → green target.

### Q5. Which fields must never reach the lake?
- [ ] `order_id` and `customer_id`
- [x] The customer's `email` and `phone`
- [ ] `ship_city`
- [ ] Every timestamp
> **Answer:** The contract's `pii_drop` lists them for the customers source. None of the five reports needs them, so the safest place to drop them is before the first byte is written: data you never store cannot leak.

### Q6. Which of these may you paste into an AI prompt for this project?
- [ ] Your `.env` file with an API key
- [ ] 500 customer records with emails and phone numbers
- [x] Column names, your own code, an error message and aggregate counts
- [ ] Another student's dbt project
> **Answer:** Secrets and row-level personal data never go into prompts. The BazaarBox data is synthetic, but practise the habit you need at work: schemas, code and aggregates, not rows.

## 7. What you learned
- A reproducible local environment for one track, checked by `scripts/doctor.sh` and `make lint test`.
- The difference between `origin` (your fork) and `upstream` (the course repo), and why tags matter.
- The brief as five numbers with a deadline, and the ways a dashboard can be plausible and wrong.
- BazaarBox's sources from the outside: a paginated, authenticated API with incremental windows and three file drops, each with its defects.
- The contract (data contract, transformation spec, fixture, golden tables, manifest) that defines "right".
- The PR + CI loop and the habit of logging AI assistance without leaking secrets or rows.

## 8. Catch-up git commands
```bash
git fetch upstream --tags
# Start Phase 1 from the reference end of Phase 0:
git switch -c my-phase-1 phase-0-end
# See what the reference changed in this phase:
git diff main phase-0-end --stat
```
