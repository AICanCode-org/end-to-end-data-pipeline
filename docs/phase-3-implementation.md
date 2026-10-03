---
phase: 3
slug: phase-3-implementation
title: "Implementation in four milestones: ingest, silver, gold, run"
duration: 15 h
start_tag: phase-2-end
end_tag: phase-3-end
---
# Phase 3 · Implementation in four milestones: ingest, silver, gold, run

## 1. Why it matters
This is where most pipelines quietly go wrong: an order counted twice because it changed status, a payment event
delivered twice, a timestamp shifted by five and a half hours because the server runs in UTC, rupees summed as
floats, a re-run that appends a second copy of yesterday. You build the pipeline **test-first**, one milestone at a
time: delete a line from a `PENDING` list, watch its tests fail, implement until they pass. Every function and model
has a `TODO` with a hint that says where in `contract/transform-spec.md` to look and which trap to avoid. At the end,
one command ingests three months of BazaarBox data, builds silver and gold, and publishes a warehouse whose five
reports match the golden tables to the paisa.

## 2. Objectives
By the end of this phase you can:
- Ingest an API with windows, pagination and retries, and file drops with validation, into idempotent lake partitions without PII (M1).
- Parse mixed time-zone timestamps, IST business dates and rupee text exactly, and build silver: the latest version per key, one row per event (M2).
- Build the gold dimensions, facts and the five reports with the exact contract columns (M3).
- Run the whole thing for a date or a range, publish atomically, and schedule it with an Airflow DAG (M4).
- Prove your gold tables equal `contract/golden/` on the fixture and on the full history.

## 3. Step-by-step instructions

### Step 3.1 · The red-green loop
For every milestone:
1. Delete the milestone's line from the `PENDING` list in **every** conftest that has it
   (`pipewright/tests/conftest.py`, `tracks/<your track>/.../conftest.py`, `orchestration/tests/conftest.py`).
2. Run the tests: they fail with `NotImplementedError: Phase 3 Mx ...` (red).
3. Implement one function or model at a time, re-running the tests (green). Commit after each green step:
   `feat(pipewright): M1 HTTP client with backoff and Retry-After`.
4. Never edit a test to make it pass. If you believe a test is wrong, open an issue quoting the spec section.
```bash
make test-pipewright          # the shared package
make test-sql                 # or: make test-pyspark
make lint                     # ruff, sqlfluff, shellcheck: keep it green as you go
```
Find every stub of a milestone with `grep -rn "Phase 3 M1" pipewright tracks orchestration`.

### Step 3.2 · M1: ingestion (shared by both tracks)
Remove `"Phase 3 M1 - ingestion"`. Implement, in this order:
1. `pipewright/http.py`: `HttpClient.delay` (exponential backoff with jitter, capped; honour a numeric
   `Retry-After`) and `get_json` (bearer token, timeout, retry 429/5xx/connection errors up to `max_attempts`,
   **never** retry 401 or other 4xx; raise `SourceError` when retries run out).
2. `pipewright/lake.py`: `write_partition`: write every file into a temporary sibling directory, then swap it in
   with a rename, so a crash leaves either the old partition or the new one.
3. `pipewright/ingest.py`: `fetch_window` (the half-open window from `config.window(day)`, follow `next_cursor`),
   `conform` (drop `pii_drop` fields, reject rows missing a required field), `ingest_api`, `validate_file`,
   `ingest_file` (a missing drop raises `MissingDrop` → exit code 3).

Try it against the stub (`make source` in another terminal):
```bash
.venv/bin/pipewright ingest --all --date 2026-06-01
ls lake/raw/*/dt=2026-06-01/ && cat lake/_manifests/orders/2026-06-01.json
grep -c email lake/raw/customers/dt=2026-06-01/* || echo "no PII in the lake"
.venv/bin/pipewright ingest --all --date 2026-06-01 && find lake/raw -type f | wc -l    # same count: idempotent
SOURCE_FAULT_RATE=0.3 make source      # restart the stub with faults; ingestion must still succeed
```
The track suites use M1 too: their fixtures ingest the 3-day fixture through your code (`pipewright.testkit`).

### Step 3.3 · M2: silver (staging) models
Remove `"Phase 3 M2 - silver (staging) models"`. Start with parsing (`transform-spec.md` §2), then one model per
source (§3.1-§3.6). The tests compare every silver table with `contract/golden/fixture-silver/`.

<!-- track:sql -->
**SQL · dbt:** implement the macros in `tracks/sql/macros/parsing.sql` (`utc_timestamp`, `ist_date`, `to_paise`),
then the models in `models/staging/`. Read the lake with `read_json(... '{{ lake_glob("orders", "jsonl") }}' ...)`
and `filename=true` to get the partition `dt`; keep only `dt <= {{ run_date() }}`. The latest version per key is a
window function:
```sql
qualify row_number() over (partition by order_id order by updated_at_utc desc, dt desc) = 1
```
Unnest order items together with their position (`unnest(generate_series(1, len(items)))` next to `unnest(items)`
in the same select) so `line_number` is the 1-based position in the *original* array, then drop `qty <= 0`. Run one model at a time:
```bash
cd tracks/sql && ../../.venv/bin/dbt build --profiles-dir . --select stg_orders --vars '{run_date: "2026-06-03"}'
```
(The tests run dbt for you against a temporary build of the fixture; this command is for exploring.)
<!-- /track -->

<!-- track:pyspark -->
**Python · PySpark:** implement `pipewright_spark/parsing.py` (`utc_timestamp`, `ist_date`, `to_paise`,
`clean_key` as **column expressions**: no Python UDFs, so Spark can optimise them), then `staging.py`. Bronze is read
by `bronze.py` with explicit schemas. The latest version per key is a window:
```python
w = Window.partitionBy("order_id").orderBy(F.col("updated_at_utc").desc(), F.col("dt").desc())
df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
```
Explode items with `F.posexplode` (position + 1 = `line_number`), then drop `qty <= 0`. Never let Spark parse the
offset with the session time zone: take the first 19 characters as local time and subtract the offset yourself.
```bash
make test-pyspark            # or: cd tracks/pyspark && PYTHONPATH=. ../../.venv/bin/pytest tests/test_parsing.py -x
```
<!-- /track -->

### Step 3.4 · M3: gold, the five reports
Remove `"Phase 3 M3 - gold models (the five reports)"`. Implement §4 (dims and facts) first, then §5 (the reports).
The tests compare every gold table with `contract/golden/fixture/`. The traps, report by report:
- `mart_revenue_daily`: revenue only for orders whose **latest** status is placed/shipped/delivered; `orders` is a
  *distinct* count (an order has several lines).
- `mart_cac_monthly`: new customers by the month of their first *valid* order; integer division rounded down; NULL
  CAC with no new customer; channels without spend appear with 0.
- `mart_mrr_monthly`: annual plans count `amount // 12`; a month is measured at its last day (or the run date).
- `mart_top_products_30d`: `run_date - 29 … run_date` is 30 days; ties by the lower SKU.
- `mart_cohort_retention`: only mature customers count; `retention_rate` is the only double in gold.

<!-- track:sql -->
**SQL · dbt:** the gold models have **enforced contracts** (`models/gold/_gold.yml`): dbt refuses to build a model
whose columns or types differ from `data-contract.yaml`, so cast explicitly (`::bigint`, `::date`). Use `{{ ref() }}`
for every upstream model so dbt builds them in order.
<!-- /track -->

<!-- track:pyspark -->
**Python · PySpark:** implement the functions in `gold.py` and `pipeline.enforce()`, which selects the contract's
columns in order and casts them to the contract's types (failing loudly on a missing column). Use integer
arithmetic for money (`F.floor(a / b)` returns a double: prefer `(a - a % b) / b` cast to `long`, or `F.expr("a div b")`).
<!-- /track -->

### Step 3.5 · M4: runner, publishing and the DAG
Remove `"Phase 3 M4 - runner and orchestration"`. Implement `warehouse.publish` (refuse unless the build's
`ops.build_info.publishable` is true, then `os.replace` the build onto the published file), `runner.build_and_publish`
(transform → dq → publish, or write an incident report and raise) and `runner.run_range` (ingest every day, then
build once as of the last day). Until Phase 4, `dq.run_check` is a stub that lets every check pass.

Then fill in the DAG skeleton `orchestration/dags/pipewright_daily.py` (every `TODO(M4)`): 06:00 IST as a UTC cron,
the business date = the IST day before, `max_active_runs=1`, `check_drops` retried every 15 minutes for 3 hours, one
`ingest_<source>` task per source, then `transform` → `dq` (no retries) → `publish`. Test it with
`make setup-airflow && make test-orchestration`.

### Step 3.6 · The full run and parity
```bash
make source                               # terminal 1
make run TRACK=sql                        # terminal 2 (or TRACK=pyspark): every day, 2026-06-01 .. 2026-08-31
.venv/bin/python scripts/parity.py compare warehouse/pipewright.duckdb contract/golden/v1.0
make parity                               # both tracks on a fresh copy of the source, against golden and each other
```
`PARITY OK` means every gold table equals the reference. If one table differs, `parity.py` shows what: go back to
the spec section of that table, write a failing test on a tiny input, fix, repeat.

## 4. AI-assist prompts

### Prompt 1: Implement one function from its TODO and the spec
```text
Here is a function stub with its TODO hint: <paste the stub>. Here is the rule it implements, from the spec:
<paste the transform-spec.md section>. Here is the failing test: <paste the test>. Write the implementation using
only <the standard library | DuckDB SQL in a dbt model | PySpark column expressions, no UDFs>. Explain each line
that handles an edge case (time zones, duplicates, nulls, empty input).
```
**Verify the output by:** running the milestone's tests (never only the one you pasted), reading every line you did
not write yourself, and checking the edge cases against the spec, not against the test: tests show examples, the
spec defines the rule. Note in `AI_LOG.md` what you changed.

### Prompt 2: Explain a parity difference
```text
My <SQL | PySpark> gold table <name> differs from the golden file. parity.py says: <paste the DIFF lines>. Here is
my model: <paste it>. Here is the spec section: <paste>. List the three most likely causes, and for each a 5-row
input that would prove or disprove it. Do not rewrite the model yet.
```
**Verify the output by:** building each suggested 5-row input (as a unit test or a small DuckDB query), confirming the
cause before changing code, and checking that the fix keeps every other table at `MATCH`.

### Prompt 3: Review my ingestion for idempotency and safety
```text
Here is my ingestion code: <paste ingest.py, lake.py, http.py>. Review it for: (1) what happens if the process is
killed at each line that writes, (2) whether re-running a day can duplicate or lose records, (3) retries that could
hammer the API or retry a request that will never succeed, (4) any path by which an email or phone number could be
written or logged. Answer with line references.
```
**Verify the output by:** turning each real finding into a test (kill = raise an exception in a fake writer; re-run =
call twice and compare the lake), and rejecting "findings" you cannot reproduce. Never paste real tokens or rows.

## 5. Deliverables
- [ ] All four Phase 3 groups removed from every `PENDING` list; `make test-pipewright`, `make test-<track>` and `make test-orchestration` green.
- [ ] `make lint` green.
- [ ] `make run TRACK=<your track>` publishes `warehouse/pipewright.duckdb` for 2026-08-31.
- [ ] `parity.py compare` against `contract/golden/v1.0` prints `PARITY OK` (and `make parity` if you have Java).
- [ ] Small commits per green step, with Conventional Commit messages.
- [ ] `AI_LOG.md` entries for this phase, including one AI-written piece of code you had to fix.

## 6. Self-check quiz

### Q1. An order appears in the lake on 1 June (placed) and on 4 June (cancelled). What does silver keep?
- [ ] Both rows, so the history is visible
- [x] One row: the latest version by `updated_at` (then `dt`), with status `cancelled`
- [ ] The first row, because it has the original amount
- [ ] Neither: changed orders are suspicious
> **Answer:** Bronze keeps both deliveries (immutable history); silver keeps the latest state per `order_id` (§3.1). Keeping both would double-count the order's lines in revenue.

### Q2. Why must `HttpClient` never retry a 401?
- [ ] 401 responses are slow
- [x] A wrong or missing token will not fix itself; retrying wastes time and can lock the account, so it must fail fast with a clear error
- [ ] The API forbids retries
- [ ] Retries are only for GET requests
> **Answer:** Retry what is transient (429, 5xx, timeouts), fail fast on what is not (401, 404, 400). Honour `Retry-After` when the server tells you how long to wait.

### Q3. `2026-06-01T19:00:00Z`: which business date is it?
- [ ] 2026-06-01
- [x] 2026-06-02 (19:00 UTC + 5:30 = 00:30 IST the next day)
- [ ] It depends on the server
- [ ] 2026-05-31
> **Answer:** `ist_date(ts) = date(ts + 5h30)`. Grouping by the UTC date puts every order between 00:00 and 05:30 IST on the wrong day; the fixture contains such orders on purpose.

### Q4. Why does `line_number` keep the position in the *original* items array even when a line with `qty <= 0` is dropped?
- [ ] To save a column
- [x] So the key `(order_id, line_number)` of every kept line is stable: dropping or fixing another line never renumbers it
- [ ] Because dbt requires sequential numbers
- [ ] It does not matter
> **Answer:** Keys that shift when unrelated data changes break incremental updates, joins with downstream systems and parity between tracks.

### Q5. Why is the warehouse published with `os.replace` instead of writing the tables directly into it?
- [ ] It is faster
- [ ] DuckDB cannot overwrite tables
- [x] The rename is atomic: the dashboard sees the complete old build or the complete new build, never a half-written one
- [ ] To keep a backup automatically
> **Answer:** Write to a build file, check it, then swap. A crash or a failed check before the swap leaves the published file untouched.

### Q6. Your PySpark CAC uses `F.floor(spend / customers)` and one row differs from golden by 1 paisa. Why?
- [ ] Spark has a bug
- [x] `/` produces a double; for large values a double cannot represent every integer quotient exactly, so flooring it can be off by one; integer division avoids it
- [ ] The golden file is wrong
- [ ] Customers should be a double
> **Answer:** Money stays integer end to end. Use integer division (`div`) so both tracks round down exactly as the spec says.

## 7. What you learned
- Test-first implementation with a pending list: red, green, commit.
- Ingestion that survives retries, crashes and re-runs, and keeps PII out.
- Exact parsing: offsets by hand, IST business dates, rupee text to integer paise.
- Silver as "latest version per key" and "one row per event", with window functions in SQL or Spark.
- Gold tables with enforced contracts, five reports with their traps, and an atomic publish.
- Parity against golden tables as the definition of "done".

## 8. Catch-up git commands
```bash
git fetch upstream --tags
git switch -c my-phase-4 phase-3-end
git diff phase-2-end phase-3-end --stat
git diff phase-2-end phase-3-end -- tracks/sql        # or tracks/pyspark, pipewright
```
