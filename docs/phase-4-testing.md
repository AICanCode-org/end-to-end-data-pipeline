---
phase: 4
slug: phase-4-testing
title: "Data tests and data-quality checks: the publish gate"
duration: 5 h
start_tag: phase-3-end
end_tag: phase-4-end
---
# Phase 4 · Data tests and data-quality checks: the publish gate

## 1. Why it matters
Unit tests prove your code does what you meant on the inputs you thought of. Data does not care what you thought
of: tomorrow the source sends a status nobody announced, a catalogue drop forgets a SKU, a join fans out after a
schema change. A pipeline needs a second kind of test that runs **on every build, on the real data, before anyone
sees it**: data tests and data-quality checks. In this phase you switch on the checks declared in the data contract,
write the dbt data tests and unit tests (SQL track), add a coverage gate, and see the publish gate do its job: a
build that fails a blocking check is never published, and an incident report explains why.

## 2. Objectives
By the end of this phase you can:
- Explain the difference between unit tests (code, small inputs), data tests (properties of the real tables) and data-quality checks (the gate before publishing).
- Implement the contract's check kinds: `row_count_min/max`, `accepted_values`, `relationship`, `max_share`, `expression`, `reconcile`, with severities.
- Record only aggregates in the report (never a row), and mask unexpected values that might be personal data.
- Write dbt generic, singular and unit tests (SQL track) or rely on the shared checks plus a coverage gate (PySpark track).
- Break the data on purpose and watch the gate keep the last good build.

## 3. Step-by-step instructions

### Step 4.1 · Three kinds of tests
Write one sentence each in your notes, then read on:
- **Unit test:** a function or model on a tiny, hand-made input (`test_parsing.py`, a dbt unit test). Runs in CI.
- **Data test:** a property every row or table must have (`order_id` is unique, every order's customer exists, revenue
  reconciles to the lines). Runs on every build, on the real data.
- **Data-quality check with a severity:** a data test wired to a decision: `error` blocks publishing, `warn` is
  published and reported. Lives in `contract/data-contract.yaml` so both tracks share it.

### Step 4.2 · Implement the contract's checks (shared)
Remove `"Phase 4 - data-quality checks"` from every `PENDING` list. Implement `pipewright/dq.py`'s `run_check`: one
branch per kind. Each runs **one aggregate query** against the build and returns a `CheckResult` with `observed`,
`threshold` and `status`:
- `accepted_values`: count rows outside the list (`@order_status` resolves to the contract's enum); report up to 5
  distinct unexpected values, masked unless they match `SAFE_VALUE` (a code is fine, an email is not).
- `relationship`: count child keys with no parent. `max_share`: the share of rows matching `where`.
- `reconcile`: two queries that must return the same number (revenue = revenue lines).
- A check that cannot run (bad SQL, unknown kind) is status `error`, **never** a pass.
```bash
make test-pipewright
.venv/bin/pipewright dq --date 2026-08-31 && cat warehouse/reports/dq-2026-08-31.json | head -40
```

### Step 4.3 · Data tests in your track
Remove `"Phase 4 - data tests"`.

<!-- track:sql -->
**SQL · dbt:** add tests until `test_dbt_tests.py` passes (at least 60 data tests and 3 unit tests, all green):
1. **Generic tests in YAML** (`models/staging/_staging.yml`, `models/gold/_gold.yml`): `unique`, `not_null`,
   `accepted_values`, `relationships` on every key and enum column (dbt 1.12 syntax:
   `data_tests: [{accepted_values: {arguments: {values: [...]}}}]`).
2. **Your own generic tests** in `tests/generic/`: `non_negative`, `positive`, `accepted_range`,
   `unique_combination` (a test is a query that returns the failing rows):
   ```sql
   {% test non_negative(model, column_name) %}
   select {{ column_name }} from {{ model }} where {{ column_name }} < 0
   {% endtest %}
   ```
3. **Singular tests** in `tests/singular/`: no order after the run date, gross = lines, revenue reconciles, cohorts
   add up, the parsing macros on known inputs.
4. **Unit tests** (`models/gold/_unit_tests.yml`): mock inputs → expected rows for the trickiest rules (CAC rounding
   and NULL, mature-only retention, annual MRR as 1/12).
```bash
cd tracks/sql && ../../.venv/bin/dbt test --profiles-dir . --select test_type:unit      # after a build
make test-sql
```
On this track `pipewright dq` also runs `dbt test` (data tests only) and merges the results into the same report,
so a failing dbt test blocks publishing like a contract check.
<!-- /track -->

<!-- track:pyspark -->
**Python · PySpark:** your data tests are the **shared contract checks**: they run on the warehouse whatever track
built it, so they test your Spark output exactly as they test the dbt output. Run them on a full build:
```bash
make run TRACK=pyspark && .venv/bin/pipewright dq --date 2026-08-31
```
Then turn on the **coverage gate** (Step 4.4) and add edge-case unit tests with tiny DataFrames for anything the
fixture does not cover well (an order exactly at 00:00 IST, a payment delivered twice on two days, an unknown
city). Spark tests are slow to start: share one `SparkSession` per test session (the `spark` fixture) and build
DataFrames with explicit schemas.
<!-- /track -->

### Step 4.4 · A coverage gate
Add `fail_under = 85` to `[tool.coverage.report]` in `pipewright/pyproject.toml` (and `tracks/pyspark/pyproject.toml`
on the PySpark track). `make test-pipewright` now fails if less than 85 % of the lines ran. Look at the report's
missing lines: an untested branch in ingestion or in a check is exactly where a 3 a.m. surprise hides. Coverage is a
floor, not a goal: 100 % covered code can still compute the wrong revenue; that is what the golden tables are for.

### Step 4.5 · Break it on purpose: the gate in action
Prove the gate works, end to end, on a copy of the data:
```bash
cp -r source-data /tmp/broken && sed -i 's/"status":"delivered"/"status":"lost"/' /tmp/broken/api/orders.jsonl
# restart the stub on /tmp/broken (python -m bazaarbox_source serve --data /tmp/broken --port 18140), then:
.venv/bin/pipewright run --date 2026-08-31; echo "exit $?"
ls warehouse/reports/ && .venv/bin/pipewright explain           # the incident note (template until Phase 6)
```
Expected: `status_accepted` fails with `observed` > 0 and the value `lost`, exit 1, nothing published, the dashboard's
warehouse still at the last good date. Restore the real source afterwards. Write down what you saw: Phase 7 opens
with the day this happens for real.

## 4. AI-assist prompts

### Prompt 1: Propose data tests from a schema
```text
Here is the schema and description of a gold table: <paste one model from _gold.yml or data-contract.yaml>, and the
rule that builds it: <paste the transform-spec.md section>. Propose data tests: which columns need unique / not_null /
accepted_values / relationships, which cross-table reconciliations prove nothing is lost or duplicated, and which
edge cases deserve a unit test with mock rows. Mark each as error or warn and justify the severity.
```
**Verify the output by:** running each proposed test on the fixture build and on the full build (a test that fails on
correct data is wrong, not strict), checking every `accepted_values` list against the contract's enums, and
discussing any severity you disagree with in your PR.

### Prompt 2: Write a dbt unit test or a PySpark edge-case test
```text
Rule: <paste one rule, e.g. CAC = spend // new customers, NULL with no new customer>. Write a <dbt unit test in YAML
(given: mock rows for each input model, expect: rows) | pytest test building tiny PySpark DataFrames with explicit
schemas> that covers: the normal case, the boundary, and the empty or NULL case. Use at most 5 input rows.
```
**Verify the output by:** computing the expected rows by hand before running the test, making the test fail once on
purpose (change the rule) to prove it can fail, and checking the mock rows' columns match the real model's.

### Prompt 3: Make a check report safe to share
```text
Here is a data-quality report my pipeline writes: <paste a dq-*.json with fake values>. It will be read by on-call
engineers and sent to an AI model for an incident summary. List every field that could carry personal or secret
data, and how to keep the report useful with aggregates only.
```
**Verify the output by:** checking each suggestion against `dq.py` (what is really written), testing the masking with
a value that looks like an email, and keeping the report useful: an incident note must still say *which* check failed
and *how much*.

## 5. Deliverables
- [ ] Both Phase 4 groups removed from every `PENDING` list; all suites green.
- [ ] `run_check` implements every check kind; a check that cannot run is an error.
- [ ] SQL track: ≥ 60 dbt data tests and ≥ 3 unit tests passing; PySpark track: edge-case tests added.
- [ ] Coverage gate at 85 % in the shared package (and the PySpark track).
- [ ] The break-it experiment: a screenshot or log of the blocked publish and the incident note, in your PR.
- [ ] `AI_LOG.md` entries, including a test the AI proposed that was wrong.

## 6. Self-check quiz

### Q1. What is the difference between a unit test and a data test?
- [ ] There is none
- [x] A unit test checks code on small hand-made inputs in CI; a data test checks a property of the real tables on every build
- [ ] Data tests are slower unit tests
- [ ] Unit tests are only for Python
> **Answer:** Code can be correct and the data still wrong (a new status, a missing SKU). You need both: unit tests before merging, data tests before publishing.

### Q2. A check fails because 41 orders have a status the contract does not know. What should happen?
- [ ] Map the new status to `placed` and continue
- [ ] Delete the 41 orders
- [x] Block the publish (severity error), keep the last good warehouse, write an incident report naming the unexpected value
- [ ] Publish with a warning
> **Answer:** An unknown status could be revenue or a loss; guessing silently changes the CEO's numbers. The contract decides, and changing it is a versioned decision (Phase 7).

### Q3. Why must a check that cannot run (e.g. a SQL error) be reported as `error`, not `pass`?
- [ ] To make the report longer
- [x] A check that silently passes when it is broken gives false confidence: the gate would be open exactly when it is blind
- [ ] Because DuckDB raises exceptions
- [ ] It should be skipped instead
> **Answer:** Fail closed. A broken smoke detector must beep, not stay quiet.

### Q4. Why does the report store "41 rows, values: lost" and never the 41 rows themselves?
- [ ] Rows are too big
- [x] The report is shared with on-call engineers and an AI model: aggregates and safe codes are enough to act on, rows could contain personal data
- [ ] JSON cannot store rows
- [ ] dbt does not return rows
> **Answer:** Design reports so they can be shared. Values that do not look like a code are masked for the same reason.

### Q5. What does `reconcile` between `mart_revenue_daily` and `fct_order_items` catch?
- [ ] Slow queries
- [x] A join or filter that drops or duplicates revenue between the facts and the report, even when every column looks valid
- [ ] Wrong column types
- [ ] Missing indexes
> **Answer:** Column tests cannot see a fan-out: every row is valid, there are just too many. Totals that must agree across layers can.

### Q6. Your coverage is 92 %. Does that mean the revenue is right?
- [ ] Yes, above 85 % is correct
- [x] No: coverage says which lines ran, not whether they compute the right number; the golden tables and reconciliation checks test correctness
- [ ] Only if it is 100 %
- [ ] Coverage does not apply to data pipelines
> **Answer:** Coverage finds untested code; it cannot find a wrong rule that is tested with a wrong expectation.

## 7. What you learned
- Unit tests, data tests and data-quality checks, and where each runs.
- Declarative checks in a shared contract, with severities that drive a publish decision.
- dbt generic, custom generic, singular and unit tests; or shared checks plus edge-case tests for Spark.
- Reports with aggregates only, masking anything that is not a safe code.
- A coverage gate as a floor, and a deliberate failure to prove the gate works.

## 8. Catch-up git commands
```bash
git fetch upstream --tags
git switch -c my-phase-5 phase-4-end
git diff phase-3-end phase-4-end --stat
git diff phase-3-end phase-4-end -- tracks/sql/tests tracks/sql/models
```
