---
phase: 5
slug: phase-5-ci
title: "CI: dbt build and tests on the fixture, cross-track parity"
duration: 3 h
start_tag: phase-4-end
end_tag: phase-5-end
---
# Phase 5 · CI: dbt build and tests on the fixture, cross-track parity

## 1. Why it matters
A pipeline's code changes far more often than its sources: a "harmless" refactor of a join, a dbt upgrade, a new
column. Each can move a number the CEO reads tomorrow. Continuous integration runs every check on every pull
request, on a clean machine, before the change is merged: lint, the unit tests, **`dbt build` with all its tests on
the committed fixture**, the DAG tests, and the strongest check of all: both tracks run the full history and must
produce the golden tables and each other's tables, row by row. In this phase you replace the starter's simple CI
with that pipeline, learn every GitHub Actions concept it uses, and make one check, `ci-ok`, the gate for merging.

## 2. Objectives
By the end of this phase you can:
- Read and write a GitHub Actions workflow: triggers, jobs, `needs`, `if`, matrix, caching, outputs, artifacts, permissions.
- Run each part only when its files changed (path filters) without blocking pull requests (an aggregator job).
- Check in CI that the committed fixture and manifest are exactly what the seeded generator produces.
- Run `dbt build` (models, data tests, unit tests, contracts) on the fixture, and the PySpark suite with Java, in CI.
- Run the full-history parity of both tracks in CI and publish its result on the run page.
- Keep CI's tool versions identical to your laptop's, and protect `main` with required checks.

## 3. Step-by-step instructions

### Step 5.1 · Read the starter CI and the explainer
Open `.github/workflows/ci.yml` (the starter: one matrix job per part, `make install/lint/test-<part>`) next to
`docs/github-actions-explained.md` §1-§6. Answer for yourself: which event starts it, how many jobs run, where the
Python version comes from, why `fail-fast: false`.

### Step 5.2 · Design the full pipeline
Sketch the jobs and their dependencies before writing YAML:
```text
changes ─┬─ contract        (fixture + manifest reproducible, shellcheck, CMS export)
         ├─ pipewright      (lint + tests + coverage gate)
         ├─ sql ──────┐     (sqlfluff + dbt build on the fixture + expected tables)
         ├─ pyspark ──┼─ parity   (full history, both tracks = golden, sql = pyspark)
         ├─ orchestration   (DAG tests, Airflow constraints)
         └──────────────┴─ ci-ok   (fails if anything failed; skipped is fine)
```
Keep the job names `pipewright · lint + unit`, `sql · lint + unit`, `pyspark · lint + unit`: branch protection
already requires them.

### Step 5.3 · Write the workflow
Replace `.github/workflows/ci.yml` step by step (the reference is `git show phase-5-end:.github/workflows/ci.yml`;
write yours first):
1. `changes` with `dorny/paths-filter@v3` and an `all` output that is true on pushes or when shared files changed
   (`github-actions-explained.md` §7).
2. `contract`: regenerate the 3-day fixture into `$RUNNER_TEMP` and `diff -r` it with `contract/fixture/source`;
   regenerate the full source with `--check contract/source-manifest-*.json`; `shellcheck scripts/*.sh`;
   `python3 scripts/build-cms.py --check`.
3. One job per part with `actions/setup-python@v5` (Python 3.12, `cache: pip`), `actions/setup-java@v4` (Temurin 21)
   for PySpark, and the same `make` targets you run locally.
4. `orchestration`: install Airflow **with its constraints file** and run `make test-orchestration`.
5. `parity`: both tracks + Java, then `scripts/parity-run.sh <version>`; append the output to `$GITHUB_STEP_SUMMARY`.
6. `ci-ok`: `if: always()`, `needs:` everything, fail on `failure` or `cancelled`.

Lint it before pushing: `actionlint` (<https://github.com/rhysd/actionlint>) catches typos in expressions and
runs shellcheck on every `run:` block.

<!-- track:sql -->
**SQL · dbt track:** the `sql` job is where dbt runs in CI: `make test-sql` builds the fixture with `dbt build` (seeds,
models, contracts, data tests, unit tests) through the same runner as production, then compares silver and gold with
the expected files. Add `--warn-error-options` only if you want warnings to fail too (we allow `warn` severity). The
`target/` folder is never committed: CI starts clean, so a model that only works because of a stale artifact fails here.
<!-- /track -->

<!-- track:pyspark -->
**Python · PySpark track:** the `pyspark` job needs `actions/setup-java@v4` before the tests, or Spark fails with
"JAVA_HOME is not set". Spark is slow to start: one session per test run (the `spark` fixture), `local[2]`, and a
`timeout-minutes` on the job so a hung JVM cannot burn six hours of free minutes.
<!-- /track -->

### Step 5.4 · Dependabot
Add `.github/dependabot.yml` with weekly updates for `github-actions` and `pip` (`/pipewright`, `/tracks/sql`,
`/tracks/pyspark`). Every Dependabot PR runs the full CI, so an upgrade that moves a number is caught by parity.
Airflow is bumped by hand together with its constraints URL.

### Step 5.5 · Push, watch, protect
1. Push a branch and open a PR: watch the jobs start, the skipped ones, and `ci-ok`.
2. Make it fail on purpose: change one rule (e.g. `>= 0` to `> 0` in a model), push, and read the `parity` summary.
   Revert.
3. In **your fork**: Settings → Branches → add a rule for `main`: require a pull request, require status checks
   (`ci-ok`, or the three `· lint + unit` checks), require branches to be up to date, no force pushes.

## 4. AI-assist prompts

### Prompt 1: Explain a workflow line by line
```text
Here is a GitHub Actions workflow: <paste ci.yml>. Explain it to a data engineer who has never used Actions: what
triggers it, which jobs run in which order and why, what each `if:` decides, what is cached and with which key.
Then list three ways it could silently stop checking something (e.g. a job that is always skipped).
```
**Verify the output by:** checking each claim against `docs/github-actions-explained.md` and the official docs, and
testing the "silently stops checking" scenarios with a PR that touches only `docs/` and one that touches
`tracks/sql/`.

### Prompt 2: Debug a red CI job
```text
This CI job fails but the same command passes on my laptop. Job log (last 60 lines): <paste>. Workflow job: <paste
the job>. My local versions: <python --version, java -version, pip freeze | grep -E "dbt|pyspark|duckdb">. What
differs between the two environments, and which single command reproduces CI locally?
```
**Verify the output by:** reproducing the failure locally with a clean checkout (`git clone` into a temp folder, then
the job's commands) before changing anything, and keeping versions pinned in both places, never "latest".

### Prompt 3: Review my CI for security and cost
```text
Review this workflow for least privilege, secret exposure, supply-chain risk and wasted minutes: <paste ci.yml>.
For each finding: the line, the risk, the fix. Do not suggest paid services.
```
**Verify the output by:** checking `permissions:` is `contents: read` by default, that no secret is needed at all,
that every job has `timeout-minutes`, and running `actionlint` after the changes.

## 5. Deliverables
- [ ] A full `.github/workflows/ci.yml`: changes, contract, pipewright, sql, pyspark, orchestration, parity, ci-ok.
- [ ] `actionlint` passes with no findings.
- [ ] A green run on `main` with the parity summary on the run page.
- [ ] A PR that failed parity on purpose (link it), then was fixed.
- [ ] `.github/dependabot.yml`.
- [ ] Branch protection on your fork's `main`.
- [ ] `AI_LOG.md` entry.

## 6. Self-check quiz

### Q1. Why not use `on: push: paths:` to skip CI when only docs change?
- [ ] It is not supported for pull requests
- [x] A skipped workflow never reports its checks, so a required check stays "pending" and blocks the PR forever; per-job `if:` with an aggregator job avoids it
- [ ] It is slower
- [ ] Paths only work with tags
> **Answer:** Always run the workflow, decide per job, and require `ci-ok`, which treats skipped jobs as fine.

### Q2. What does the `contract` job's `diff -r` of a regenerated fixture prove?
- [ ] The fixture is small
- [x] The committed fixture is exactly what the seeded generator produces, so nobody hand-edited it to make a test pass
- [ ] The generator is fast
- [ ] The API stub works
> **Answer:** Test data is code. If it can drift from its generator, a failing test can be "fixed" by editing the data.

### Q3. Why does CI install Airflow with a constraints file?
- [ ] It is faster
- [x] Airflow has hundreds of transitive dependencies; the constraints pin all of them to versions tested together, the same as locally and in the Docker image
- [ ] Airflow refuses to install otherwise
- [ ] To install the Postgres provider
> **Answer:** Without pins, a new release of some dependency can break the DAG tests overnight with no change on your side.

### Q4. The `parity` job runs the full history through both tracks. Why is the fixture not enough?
- [ ] The fixture has no orders
- [x] Three days cannot exercise month boundaries, 30-day windows, cohort maturity or late status changes across weeks; the full history can, and the two tracks must agree on all of it
- [ ] dbt needs at least 30 days
- [ ] The golden files only cover the full history
> **Answer:** The fixture is for fast, hand-checkable unit tests; parity on three months is the end-to-end proof.

### Q5. CI is red, your laptop is green. What is the first thing to compare?
- [ ] The color of the terminal
- [x] Tool versions (Python, Java, dbt, PySpark, DuckDB) and untracked files on your laptop (a `target/`, a `lake/`, a local seed)
- [ ] The GitHub status page
- [ ] The number of CPU cores
> **Answer:** CI starts from a clean checkout with pinned versions. Most "works on my machine" bugs are a version or a file that exists only on your machine.

### Q6. Which permission does Pipewright's CI need?
- [ ] `contents: write` to commit results
- [ ] `packages: write`
- [x] `contents: read` (and `pull-requests: read` for the path filter); nothing else, no secrets
- [ ] An admin token
> **Answer:** Least privilege: CI reads code and reports checks. The pipeline's production runs are Airflow's job, not CI's.

## 7. What you learned
- GitHub Actions: triggers, jobs, `needs`, `if`, matrix, caching, outputs, step summaries, permissions.
- Path filters with an aggregator job, and why required checks must always report.
- dbt build and tests on a fixture in CI; Spark with Java in CI; Airflow with constraints.
- Reproducible test data checked in CI, and full-history parity of two implementations as the merge gate.
- Dependabot and actionlint; branch protection with required checks.

## 8. Catch-up git commands
```bash
git fetch upstream --tags
git switch -c my-phase-6 phase-5-end
git diff phase-4-end phase-5-end -- .github
```
