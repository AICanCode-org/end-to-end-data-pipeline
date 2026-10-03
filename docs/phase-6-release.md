---
phase: 6
slug: phase-6-release
title: "v1.0: the scheduled pipeline, the dashboard, the AI explainer, Docker"
duration: 6 h
start_tag: phase-5-end
end_tag: phase-6-end
release: v1.0.0
---
# Phase 6 · v1.0: the scheduled pipeline, the dashboard, the AI explainer, Docker

## 1. Why it matters
Until now the pipeline runs when you type a command. A data product runs **every morning without you**: Airflow
starts it at 06:00 IST, waits for late files, retries what is flaky, never retries a failed check, and the CEO opens a
dashboard that says which day it shows. When a check blocks the publish, the on-call engineer gets an incident note
that says what failed and what to try first. In this phase you build the Streamlit dashboard, the optional AI incident
explainer with guardrails and an offline fallback, the Docker images and Compose stack (non-root, read-only, under
6 GB), an acceptance check that runs all of it in CI, a runbook, and you release **v1.0.0**.

## 2. Objectives
By the end of this phase you can:
- Build a dashboard that reads only the published warehouse, shows its business date and the five reports, and is tested in CI.
- Add an AI feature safely: only aggregate facts go out, the answer is checked by guardrails, and every failure falls back to a deterministic template.
- Package every component in small, non-root Docker images with read-only file systems, and wire them with Compose profiles.
- Run the Airflow DAG end to end in a container, on Postgres, and read its logs.
- Prove the release with one acceptance script (images, pipelines in containers, parity, smoke tests, AI provider down, no secrets in logs).
- Write a runbook and release v1.0.0 with a CHANGELOG.

## 3. Step-by-step instructions

### Step 6.1 · The dashboard
Remove `"Phase 6 - dashboard"` from the `PENDING` lists. The dashboard (`dashboard/app.py` + `dashboard/data.py`) has
one tab per report plus a *Data health* tab. Rules:
- read `warehouse/pipewright.duckdb` **read-only**, never the build file;
- show "Data as of <business date>" and the age in days at the top; a stale dashboard must say so;
- format money from paise as ₹ with Indian digit grouping (₹3,98,25,047), computed in Python, not in SQL strings;
- keep queries in `data.py` (testable without Streamlit); `app.py` only lays out.
```bash
make install-dashboard && make test-dashboard
make dashboard                       # http://127.0.0.1:18142 on the warehouse from your last `make run`
```
The tests use Streamlit's `AppTest` to render every tab against a small warehouse built from the fixture.

### Step 6.2 · The AI incident explainer (optional at runtime, required in tests)
Remove `"Phase 6 - AI incident explainer"`. Implement `pipewright/explainer.py`'s `validate` and `explain`:
1. **Facts, not data:** `facts_from(report)` turns the failed checks into numbered facts (`[1] status_accepted
   failed: 41 rows outside the enum, values: lost`). Only these facts are sent: no rows, no customer ids.
2. **Provider:** `AI_PROVIDER=template` (default) never calls anything. `AI_PROVIDER=openai` calls any
   OpenAI-compatible endpoint (`AI_BASE_URL`, `AI_MODEL`, optional `AI_API_KEY`): Ollama locally
   (`http://localhost:11434/v1`), or a free tier. No paid key is ever required.
3. **Guardrails (`validate`):** the answer must cite at least one existing fact `[n]`, contain no number that is not
   in the facts, no URL, no data-changing SQL (`DROP`, `DELETE`, `UPDATE`, ...), and be short. Otherwise → reject.
4. **Fallback:** a timeout, an HTTP error, a missing setting or a rejected answer → the deterministic template, and
   the note says why (`Source: template (provider unavailable: ...)`). The key is never printed or logged.
```bash
.venv/bin/pipewright explain --report contract/fixture/incident-report.json
AI_PROVIDER=openai AI_BASE_URL=http://10.255.255.1:9/v1 AI_MODEL=x AI_TIMEOUT=2 \
  .venv/bin/pipewright explain --report contract/fixture/incident-report.json     # unreachable → template, says why
```
The tests use a local fake LLM (`scripts/fake_llm.py`, modes `good`, `numbers`, `url`, `slow`) that answers well,
invents a number, adds a URL or hangs.

### Step 6.3 · Docker images and Compose
Read the Dockerfiles (`source/`, `tracks/sql/`, `tracks/pyspark/`, `dashboard/`, `orchestration/`) and
`docker-compose.yml`. What to look for:
- `python:3.12-slim` bases, dependencies installed in one layer **before** the code is copied (fast rebuilds);
- a non-root user (`uid 10001`), `read_only: true`, `tmpfs: /tmp`, `cap_drop: [ALL]`, `no-new-privileges`;
- ports bound to `127.0.0.1` only; HTTPS package mirrors (`DEBIAN_MIRROR` build arg) for networks that block HTTP;
- profiles: default (source + dashboard), `pipeline` (the two pipelines + parity), `airflow` (Postgres + Airflow),
  `ai-test` (the fake LLM); memory limits (Airflow 3 GB, PySpark 2 GB): about 5 GB with everything up.
```bash
cp .env.example .env                     # every variable is optional; defaults are offline
docker compose up -d --build             # source :18140 + dashboard :18142
docker compose --profile pipeline run --rm pipeline-sql run --start 2026-06-01 --end 2026-08-31
docker compose --profile pipeline run --rm pipeline-pyspark run --start 2026-06-01 --end 2026-08-31
docker compose --profile pipeline run --rm parity
```

<!-- track:sql -->
**SQL · dbt track:** the `pipeline-sql` image holds the `pipewright` CLI with dbt-duckdb and your dbt project; dbt
writes `target/` and logs to a writable volume path (the root file system is read-only), and anonymous usage stats
are off (`DBT_SEND_ANONYMOUS_USAGE_STATS=false`). The Airflow image runs the same CLI from its own virtualenv.
<!-- /track -->

<!-- track:pyspark -->
**Python · PySpark track:** the `pipeline-pyspark` image adds a headless JRE. With a read-only root file system Spark
needs a writable scratch dir that allows executing files (`/tmp` as a tmpfs mounted with `exec`: Spark unpacks
native libraries there), and in a container its driver must
bind to `127.0.0.1` (`SPARK_DRIVER_BIND_ADDRESS`). Memory: `SPARK_DRIVER_MEMORY=1g` inside a 2 GB container limit.
<!-- /track -->

### Step 6.4 · Airflow, scheduled
```bash
docker compose --profile airflow up -d          # Postgres + Airflow standalone, UI on http://127.0.0.1:18144
                                                 # (local only: no login, the port binds to 127.0.0.1)
docker compose exec airflow airflow dags test pipewright_daily 2026-09-01T00:30:00+00:00
```
The DAG run for 06:00 IST on 1 September builds business date 31 August. In the UI, open the run: `check_drops`,
five `ingest_*` tasks in parallel, `transform`, `dq`, `publish`. Kill a task, read its log, clear it.

### Step 6.5 · The acceptance check, locally and in CI
```bash
AIRFLOW=1 scripts/docker-check.sh
```
It builds every image, runs both pipelines in containers and compares them, smoke-tests the source, the dashboard
and the explainer three ways (template; AI provider unreachable with a fake key that must never appear in any log;
the fake LLM), checks non-root and read-only, runs the DAG, then removes everything. Add it to CI as the `docker` job,
plus a `dashboard` job (`git show phase-6-end:.github/workflows/ci.yml`).

### Step 6.6 · Runbook, CHANGELOG, release
Write `docs/runbook.md` (how to run, how to re-run a day, what each exit code means, what to do when a check blocks the
publish, how to read an incident note), move the CHANGELOG's *Unreleased* items under `1.0.0`, then:
```bash
git tag -a v1.0.0 -m "Pipewright v1.0.0"   # in your fork, after CI is green on main
git push origin v1.0.0
```

## 4. AI-assist prompts

### Prompt 1: Red-team my AI guardrails
```text
My pipeline sends numbered facts from a data-quality report to an LLM and accepts its incident note only if it cites
existing facts [n], contains no number absent from the facts, no URL and no data-changing SQL, and is under N words.
Here is my validate() function: <paste>. Write 10 model answers that should be REJECTED but might pass my checks
(e.g. numbers spelled out, a URL without http, SQL in a code block), and 3 good answers that might be wrongly rejected.
```
**Verify the output by:** turning each case into a test in `pipewright/tests/test_explainer.py`, fixing `validate`
only where a case really is harmful, and keeping the fallback path the default: when in doubt, use the template.

### Prompt 2: Review my Dockerfile
```text
Review this Dockerfile and compose service for image size, build caching, security (user, file system, capabilities,
secrets) and reproducibility (pinned versions, mirrors): <paste>. For each finding give the line, the risk and a fix.
Assume the stack runs on a developer laptop with 8 GB RAM and in GitHub Actions.
```
**Verify the output by:** rebuilding and comparing `docker image ls` sizes, running `docker run --rm --entrypoint id
<image> -u` (must not be 0), and re-running `scripts/docker-check.sh`; reject suggestions that need a paid registry.

### Prompt 3: Draft the runbook from my code
```text
Here are my CLI's commands and exit codes: <paste pipewright --help and the exit-code table>, and my DAG: <paste>.
Draft an on-call runbook: daily checks, re-running a day, a late file, a blocked publish, a failed ingestion, rotating
the AI key, where logs are. Use only commands that exist in what I pasted.
```
**Verify the output by:** running every command in the runbook against your local stack, deleting anything that does
not exist, and asking a classmate to follow one section without help.

## 5. Deliverables
- [ ] Both Phase 6 groups removed from every `PENDING` list; dashboard and explainer tests green.
- [ ] The dashboard shows the five reports and "Data as of" from your warehouse.
- [ ] `pipewright explain` works with the template, and falls back with a reason when the provider is unreachable.
- [ ] `AIRFLOW=1 scripts/docker-check.sh` passes locally; the `docker` and `dashboard` CI jobs are green.
- [ ] A successful DAG run in Airflow (screenshot of the grid view in your PR).
- [ ] `docs/runbook.md`, CHANGELOG `1.0.0`, tag `v1.0.0`.
- [ ] `AI_LOG.md` entries, including a guardrail case the AI found.

## 6. Self-check quiz

### Q1. Why does the explainer send numbered facts instead of the failing rows?
- [ ] Rows are too long for the model
- [x] Facts are aggregates (check ids, counts, safe codes) that cannot leak personal data, and numbering them lets the guardrails check every claim cites a real fact
- [ ] The model cannot read JSON
- [ ] It is cheaper
> **Answer:** Data minimisation plus verifiability: the model only sees what it needs, and its answer can be checked against what it saw.

### Q2. The AI provider times out. What does the on-call engineer get?
- [ ] An error and no note
- [ ] A retry loop until the provider answers
- [x] The deterministic template note, which says it is the template and why (provider unavailable)
- [ ] The last AI note from yesterday
> **Answer:** The AI is an optional improvement; the template is always available offline and is what the tests and CI rely on.

### Q3. Why does the dashboard read the published file read-only and never the build file?
- [ ] The build file is encrypted
- [x] The build file may be half-built or failed; the published file is complete, checked and swapped in atomically
- [ ] Streamlit cannot open two files
- [ ] For speed
> **Answer:** That is the publish gate from Phase 2, seen from the reader's side.

### Q4. Which container setting limits damage if the dashboard is compromised?
- [ ] `restart: always`
- [x] A non-root user, `read_only: true`, `cap_drop: [ALL]`, `no-new-privileges`, and a port bound to 127.0.0.1
- [ ] A bigger memory limit
- [ ] The `latest` tag
> **Answer:** Defence in depth: an attacker who gets code execution cannot write to the image, gain privileges or reach the container from another machine.

### Q5. Why does `docker-check.sh` grep every log for a fake API key?
- [ ] To test grep
- [x] To prove the key is never printed (in errors, debug output or tracebacks), even when the provider fails
- [ ] Because the key is needed in logs for debugging
- [ ] To count requests
> **Answer:** A secret that reaches a log reaches every system the logs are shipped to. Test for it like any other requirement.

### Q6. The DAG's `dq` task has `retries=0`. Why?
- [ ] Retries are expensive
- [x] A failed check is a fact about the data, not a flaky failure; retrying gives the same result and delays the alert
- [ ] Airflow cannot retry Bash tasks
- [ ] The check is too slow
> **Answer:** Retry transient failures (network, late files); fail fast on deterministic ones and page a human.

## 7. What you learned
- A dashboard that only reads published, checked data and states its freshness.
- A safe optional AI feature: data minimisation, numbered facts, guardrails, an offline fallback, no paid keys.
- Small, hardened Docker images and Compose profiles within a RAM budget.
- Airflow running the CLI on a schedule, with retries where they make sense.
- One acceptance script for the release, locally and in CI; a runbook and a tagged release.

## 8. Catch-up git commands
```bash
git fetch upstream --tags
git switch -c my-phase-7 phase-6-end         # = v1.0.0
git diff phase-5-end phase-6-end --stat
```
