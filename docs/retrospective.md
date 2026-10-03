# Retrospective: Pipewright v1.0 → v1.1

Scope: phases 0-7, June-September 2026 data, both tracks. Every point cites evidence in this repository.

## By the numbers
| What | Value | Evidence |
|---|---|---|
| Data | 18,314 orders, 6,647 customers, 4 months, 5 sources | `contract/source-manifest-1.1.json` |
| Full run, all history | about 25 s (SQL · dbt), 40 s (PySpark, local mode) | `scripts/parity-run.sh 1.1` |
| Gold tables equal across tracks and golden files | 9 / 9 | `contract/golden/v1.1/fingerprints.json` |
| Checks on a build | 107 on SQL (15 contract + 92 dbt), 15 on PySpark | `warehouse/reports/dq-<date>.json` |
| Unit tests | package 83 (96 % coverage), SQL 23, PySpark 38 (99.6 %), dashboard 16, DAG 6 | `scripts/check-all.sh` |
| Incremental = full refresh | 9 / 9 tables, both tracks | `make incremental` |
| Docker | 5 images, all non-root (uid 10001), about 5 GB RAM with everything up | `scripts/docker-check.sh` |

## What went well
1. **Writing the spec before the code.** `contract/transform-spec.md` settled the arguments early (IST days, latest
   status wins, paise, the CAC denominator). Both tracks were written against it, and the parity check found the
   disagreements the spec had left open instead of production finding them.
2. **The publish gate.** On 2 September a new channel stopped the run and the dashboard kept the last good day,
   clearly dated (`docs/postmortem-2026-09.md`). The 1.0 pipeline on 1.1 data still refuses to publish
   (`make backcompat`, check C).
3. **Golden files plus two implementations.** A bug has to be made twice, the same way, to get through. Example: a
   CAC join that multiplied spend rows went red against the golden `mart_cac_monthly` before it could reach a
   report (`AI_LOG.md`, 2026-08-11).
4. **One CLI with exit codes.** Airflow, Docker, CI and the drill all call `pipewright`; a missing drop (exit 3) is
   retried, a blocking DQ failure (exit 1) is not. The DAG stayed small and testable without Airflow running a task.

## What did not
1. **No alert in v1.0.** The gate worked; nobody heard it. The CEO found the incident at 09:00. Fixed in v1.1
   (`pipewright monitor`, `pipewright_watch`), but NFR-1 (freshness) had no way to be noticed in v1.0 other than
   reading the run log.
2. **A fixed CSV header in PySpark.** A fifth column in the ads export broke the PySpark track (malformed CSV, exit 6)
   while dbt read it by name. Fixed by grouping files by header; tested with 4- and 5-column files.
3. **The incremental edge cases took longer than the incremental code.** The late-arriving product (category
   `unknown` never rebuilt) was found only because `make incremental` compared against a full refresh.
4. **Spark in a hardened container.** With a read-only root file system Spark could not load its native snappy
   library; it needs an executable `/tmp` (`tmpfs /tmp:exec` in `docker-compose.yml`). An hour lost, now a comment
   and a smoke check (`AI_LOG.md`, 2026-08-28).

## What I would change
- Put alerting and a freshness SLA in the first release, next to the gate.
- Ask upstream teams for a contract PR before a launch (now in the runbook).
- Write the incremental check before the incremental model, as with the parity check.

## AI: what helped, what misled
Saved time:
1. Reviewing the API client: three real findings, including retries on a 401 (`AI_LOG.md`, 2026-08-14).
2. Edge cases for the incremental merge: one of three was a real bug (2026-09-08).
3. Drafting data tests: 12 of 14 kept after running them on correct data (2026-08-18).

Wrong, and how it was caught:
1. Timestamp parsing that silently used the session time zone (off by 5:30): caught by tests with `Z` and `+05:30`
   inputs run under `TZ=UTC` (2026-08-08).
2. A plausible CAC query whose join multiplied spend: caught by the golden fixture (2026-08-11).
3. A post-mortem draft that invented an "08:15 Slack message": caught by checking every timestamp against
   `ops.run_log` and the alert log (2026-09-23).

Rule I keep: **an AI answer is a hypothesis until a test, a golden file or a log line agrees with it.**
