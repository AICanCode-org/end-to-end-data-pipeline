# Runbook: Pipewright

For whoever is on call for BazaarBox's daily dashboard. Every command here exists; run them from the repository root
(locally with `.venv/bin/pipewright`, in Docker with `docker compose --profile pipeline run --rm pipeline-sql <command>`,
in Airflow from the task's log).

## 1. What runs, when
| When (IST) | What | Where |
|---|---|---|
| by 05:00 | partners drop payments, ad spend and the catalogue for D-1 | `drops/` |
| 06:00 | DAG `pipewright_daily`: `check_drops` → `ingest_*` ×5 → `transform` → `dq` → `publish` | Airflow (`docker compose --profile airflow up -d`, UI on 127.0.0.1:18144) |
| 06:30 | SLA: D-1 published | the dashboard shows "Data as of D-1" |
| 07:00 | the CEO's review | dashboard (127.0.0.1:18142) |

## 2. Daily check (2 minutes)
1. Dashboard header: "Data as of" = yesterday? If not, go to §4.
2. Airflow grid: the last `pipewright_daily` run is green.
3. `ls warehouse/reports/` shows `dq-<D>.json`; `incident-<D>.md` exists only when something blocked publishing.

## 3. Exit codes
| Code | Meaning | First action |
|---|---|---|
| 0 | done | none |
| 1 | a blocking data-quality check failed, or publish refused (the build is not checked/publishable) | §4 |
| 2 | bad command-line arguments, or `explain` found no report | read the message |
| 3 | a file drop is missing (`check-drops`, `ingest` of a file source) | §5; Airflow retries every 15 min for 3 h |
| 4 | the source API failed after all retries | §6 |
| 5 | a contract violation at ingestion (a drop file without a required column, unparseable lines) | §7 |
| 6 | the transformation failed (dbt or Spark error) | read the task log; reproduce with `pipewright transform --date D` |

## 4. A blocked publish (exit 1 at `dq`)
The dashboard still shows the last good day: that is intended. Then:
```bash
cat warehouse/reports/incident-<D>.md                    # what failed, the facts, likely causes, next steps
.venv/bin/pipewright explain --report warehouse/reports/dq-<D>.json    # regenerate the note (template, or AI if configured)
```
- `accepted_values` failed with a new code (e.g. a new status): **do not** map it silently. Ask the source owner what
  it means; if it is legitimate, change the contract (new version, spec, golden files, both tracks) via a PR.
- `relationship` / `row_count_min`: usually a late or partial source: check `lake/_manifests/<source>/<D>.json`
  (counts), re-ingest that day (`pipewright ingest --source <s> --date D`), re-run (`pipewright run --date D`).
- `reconcile`: a model drops or duplicates rows: reproduce on the fixture, fix with a test, deploy, re-run.
Tell the CEO which business date the dashboard shows and when you expect the fix.

## 5. A late file (exit 3)
`check_drops` retries every 15 minutes for up to 3 hours. If the partner confirms a longer delay, leave the dashboard on
D-2 (labelled) and, when the file lands, run the day by hand (or clear the Airflow task):
```bash
.venv/bin/pipewright check-drops --date D && .venv/bin/pipewright run --date D
```

## 6. The source API fails (exit 4)
`curl -s http://<source>/health`. 429/503 are retried with backoff; persistent failures are the source's incident.
A 401 means the token is wrong (`SOURCE_API_TOKEN`): it is never retried. Re-run the ingest task when it is back.

## 7. Re-running and backfilling
- Re-run one day: `pipewright run --date D` (idempotent: the day's partitions are replaced, never appended).
- Rebuild without re-ingesting: `pipewright transform --date D && pipewright dq --date D && pipewright publish`.
- A range: `pipewright run --start D1 --end D2` (ingests each day, builds once as of D2).
- Never run two at once: the DAG has `max_active_runs=1`; pause it while you work by hand.

## 8. Secrets and the AI explainer
- The only secret-like settings are `AI_API_KEY` (optional) and `AIRFLOW_DB_PASSWORD` (local). They live in `.env`
  (never committed) and are never printed by the pipeline. Rotate a key by changing `.env` and restarting the services.
- To switch the explainer off: `AI_PROVIDER=template` (the default). The pipeline never depends on it.

## 9. Where things are
| What | Where |
|---|---|
| raw data | `lake/raw/<source>/dt=<D>/`, bad rows in `lake/quarantine/`, manifests in `lake/_manifests/` |
| the published warehouse | `warehouse/pipewright.duckdb` (`ops.build_info` has its business date and verdict) |
| check reports, incident notes | `warehouse/reports/dq-<D>.json`, `warehouse/reports/incident-<D>.md` |
| task logs | Airflow UI, or `docker compose logs airflow` |
