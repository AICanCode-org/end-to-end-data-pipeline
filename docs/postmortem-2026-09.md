# Post-mortem PW-2026-09: the dashboard stopped at 31 August

Status: closed · Severity: SEV-3 (stale data, no wrong number published) · Format: blameless · Times: IST

## Summary
On 2 September the daily run for business date 1 September refused to publish: Marketing had launched an
`influencer` acquisition channel on 1 September, and the first customers it acquired failed the contract check
`channel_accepted` (severity error). The publish gate worked as designed: the dashboard kept showing 31 August with
its date, and no wrong number reached anyone. But nothing told a human: the CEO noticed at 09:00, three hours after
the run and two hours after her daily review. Replaying the rest of September found three more changes the v1.0
pipeline could not handle: order status `rto` (from 5 September), coupons with a new ads-export column (from 15
September, which the PySpark track could not even read), and a spend file three hours late (21 September).

## Impact
- 2 September: the CEO's 07:00 review used 31 August numbers (labelled "Data as of 2026-08-31"); about three hours
  of a stale dashboard, one leadership meeting delayed.
- No incorrect number was published, at any point. No data was lost: the lake kept every partition and the next
  successful run rebuilt everything.

## Timeline (2 September unless stated)
| Time (IST) | What happened |
|---|---|
| 1 Sep, all day | Marketing's influencer campaign goes live; 158 customers acquired through it in September, the first on 1 Sep. |
| 06:00 | `pipewright_daily` starts for business date 2026-09-01; all drops present; ingest and transform succeed. |
| 06:01 | `dq`: 1 blocking failure (`channel_accepted`); not published; `warehouse/reports/incident-2026-09-01.md` written; the task fails (no retries for the gate), the DAG run is marked failed. |
| 07:00 | The CEO's review: the dashboard shows 31 August under a red "NOT published" banner. Nobody on the data team is online yet. |
| 09:00 | The CEO asks the data team why the dashboard is a day old. First human aware of the incident. |
| 09:20 | The engineer reads the incident note (the failing check and its explanation) and confirms the new channel with Marketing. |
| 11:30 | An interim, reviewed change adds `influencer` to the accepted channels; the run is cleared in Airflow and publishes 1 September. |
| 5-6 Sep | First `rto` statuses (return to origin) on 5 Sep block the next run the same way; Finance agrees `rto` is never revenue; second interim change. |
| 15 Sep | Coupons launch: orders carry `coupon_code`/`discount_paise`, the ads export gains a fifth column `campaign_id`. The SQL track reads it by name; the PySpark track fails with a malformed-CSV error (exit 6), found in the September replay. |
| 21 Sep | The spend file for 20 Sep arrives at about 09:00 instead of before 05:00; the run waits (retries) and publishes late, again without an alert. |
| 25 Sep | v1.1.0 released with every action below. |

## Detection
Three hours, by the most senior user. The pipeline knew at 06:01 (failed task, incident note) but had no channel to
tell anyone, and a banner on the dashboard does not page anyone.

## Root cause
Upstream teams change what they send (new values, new columns) with no channel to tell the data team, and v1.0 had
**no alerting on a blocked publish or a missed SLA**. The contract check behaved correctly; what was missing was the
path from a failed check to a person, and a fast, safe way to evolve the contract.

## What went well
- The gate: nothing wrong was published, and the last good day stayed visible with its date.
- The incident note named the failing check and explained it in plain words: diagnosis took 20 minutes.
- Atomic publishing and an append-only lake made recovery a re-run, not a repair.

## What went badly
- No alert: the CEO was the monitoring system.
- Each new value needed a hand-made change under time pressure, with no golden files to prove nothing else moved.
- The PySpark track had read CSV files with one fixed header: a fifth column broke it, so the two tracks were no
  longer interchangeable.
- A late file and a blocked publish looked the same from outside: "the dashboard is old".

## Actions (all done in v1.1.0)
| # | Action | Owner | Evidence |
|---|---|---|---|
| 1 | Contract 1.1.0: `rto`, `influencer`, optional coupon fields and `campaign_id`, `discount_paise`/`net_paise` appended; 1.0.0 in `contract/history/` | data eng | `contract/data-contract.yaml`, ADR-0006 |
| 2 | Monitoring: freshness per source, volume, publish SLA 06:30, DQ history; alerts to `warehouse/reports/alerts.jsonl` and an optional webhook | data eng | `pipewright monitor`, `pipewright_watch` (05:45, 06:45) |
| 3 | PySpark reads CSV files grouped by header, union by name | data eng | `tracks/pyspark/.../bronze.py`, golden v1.1 parity |
| 4 | Backward compatibility as a CI check: 1.1 on 1.0 data = golden 1.0; 1.0 dashboard on 1.1; 1.0 pipeline on 1.1 refuses to publish | data eng | `make backcompat`, CI job `v11` |
| 5 | The late-file morning as a repeatable drill | data eng | `make drill` |
| 6 | Upstream changes announced in the weekly Marketing/Product sync, with a contract PR before launch | Head of Marketing + data eng | process (runbook) |

## Lessons
A gate without an alert turns a data incident into a trust incident. Fail safe, then tell a person, before the user
notices.
