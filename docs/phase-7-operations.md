---
phase: 7
slug: phase-7-operations
title: "v1.1: incremental models, backfills, schema evolution, monitoring and alerts"
duration: 6 h
start_tag: phase-6-end
end_tag: phase-7-end
release: v1.1.0
---
# Phase 7 · v1.1: incremental models, backfills, schema evolution, monitoring and alerts

## 1. Why it matters
v1.0 went live on 1 September. On 2 September the 06:00 run was **blocked**: Marketing had launched a new
`influencer` channel and the first customers it acquired failed the `channel_accepted` check. The gate did its job
(the CEO saw 31 August's numbers, clearly labelled), but nobody noticed until the CEO asked at 09:00 why the dashboard
was a day old. Three days later the orders API started sending a new status, `rto` (return to origin: the courier
could not deliver a cash-on-delivery order). On 15 September coupons launched and the ads export gained a column, and
on 21 September the spend file arrived three hours late. This phase is the pipeline's
second month: you version the contract for the new values without breaking v1.0 readers, make the daily build
incremental (and prove it equals a full rebuild), add a backfill command, and add the monitoring that would have paged
someone at 06:45 instead of the CEO at 09:00. You end with a post-mortem and **v1.1.0**.

## 2. Objectives
By the end of this phase you can:
- Evolve a data contract additively (new enum values, new optional columns), version it, and prove backward compatibility in both directions.
- Make fact tables incremental with a lookback window for late updates, and prove incremental = full refresh on every table.
- Backfill a past range safely (re-ingest, rebuild only what it touched) without touching later days.
- Monitor freshness, volume, the publish SLA and data-quality history; raise alerts to a log and an optional webhook without leaking its URL.
- Run a late-file drill and write a blameless post-mortem with actions.

## 3. Step-by-step instructions

### Step 7.1 · Read the incident, write the post-mortem skeleton
Generate the September data and replay 2 September with your v1.0 code (or `make backcompat`, check C, which does it
for you):
```bash
cd source && ../.venv/bin/python -m bazaarbox_source generate --out ../source-data --version 1.1 \
  --check ../contract/source-manifest-1.1.json && cd ..
```
Start `docs/postmortem-2026-09.md`: timeline (IST), impact, detection (how long until someone noticed, and who), root
cause, what went well (the gate), what went badly (no alert; the PySpark track could not even read the new spend CSV
with a fifth column), actions. You will fill the actions in as you build them.

### Step 7.2 · Contract 1.1.0: additive schema evolution
Copy `contract/data-contract.yaml` to `contract/history/data-contract-1.0.0.yaml`, then bump to `1.1.0`:
- enums: `order_status` + `rto` (not revenue, not a valid order), `channel` + `influencer`;
- orders: optional `coupon_code`, `discount_paise`; spend: optional `campaign_id`;
- gold `fct_orders` + `discount_paise`, `net_paise` (= gross − discount), **appended at the end** so readers that
  select columns by name keep working; a `monitoring:` section (Step 7.5).
Update `contract/transform-spec.md` (rto excluded like cancelled; §6 incremental builds) and regenerate the golden
files from both tracks. Additive changes are minor versions; renaming or removing a column would be 2.0.0.

### Step 7.3 · Incremental facts, proven equal to a full refresh
A full rebuild re-reads every partition every day. Incremental: rebuild only the orders **updated since D − 3 days**
(the `lookback_days` of late status changes), plus lines whose category was `unknown` (a late-arriving product), and
merge them into the previously published facts. A full refresh happens when there is no published warehouse, the
contract version changed, the other track built it, or you pass `--full-refresh`.

<!-- track:sql -->
**SQL · dbt:** make `fct_order_items` and `fct_orders` incremental:
```sql
{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='delete+insert',
          on_schema_change='fail') }}
...
{% if is_incremental() %}
where {{ ist_date('orders.updated_at_utc') }} >= {{ incremental_since() }}      -- var since, or run_date - lookback
   or items.order_id in (select previous.order_id from {{ this }} as previous where previous.category = 'unknown')
{% endif %}
```
`delete+insert` by `order_id` replaces every line of a changed order (a line can disappear). `on_schema_change='fail'`
turns an unplanned column change into a loud error instead of a silent drift. The runner copies the published
warehouse into the build first, so `{{ this }}` is yesterday's facts. dbt unit tests on an incremental model need
`overrides: {macros: {is_incremental: false}}`.
<!-- /track -->

<!-- track:pyspark -->
**Python · PySpark:** the runner exports the previously published facts to Parquet (`--previous`); `pipeline.facts()`
keeps the previous rows whose `order_id` is **not** in the rebuilt set and unions the rebuilt rows:
```python
changed = silver_orders.where(F.col("updated_at_utc") >= since).select("order_id").union(unknown_ids).distinct()
rebuilt = build_facts(silver.join(changed, "order_id"))
merged = previous.join(changed, "order_id", "left_anti").unionByName(rebuilt)
```
Write Parquet to `<table>.next` and swap, never overwrite the folder you are reading from. Read CSV drops grouped by
header, so 4- and 5-column spend files can be read in one run (the September bug).
<!-- /track -->

Prove it: `make incremental` runs 26-30 September one day at a time (incremental) and compares every gold table with a
`--full-refresh` of the same lake, on both tracks. It must print `MATCH` for all nine tables.

### Step 7.4 · Backfills
`pipewright backfill --start 2026-09-10 --end 2026-09-12` re-ingests the range (the source may have corrected it) and
rebuilds with `since` = the first day, so every order touched in the range is rebuilt; later days are untouched and the
published date does not move backwards. Every run (daily, backfill, blocked, failed) is recorded in `ops.run_log`.

### Step 7.5 · Monitoring and alerts
Implement `pipewright monitor --date D [--now ISO]` from the contract's `monitoring:` section:
- **freshness:** file drops by 05:00 IST (payments `error`, spend and catalogue `warn`); API data in the lake by 06:15;
- **volume:** the day's rows between 0.5× and 2× the median of the previous 7 days;
- **SLA:** D published by 06:30 IST; **dq:** the latest report's blocking failures.
Results go to `ops.duckdb` (rewritten copy-and-replace so the dashboard can read it) and every alert to
`warehouse/reports/alerts.jsonl`; alerts at or above `ALERT_MIN_SEVERITY` (default `error`) are POSTed to `ALERT_WEBHOOK_URL`
if set. The URL is a secret: never log it, and a failing webhook never fails the pipeline. Add a `monitor` task at the
end of the daily DAG (`trigger_rule=ALL_DONE`, so it runs even when publishing failed) and a `pipewright_watch` DAG at
05:45 and 06:45 IST. Add a *Data health* section to the dashboard.

### Step 7.6 · The late-file drill and backward compatibility
```bash
make drill        # replays 21 Sep: 05:45 warn (spend missing) → 06:00 run waits (exit 3) → 06:45 errors (SLA, orders)
                  # → the file arrives → incremental publish → monitor green
make backcompat OLD=../v1.0.0     # git worktree add ../v1.0.0 v1.0.0 first
```
Backcompat checks three things: **A** the v1.1 pipeline on v1.0 data reproduces the v1.0 golden reports (old data, new
code); **B** the v1.0 dashboard renders a v1.1 warehouse (new data, old reader); **C** the v1.0 pipeline on v1.1 data
refuses to publish and the last good day stays (the September incident, contained by the gate). Add `make incremental`,
`make drill` and the backcompat check to CI (the `v11` job, with `fetch-depth: 0` for the tag), finish the post-mortem,
add a monitoring section to the runbook, CHANGELOG `1.1.0`, tag `v1.1.0`.

## 4. AI-assist prompts

### Prompt 1: Classify a schema change
```text
My data contract (YAML) is version 1.0.0: <paste the relevant part>. The source now sends <a new enum value rto | a
new optional column coupon_code | a renamed column>. For each consumer (ingestion, silver, gold, data-quality checks,
dashboard, the previous release's code), say whether the change is backward compatible, forward compatible, or
breaking, what must change, and whether this is a minor or a major contract version.
```
**Verify the output by:** testing each claim with the backcompat check (A/B/C), not by reasoning alone, and making sure
a new enum value's *business meaning* (is `rto` revenue?) was decided by a person and written in the spec.

### Prompt 2: Find the hole in my incremental logic
```text
My fact table is rebuilt incrementally: <paste the dbt model or the PySpark merge>. Late updates arrive up to 3 days
after an order; products can appear in the catalogue after their first order; orders can lose a line. Construct
three concrete event sequences (with dates) where my incremental result would differ from a full rebuild.
```
**Verify the output by:** turning each sequence into a test or a run of `make incremental` over a range that contains
it; only a difference you can reproduce is a bug. Keep the full-refresh comparison as the final judge.

### Prompt 3: Write the post-mortem with me
```text
Here is the timeline of a data incident (IST), the alerts that existed, and what we changed: <paste>. Draft a blameless
post-mortem: summary, impact (who saw what, for how long), detection, root cause (5 whys), what went well, what went
badly, actions with owners and due dates. Do not invent events that are not in my timeline.
```
**Verify the output by:** checking every timestamp against your logs (`ops.run_log`, `alerts.jsonl`, the drill output),
removing blame from people's names, and making sure every action is something you actually built or a ticket.

## 5. Deliverables
- [ ] Contract 1.1.0 with `history/data-contract-1.0.0.yaml`, the updated spec, golden files for v1.1.
- [ ] Incremental facts on your track; `make incremental` prints `MATCH` for all nine tables.
- [ ] `pipewright backfill` and `pipewright monitor`; alerts in `warehouse/reports/alerts.jsonl`; the webhook URL never logged.
- [ ] The `monitor` task and the `pipewright_watch` DAG; the dashboard's Data health section.
- [ ] `make drill` and `make backcompat` pass; the `v11` CI job is green.
- [ ] `docs/postmortem-2026-09.md`, runbook monitoring section, CHANGELOG `1.1.0`, tag `v1.1.0`.
- [ ] `AI_LOG.md` entries, including an incremental edge case the AI proposed that you disproved (or proved).

## 6. Self-check quiz

### Q1. Why is adding `rto` to the order statuses a minor (1.1.0) and not a patch or major change?
- [ ] Because it is small
- [x] It is additive: existing values, columns and meanings stay, so old readers keep working; but numbers can change (rto orders stop counting), so it is more than a patch; nothing is removed or renamed, so it is not major
- [ ] Enum changes are always major
- [ ] Versions do not matter for data
> **Answer:** Version the contract like an API. Additive = minor; removing, renaming or changing a meaning = major, with a migration plan.

### Q2. Why does the incremental build rebuild orders updated in the last 3 days instead of only yesterday's?
- [ ] To be safe in general
- [x] Status changes and corrections can arrive late; the lookback re-processes every order that may have changed since the last builds, so the facts stay equal to a full rebuild
- [ ] dbt requires 3 days
- [ ] Because the API is slow
> **Answer:** Incremental logic is a promise: "same result as a full rebuild, cheaper". The lookback window (and the unknown-category rule) is what keeps it; `make incremental` checks it.

### Q3. Why `delete+insert` by `order_id` rather than `merge` by `(order_id, line_number)`?
- [ ] It is faster
- [x] An updated order can lose a line; replacing all lines of each changed order removes it, while a per-line merge would keep the stale line
- [ ] DuckDB has no merge
- [ ] Line numbers change
> **Answer:** Choose the incremental grain from how the source changes: here the order is the unit of change.

### Q4. The spend file is late. At 05:45 the monitor raises a `warn`; at 06:45 an `error`. Why two severities?
- [ ] To produce more alerts
- [x] Before the deadline it is a heads-up (the file may still arrive); after the SLA it is an incident that needs a human, and only that should page
- [ ] Warnings are free
- [ ] The webhook only accepts errors
> **Answer:** Alert on symptoms that matter, at the time they matter. Too many pages train people to ignore them.

### Q5. Why must the webhook URL never appear in logs?
- [ ] It is long
- [x] Incoming-webhook URLs contain a secret token: anyone with the URL can post to the channel
- [ ] Logs cannot contain URLs
- [ ] It changes every day
> **Answer:** Treat it like a password. Log "webhook delivery failed (HTTP 500)" without the URL, and never let a failed alert fail the pipeline.

### Q6. Backcompat check C runs the v1.0 pipeline on v1.1 data and expects it to FAIL. Why is that a pass?
- [ ] It is a bug in the check
- [x] Old code meeting new values must refuse to publish rather than publish wrong numbers; the check proves the gate still contains the incident and the last good day stays visible
- [ ] v1.0 should be deleted
- [ ] Because v1.1 is better
> **Answer:** Forward compatibility of old code is not always possible; failing safe is the requirement. Check C turns the September incident into a regression test.

## 7. What you learned
- Contract versioning and additive schema evolution, with backward-compatibility checks in both directions.
- Incremental models with a lookback for late data, and full-refresh equality as their test.
- Safe backfills and a run log.
- Monitoring of freshness, volume, SLA and data quality; alerting with severities and secret-safe webhooks.
- Drills and blameless post-mortems as part of operating a pipeline.

## 8. Catch-up git commands
```bash
git fetch upstream --tags
git switch -c my-phase-8 phase-7-end         # = v1.1.0
git diff v1.0.0 v1.1.0 --stat
git diff v1.0.0 v1.1.0 -- contract/data-contract.yaml
```
