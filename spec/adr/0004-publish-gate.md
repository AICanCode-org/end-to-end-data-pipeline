# ADR-0004: Build, check, then publish with one atomic rename

- Status: accepted
- Date: 2026-10-03

## Context
NFR-2 (correctness) and US-1: the CEO must never see a half-built or failed build; a stale but labelled number is
acceptable, a wrong one is not.

## Options considered
1. Transform in place in the warehouse the dashboard reads; run checks afterwards.
2. Write-audit-publish with schema swaps inside one database.
3. Build into a separate file, check it, record the verdict in the build, `os.replace` it onto the published file.

## Decision
Option 3. `pipewright transform` writes `warehouse/pipewright_build.duckdb`; `pipewright dq` writes
`ops.build_info(run_date, publishable, ...)` into it; `pipewright publish` refuses (exit 1) unless publishable, then
renames the build onto `warehouse/pipewright.duckdb`.

## Consequences
- A crash at any step leaves the published warehouse untouched; readers open it read-only and see either the old or
  the new file (rename is atomic on one file system).
- The published file carries its own business date and check results: the dashboard can show "data as of".
- One writer at a time (the build file is shared): the DAG has `max_active_runs=1`.
- The build and published files must be on the same file system (one Docker volume).
