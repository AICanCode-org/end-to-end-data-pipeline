# Pipewright documentation

Start with [Phase 0](phase-0-setup.md). Each phase lists its start and end tag in the header, so you can start any
phase from the reference state (`git switch -c my-phase-N phase-(N-1)-end`).

| # | Phase | Duration | Start | End |
|---|---|---|---|---|
| 0 | [Setup, the brief and the sources](phase-0-setup.md) | 2 h | `main` | `phase-0-end` |
| 1 | [Requirements: the five reports, SLAs and acceptance criteria](phase-1-requirements.md) | 2.5 h | `phase-0-end` | `phase-1-end` |
| 2 | [Design: architecture, the data contract, layers and ADRs](phase-2-design.md) | 4 h | `phase-1-end` | `phase-2-end` |
| 3 | [Implementation in four milestones: ingest, silver, gold, run](phase-3-implementation.md) | 15 h | `phase-2-end` | `phase-3-end` |
| 4 | [Data tests and data-quality checks: the publish gate](phase-4-testing.md) | 5 h | `phase-3-end` | `phase-4-end` |
| 5 | [CI: dbt build and tests on the fixture, cross-track parity](phase-5-ci.md) | 3 h | `phase-4-end` | `phase-5-end` |
| 6 | [v1.0: the scheduled pipeline, the dashboard, the AI explainer, Docker](phase-6-release.md) | 6 h | `phase-5-end` | `phase-6-end` / `v1.0.0` |
| 7 | [v1.1: incremental models, backfills, schema evolution, monitoring and alerts](phase-7-operations.md) | 6 h | `phase-6-end` | `phase-7-end` / `v1.1.0` |
| 8 | [Retrospective and portfolio](phase-8-retrospective.md) | 1.5 h | `phase-7-end` | `phase-8-end` |

Reference material:
- [GitHub Actions explained](github-actions-explained.md): triggers, jobs, matrix, caching, path filters, the
  aggregator job, Docker in CI, history and worktrees, permissions, secrets, Dependabot, actionlint.
- [`../contract/`](../contract/README.md): the data contract, the transformation spec, seeds, the fixture, the golden
  tables and the source manifests.
- [`../source/`](../source/README.md): the source systems (API and file drops).
- [`../spec/`](../spec/README.md): requirements, design, ADRs.
- Solution branch only: `runbook.md` (Phase 6, monitoring section in Phase 7), `postmortem-2026-09.md` (Phase 7),
  `retrospective.md` (Phase 8).

## From the old five-step guide
The old guide had five steps. Their content moved like this:

| Old step | Now |
|---|---|
| 1 · Architecture & Design | Phase 1 (requirements: the five reports, SLAs, acceptance criteria) and Phase 2 (architecture, layers, data contract, ADRs, tool choices and RAM budget) |
| 2 · Data Sources & Ingestion (Airbyte) | Phase 0 (the sources) and Phase 3 M1 (ingestion package: windows, retries, idempotent lake partitions, PII; ADR-0002 explains replacing Airbyte) |
| 3 · dbt Transformation Models | Phase 3 M2-M3 (silver and gold in dbt **or** PySpark, enforced contracts) and Phase 4 (dbt data tests, unit tests, contract checks, the publish gate) |
| 4 · Airflow Orchestration | Phase 3 M4 (the DAG) and Phase 6 (Airflow in Docker on a schedule); Phase 7 (the watch DAG, monitoring, backfills) |
| 5 · Metabase Dashboard & Final Deliverables | Phase 6 (Streamlit dashboard, AI explainer, Docker, v1.0.0) and Phase 8 (case study and retrospective) |

New: Phase 5 (CI with parity of both tracks) and Phase 7 (v1.1: incremental models, schema evolution, monitoring).

## Page format
Every phase page has front matter (`phase`, `slug`, `title`, `duration`, `start_*`, `end_tag`, `release`) and the
same eight sections, which `scripts/build-cms.py` turns into `cms/end-to-end-data-pipeline.json` (project metadata
comes from `cms/project.json`):

1. Why it matters · 2. Objectives · 3. Step-by-step instructions (`### Step N.x`) ·
4. AI-assist prompts (`### Prompt n`, a `text` block, **Verify the output by**) · 5. Deliverables ·
6. Self-check quiz (`### Qn`, `- [ ]` / `- [x]`, `> **Answer:**`) · 7. What you learned ·
8. Catch-up git commands

Track-specific parts are wrapped in `<!-- track:sql -->` … `<!-- /track -->` (also `pyspark`). The site shows only
the learner's track. On GitHub you see both.
