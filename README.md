# Pipewright: an end-to-end data pipeline for a CEO dashboard, built like a pipe that never leaks

[![CI](https://github.com/AICanCode-org/end-to-end-data-pipeline/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AICanCode-org/end-to-end-data-pipeline/actions/workflows/ci.yml)
[![CI (solution)](https://github.com/AICanCode-org/end-to-end-data-pipeline/actions/workflows/ci.yml/badge.svg?branch=solution)](https://github.com/AICanCode-org/end-to-end-data-pipeline/actions/workflows/ci.yml?query=branch%3Asolution)

Pipewright is a guided project from **[AiCanCode](https://www.aicancode.org)**, the capstone of the Data
Engineering track. Over 9 phases (about 45 hours) you build **one production-style pipeline the whole way**:
requirements, a data contract and a design with ADRs, test-first ingestion and transformations, data-quality
checks and data tests, CI, a scheduled v1.0 with a dashboard in Docker, and a v1.1 that survives what real
pipelines meet in their second month (late files, schema changes, backfills, freshness SLAs). AI assistants help at
every step, and you **verify** every number they produce.

**The situation.** BazaarBox is a direct-to-consumer home & kitchen brand that ships across India (Bengaluru,
Delhi, Pune, Hyderabad, Jaipur, Kolkata, ...). Every morning the CEO pieces together five numbers from four tools
and a spreadsheet, and they never agree. She asks for **one dashboard, correct by 06:30 IST every day**, with:
1. revenue by category and region, per day;
2. customer acquisition cost (CAC) by marketing channel, per month;
3. the monthly recurring revenue (MRR) of the *BazaarBox Prime* membership;
4. the top 10 products of the last 30 days;
5. 30-day repeat-purchase retention by signup cohort.

**The data.** A seeded, fully offline copy of BazaarBox's systems: an **orders & customers API** (bearer token,
cursor pagination, `updated_since` windows, optional 429/503 faults) and **daily file drops** (payment events as
JSONL, the ads export as CSV, the product catalogue as CSV). Like real sources, they are imperfect:

| Source | Volume (v1.0, Jun-Aug 2026) | Known problems |
|---|---|---|
| orders API | 10,804 orders | times with `Z` *and* `+05:30`, orders that change status days later (and reappear in later windows), test orders, ` delhi ` / `Bangalore` / `BENGALURU`, quantities ≤ 0, unknown SKUs |
| customers API | 4,693 customers | profile versions, email and phone (PII that must never reach the lake), test accounts |
| payments drop | 8,021 events | at-least-once delivery: the same event twice, the same day or the next; Prime subscriptions with refunds and cancellations |
| ads spend drop | 460 rows | amounts as text: `"₹6,485.40"`, `"6,110.68"` |
| catalogue drop | 92 daily snapshots | ` Kitchen` vs `kitchen`, prices in rupees as text, SKUs that appear late |

**What you deliver:** idempotent ingestion into an immutable lake (bronze), silver and gold models that turn it
into the five reports, declarative data-quality checks and a **publish gate** (a bad build is never shown to the
CEO), an Airflow DAG that runs it every day at 06:00 IST, a Streamlit dashboard, Docker Compose for all of it, and
a second release with incremental models, backfills, monitoring and alerts. It is judged not on how many tools
you wired together, but on whether the numbers are **right, repeatable and explainable**: the same input gives the
same tables, re-running a day changes nothing, and every report reconciles to the order lines.

**Pick one track.** Ingestion, the checks, the DAG and the dashboard are shared Python; only the transformations
differ, and both tracks must produce **the same gold tables, row by row**.

| Track | Stack | Folder |
|---|---|---|
| SQL · dbt | dbt-core 1.12 + dbt-duckdb · dbt data tests, unit tests and enforced model contracts · sqlfluff | `tracks/sql` |
| Python · PySpark | PySpark 4.2 in local mode (no cluster) · pytest with small DataFrames · ruff | `tracks/pyspark` |

The optional **AI incident explainer** (`pipewright explain`, from Phase 6) turns a failed data-quality report into
an incident note. It sees only numbered aggregate facts (check ids, counts, thresholds; never rows), its answer is
checked by guardrails, and without a model it falls back to a deterministic template. Everything runs **on your
laptop, offline, with no API key and no paid service**; Docker Compose with everything up stays under 6 GB RAM.

Course repository: **<https://github.com/AICanCode-org/end-to-end-data-pipeline>** (`main` = starter,
`solution` = reference).

---

## Repository layout
```text
.
├── README.md                 ← you are here
├── AI_LOG.md                 ← your log of AI prompts, outputs and how you verified them
├── docs/                     ← the course: phase-0 … phase-8 + GitHub Actions explained
├── spec/                     ← requirements, design, ADRs (you write them in Phases 1-2)
├── contract/                 ← data-contract.yaml (sources, enums, gold schemas, checks), transform-spec.md (every
│                               rule), seeds, the 3-day fixture, the golden gold tables, the source manifests
├── source/                   ← BazaarBox's systems: the seeded generator and the offline API stub
├── pipewright/               ← the shared Python package: ingestion, lake, checks, publish gate, runner, CLI
├── tracks/sql/               ← the dbt project (silver + gold)
├── tracks/pyspark/           ← the same models in PySpark
├── orchestration/            ← the Airflow DAG and its tests
├── dashboard/                ← the Streamlit dashboard (Phase 6)
├── scripts/                  ← doctor.sh, check-all.sh, parity.py, parity-run.sh, verify-tags.sh, build-cms.py, ...
├── cms/                      ← machine-readable export of the course for the AiCanCode site
└── .github/                  ← CI workflows, issue/PR templates
```

## Prerequisites (per operating system)
You need **Git, `make` and Python 3.12** for every track. The PySpark track (and the parity check, which runs both
tracks) also needs **Java 17 or 21**. Docker is needed from Phase 6. Run `scripts/doctor.sh` at any time.

| | Windows 10/11 | macOS | Linux (Ubuntu/Debian) |
|---|---|---|---|
| Shell | **WSL2** with Ubuntu: `wsl --install` in an admin PowerShell, reboot | Terminal (zsh) | any |
| Git, make | inside WSL: `sudo apt install git make shellcheck` | `xcode-select --install`, `brew install shellcheck` | `sudo apt install git make shellcheck` |
| Python 3.12 | inside WSL (Ubuntu 24.04): `sudo apt install python3.12 python3.12-venv` | `brew install python@3.12` | `sudo apt install python3.12 python3.12-venv` (or the deadsnakes PPA) |
| Java (PySpark) | inside WSL: `sudo apt install openjdk-21-jre-headless` | `brew install openjdk@21` | `sudo apt install openjdk-21-jre-headless` |
| Docker (Phase 6+) | Docker Desktop with the WSL 2 engine | Docker Desktop (or Colima) | Docker Engine + compose plugin |

**Windows, important:** clone and work **inside the WSL file system** (`~/code/end-to-end-data-pipeline`), not
under `/mnt/c/...`. Hardware: 8 GB RAM is plenty. With 4-6 GB, use the SQL track and skip the Airflow profile
(the same pipeline runs with `make run`).

## Quick start (5 minutes)
```bash
git clone https://github.com/<you>/end-to-end-data-pipeline.git && cd end-to-end-data-pipeline   # your fork
scripts/doctor.sh                 # check your tools
make setup                        # .venv with Python 3.12: the shared package + both tracks' tools
make lint test                    # all tests are "pending" (skipped) until Phase 3
make source                       # generate the source data, check it against the manifest, serve the API stub
```

| Command | What it does |
|---|---|
| `make setup` / `make setup-airflow` | create `.venv` (everything but Airflow) / `.venv-airflow` (the DAG tests) |
| `make lint` | ruff (all Python), sqlfluff (dbt models), shellcheck (scripts) |
| `make test` | every suite on the 3-day fixture (seconds; pending tests are skipped) |
| `make test-pipewright`, `test-sql`, `test-pyspark`, `test-orchestration` | one suite |
| `make source` | generate the source data and serve the API stub on `127.0.0.1:18140` |
| `make run TRACK=sql` | ingest + transform + check + publish every day from 2026-06-01 (needs `make source`) |
| `make parity` | the full data through **both** tracks: equal to `contract/golden/`, and to each other |
| `make check` | lint + tests + parity: what CI checks |

## How the phases work
| Phase | Topic | You end at tag |
|---|---|---|
| 0 | [Setup, the brief and the sources](docs/phase-0-setup.md) | `phase-0-end` |
| 1 | [Requirements: the five reports, SLAs and acceptance criteria](docs/phase-1-requirements.md) | `phase-1-end` |
| 2 | [Design: architecture, the data contract, layers and ADRs](docs/phase-2-design.md) | `phase-2-end` |
| 3 | [Implementation in four milestones: ingest, silver, gold, run](docs/phase-3-implementation.md) | `phase-3-end` |
| 4 | [Data tests and data-quality checks: the publish gate](docs/phase-4-testing.md) | `phase-4-end` |
| 5 | [CI: dbt build and tests on the fixture, cross-track parity](docs/phase-5-ci.md) | `phase-5-end` |
| 6 | [v1.0: the scheduled pipeline, the dashboard, the AI explainer, Docker](docs/phase-6-release.md) | `phase-6-end` = **`v1.0.0`** |
| 7 | [v1.1: incremental models, backfills, schema evolution, monitoring and alerts](docs/phase-7-operations.md) | `phase-7-end` = **`v1.1.0`** |
| 8 | [Retrospective and portfolio](docs/phase-8-retrospective.md) | `phase-8-end` |

Each phase page has the same 8 blocks: *Why it matters · Objectives · Step-by-step instructions · AI-assist
prompts · Deliverables · Self-check quiz · What you learned · Catch-up git commands*.

### Branches and tags
- **`main`**: the **starter**. The shared package and both tracks have their structure, the full (pending) test
  suite and a `TODO` with a hint (and a pointer into `contract/transform-spec.md`) in every function and model;
  the DAG is a skeleton. The source generator, the contract, the fixture and the golden files are complete.
- **`solution`**: the reference, one or more commits per phase, an annotated tag at the end of every phase
  (`phase-0-end` … `phase-8-end`) and the releases `v1.0.0` and `v1.1.0`. Every tag passes its checks.

**Stuck or behind?** Start the next phase from the reference end of the previous one:
```bash
git fetch upstream --tags
git switch -c my-phase-4 phase-3-end
```
**Compare with the reference:** `git diff phase-3-end -- tracks/sql`. Try first, then compare.

### Pending tests
The complete test suite is already on `main` but switched off in one `PENDING` list per suite
(`pipewright/tests/conftest.py`, `tracks/sql/pytests/conftest.py`, `tracks/pyspark/tests/conftest.py`,
`orchestration/tests/conftest.py`). In Phase 3 you remove one entry at a time, watch the tests fail, and make
them pass. Skipped is never reported as passed.

## Working with AI
Use any assistant you like. Each phase has copy-paste prompts and a **"Verify the output by"** checklist; log what
you asked and how you checked it in [`AI_LOG.md`](AI_LOG.md). Three rules matter most in data engineering: never
paste secrets, tokens or row-level customer data (emails, phones, addresses) into a prompt (schemas, your own code,
error messages and aggregates are fine); never accept a transformation without running it on the fixture and
comparing it with the expected tables; and be most suspicious of AI-written SQL that "works": a join that fans
out or a filter on the wrong side gives a plausible number that is wrong. The reconciliation checks exist for
exactly that.

## Coming from the old five-step guide?
The earlier version (Architecture · Ingestion with Airbyte · dbt models · Airflow · Metabase) named the right tools
but shipped no data, no tests and no way to know if the dashboard was right, and its stack needed 16+ GB of RAM.
This version ships a seeded source with real-world defects, a contract with every rule written down, golden
tables, two transformation tracks that check each other, data-quality gates, CI, a scheduled release and a second
release for operations. The tool swaps (Airbyte → a small readable ingestion package, Delta + a Spark cluster →
lake files + DuckDB with PySpark as a track, Metabase → Streamlit) are explained in `spec/design.md` (Phase 2).
The mapping of the old steps is in [`docs/README.md`](docs/README.md).

## More
- [docs/README.md](docs/README.md): index of all docs
- [docs/github-actions-explained.md](docs/github-actions-explained.md): every CI concept used here
- [contract/transform-spec.md](contract/transform-spec.md): every parsing, silver, gold and report rule
- [source/README.md](source/README.md): the source systems (API and file drops)
- [CONTRIBUTING.md](CONTRIBUTING.md) · [CHANGELOG.md](CHANGELOG.md) · [LICENSE](LICENSE) (MIT)

Questions or problems: open an issue using the templates, or contact us via <https://www.aicancode.org/contact>.
