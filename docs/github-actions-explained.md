# GitHub Actions, explained for data engineers

This page explains every GitHub Actions concept used by Pipewright's workflows, in the order you meet them. It is
shorter than a full course: each section is what you need to read our YAML and change it safely. Keep the files
open next to it:

| File | Appears in | Purpose |
|---|---|---|
| `.github/workflows/ci.yml` | starter (simple), Phase 5 (full), Phase 6 (+ dashboard, Docker), Phase 7 (+ v1.1 checks) | lint, tests on the fixture (incl. `dbt build`), the DAG tests, parity of both tracks on the full data, containers, incremental/backcompat checks |
| `.github/dependabot.yml` | Phase 5 | weekly dependency update PRs |

Official reference: <https://docs.github.com/actions>. When this page and the docs disagree, the docs win; please
open an issue so we can fix this page.

---

## 1. Vocabulary in one minute
- **Workflow:** a YAML file in `.github/workflows/`. It runs when an **event** happens.
- **Job:** a set of steps on one fresh virtual machine (a **runner**, here `ubuntu-latest`). Jobs run in parallel unless one `needs:` another.
- **Step:** a shell command (`run:`) or a packaged **action** (`uses: actions/checkout@v4`).
- **Run:** one execution of a workflow, with a page, logs and a status (the green tick or red cross on a commit).
- **Check:** each job's result as shown on a pull request; branch protection can require some of them.

## 2. Triggers (`on:`)
```yaml
on:
  push:
    branches: [main, solution]   # every push to these branches
  pull_request:                  # every PR, whatever its branch
  workflow_dispatch:             # a "Run workflow" button on the Actions tab
```
CI does not run for tags: a tag points to a commit CI has already checked on its branch. A data pipeline usually
also has a `schedule:` trigger somewhere; ours does not, on purpose: the *pipeline* is scheduled by Airflow
(Phase 6), CI only checks the *code*. Mixing the two (running production loads from CI) couples deployments to
data runs and hands CI credentials it should never need.

## 3. Jobs, steps and the keywords around them
- `runs-on: ubuntu-latest`: the machine. It starts with common tools (Python, Java, Docker, git, shellcheck).
- `needs: [sql, pyspark]`: wait for those jobs; their outputs and results become available.
- `if:`: run the job or step only when a condition holds (`if: failure()` runs only after a failed step).
- `timeout-minutes:`: kill a stuck job (a hung Spark session, a dbt run waiting on a lock). The default is 6 hours.
- `working-directory: source`: where a `run:` starts.
- `concurrency:` with `cancel-in-progress: true`: a new push to a branch cancels the old run of that branch.

## 4. Expressions and contexts: `${{ … }}`
GitHub replaces `${{ … }}` before the step runs. The contexts we use: `github` (event, ref), `needs` (results and
outputs of earlier jobs), `steps` (outputs of earlier steps), `matrix`, `runner` (`runner.temp`) and `env`:
```yaml
if: needs.changes.outputs.all == 'true' || needs.changes.outputs.sql == 'true'
name: ${{ matrix.part }} · lint + unit
```
Prefer environment variables over pasting `${{ }}` into a shell script:
`env: { RESULTS: ${{ join(needs.*.result, ' ') }} }`, then `$RESULTS` in the script. It avoids quoting bugs and
script injection (a branch name is user input).

## 5. Matrix builds
The starter's `ci.yml` runs one job per part with a matrix:
```yaml
strategy:
  fail-fast: false            # one red part does not cancel the others
  matrix:
    part: [pipewright, sql, pyspark]
name: ${{ matrix.part }} · lint + unit
```
Three jobs, three checks: `pipewright · lint + unit`, `sql · lint + unit`, `pyspark · lint + unit`. These are the
names branch protection requires on `main`. The full CI keeps exactly these job names, so protection keeps
working when you replace the file in Phase 5. Each job calls the same Makefile targets you run locally
(`make install-<part>`, `make lint-<part>`, `make test-<part>`): CI is never a second, different definition of
"the checks".

## 6. Toolchains and caching
1. **Python:** `actions/setup-python@v5` with `python-version: "3.12"` (the version of `make setup` and the Docker
   images) and `cache: pip`, which restores pip's download cache keyed by the hash of the files in
   `cache-dependency-path`. A changed requirements file means a new key and a fresh install.
2. **Java for Spark:** `actions/setup-java@v4` with `distribution: temurin`, `java-version: "21"`, only in the
   jobs that start a Spark session (`pyspark`, `parity`, `v11`). PySpark is a pip package, but it needs a JVM.
3. **Airflow:** installed with its official **constraints file**
   (`--constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt`), the
   same pins as `make setup-airflow` and the Docker image. Airflow has hundreds of transitive dependencies;
   without the constraints a new release of one of them breaks your DAG tests on a Monday morning. It gets its own
   job (and its own virtualenv locally) so its pins never fight dbt's.
4. **No database server:** DuckDB runs inside the Python process and the fixture is in the repository, so no
   `services:` container is needed until the Docker job.

## 7. Job outputs, path filters and the aggregator job
GitHub's built-in `on: push: paths:` skips the **whole workflow**, and a required check that never runs blocks a
PR forever. So the workflow always runs and decides per job:
1. `changes` runs `dorny/paths-filter@v3` and outputs `'true'`/`'false'` per area (`shared`, `docs`, `sql`,
   `pyspark`, `orchestration`, from Phase 6 also `dashboard` and `docker`). It also computes one output itself:
   ```yaml
   outputs:
     all: ${{ github.event_name != 'pull_request' || steps.filter.outputs.shared == 'true' }}
   ```
   A change to `pipewright/`, `contract/`, `source/`, `scripts/`, `Makefile` or the workflows is `shared`: it
   re-runs everything, because both tracks' numbers depend on it. Pushes to `main`/`solution` always run everything.
2. Each job has an `if:` on those outputs.
3. `ci-ok` has `if: always()`, `needs:` on everything, and fails only if a result is `failure` or `cancelled`;
   skipped jobs are fine. In your own fork, `ci-ok` is the one check to require.

## 8. The checks, job by job (Phase 5)
| Job | What it proves | Locally |
|---|---|---|
| `contract` | the committed fixture and the source manifest are exactly what the seeded generator writes; scripts pass shellcheck; `cms/` is up to date | `python -m bazaarbox_source generate ... --check`, `python3 scripts/build-cms.py --check` |
| `pipewright · lint + unit` | ruff on all shared Python + shellcheck; the shared package's tests with the coverage gate | `make lint-pipewright test-pipewright` |
| `sql · lint + unit` | sqlfluff (dbt templater); **`dbt build` on the fixture** (seeds, models, data tests, unit tests, contracts), then silver and gold compared with `contract/golden/fixture*` | `make lint-sql test-sql` |
| `pyspark · lint + unit` | ruff; the same expected tables + unit tests with small DataFrames, coverage gate | `make lint-pyspark test-pyspark` |
| `orchestration · DAG tests` | the DAG imports, has the right tasks, order, schedule, retries | `make test-orchestration` |
| `parity` | the full history through **both** tracks: every gold table equals `contract/golden/v1.x` and the two tracks equal each other row by row | `make parity` |

The `parity` job writes its result to the run page with `$GITHUB_STEP_SUMMARY`, a file whose Markdown content
GitHub shows on the run's summary.

## 9. Docker in CI (Phase 6)
The runner has Docker and the compose plugin. The `docker` job runs `scripts/docker-check.sh` with `AIRFLOW=1`,
exactly what you run locally: build every image, start the source and the dashboard, run both pipelines in their
containers, compare their gold tables, smoke-test the dashboard and the explainer (template, AI provider
unreachable, a fake LLM), prove every container is non-root with a read-only root file system and that a fake API
key never appears in any log or image, then trigger the Airflow DAG and wait for success. On failure the
container logs are uploaded as an **artifact**:
```yaml
- if: failure()
  uses: actions/upload-artifact@v4
  with: { name: docker-logs, path: "${{ runner.temp }}/docker-logs", retention-days: 7 }
```
Artifacts are files saved from a job, downloadable from the run page. Jobs never share files any other way: each
runs on a fresh machine. A `Tear down` step with `if: always()` runs `docker compose down -v` even after a failure.

## 10. History in CI: `fetch-depth` and worktrees (Phase 7)
`actions/checkout` fetches **one commit** by default. The backward-compatibility check needs the `v1.0.0` tag:
```yaml
- uses: actions/checkout@v4
  with: { fetch-depth: 0 }                      # all history and tags
- run: git worktree add "$RUNNER_TEMP/v1.0.0" v1.0.0 && make backcompat OLD="$RUNNER_TEMP/v1.0.0"
```
A worktree is a second checkout of the same repository in another folder: the old release's code next to the new
one, without cloning twice.

## 11. Permissions and `GITHUB_TOKEN`
Every run gets an automatic, short-lived token, valid only for this repository and only while the job runs. Its
powers are set by `permissions:`:
```yaml
permissions:
  contents: read          # top of the workflow: least privilege by default
jobs:
  changes:
    permissions: { contents: read, pull-requests: read }   # the path filter lists a PR's files through the API
```
Anything not listed is `none`. Our CI never writes to the repository, never pushes images and needs no secret.

## 12. Secrets and variables
**Secrets** are encrypted and masked as `***` in logs; **variables** are plain configuration. Pipewright's CI uses
none: the source is offline, the AI explainer's tests use a fake LLM server, the Docker job points the explainer
at an unreachable address to prove the fallback, and the monitoring webhook (`ALERT_WEBHOOK_URL`, Phase 7) is a
runtime setting of *your* deployment, not of CI. If you add one later: **Settings → Secrets and variables →
Actions**, or `gh secret set NAME` (it prompts, so the value never lands in your shell history). Secrets cannot be
used in `if:` conditions; copy them into `env:` and test the variable.

## 13. Pinning, Dependabot and actionlint
- `uses: actions/checkout@v4` follows the `v4` tag, which the publisher can move. For maximum supply-chain safety
  pin a full commit SHA (`@<40-char-sha> # v4.2.2`). We use major tags for readability, and Dependabot
  (`package-ecosystem: github-actions` and `pip`) proposes updates weekly. Every Dependabot PR runs the full CI,
  including parity, so a dbt or Spark upgrade that moves a number is caught before it is merged.
- `actionlint` (<https://github.com/rhysd/actionlint>) checks workflows, expressions and embedded shell (via
  shellcheck). Run it before pushing; our workflows pass it with no findings.

## 14. Debugging a red run
1. Open the failed step and read the **last** error, then scroll up to the first one.
2. Reproduce locally with the same command (`make test-sql`, `make parity`), or everything at once with
   `scripts/check-all.sh --full`.
3. `sql` red with a dbt error? The log names the failing test; `cd tracks/sql && dbt test --select <name>` and read
   the compiled SQL in `target/compiled/`.
4. Parity red? The step prints `MATCH` or `DIFF` per table, with what differs (missing or extra keys, differing
   cells, or a different fingerprint for the big tables). Fix the transformation, not the
   golden file: golden files change only with a contract version bump.
5. Green locally, red in CI? Compare versions (`python --version`, `java -version`), and remember CI starts from a
   clean checkout: an untracked seed, a stale `target/` or a `lake/` folder on your laptop does not exist there.
6. Still unclear? **Re-run jobs → Enable debug logging**, or add a step `run: env | sort` (secrets stay masked).

## 15. Annotated walk-through of a pull request that touches `tracks/sql`
1. Event `pull_request` → `changes` runs: `sql` = true, `shared` = false, so `all` = false.
2. Jobs whose `if:` is false are **skipped** (`pipewright`, `pyspark`, `orchestration`); `sql` and then `parity`
   run (`parity` needs both tracks' jobs to be green or skipped, and its `if:` is true because SQL changed).
3. `sql`: checkout → Python 3.12 with the pip cache → `make install-sql` → sqlfluff → `dbt build` on the fixture and
   the comparison with the expected tables.
4. `parity`: Python + Java → both tracks installed → `scripts/parity-run.sh` generates the full source (checked
   against the manifest), runs every day through both tracks, compares with the golden tables and with each other.
5. `ci-ok` collects the results: success + skipped → green. Merge.
