#!/usr/bin/env bash
# Run the same checks CI runs, locally, in one go.
#   scripts/check-all.sh          lint + every test suite (pending tests are skipped)
#   scripts/check-all.sh --full   + the full data through both tracks (parity with the golden files and with each
#                                 other), the CMS export check and, from v1.1, the incremental-vs-full check
# Settings (maintainers, scripts/verify-tags.sh):
#   PY=/path/to/python         a Python with every requirement installed (default: .venv, else python3)
#   AIRFLOW_PY=/path/to/python a Python with Airflow for the DAG tests (default: .venv-airflow; skipped if missing)
#   PORT=18141                 the port for the source stub of the full run
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1
FULL=0
[ "${1:-}" = "--full" ] && FULL=1
PY="${PY:-$( [ -x .venv/bin/python ] && echo "$ROOT/.venv/bin/python" || echo python3 )}"
AIRFLOW_PY="${AIRFLOW_PY:-$ROOT/.venv-airflow/bin/python}"
RESULTS=()
FAILED=0

step() { # step <label> <command...>
  local label="$1"; shift
  echo; echo "──── $label"
  if "$@"; then RESULTS+=("PASS  $label"); else RESULTS+=("FAIL  $label"); FAILED=$((FAILED + 1)); fi
}

step "lint: ruff, sqlfluff, shellcheck" make -s lint PY="$PY"
step "pipewright: tests" make -s test-pipewright PY="$PY"
step "sql: tests" make -s test-sql PY="$PY"
step "pyspark: tests" make -s test-pyspark PY="$PY"
if [ -x "$AIRFLOW_PY" ]; then
  step "orchestration: DAG tests" make -s test-orchestration AIRFLOW_PY="$AIRFLOW_PY"
else
  RESULTS+=("SKIP  orchestration: DAG tests (no $AIRFLOW_PY: make setup-airflow)")
fi
[ -d dashboard ] && step "dashboard: tests" make -s test-dashboard PY="$PY"

if [ "$FULL" = 1 ]; then
  version=$("$PY" -c 'import yaml; v = yaml.safe_load(open("contract/data-contract.yaml"))["version"]; print(v.rsplit(".", 1)[0])')
  step "parity: full data $version through both tracks = golden, and sql = pyspark" env PY="$PY" PORT="${PORT:-18141}" \
    scripts/parity-run.sh "$version"
  [ -x scripts/incremental-check.sh ] && step "incremental builds = full refresh (both tracks)" env PY="$PY" PORT="${PORT:-18141}" \
    scripts/incremental-check.sh
  step "cms: the export is up to date with docs/" python3 scripts/build-cms.py --check
fi

echo; echo "════ Summary"
printf '%s\n' "${RESULTS[@]}"
[ "$FAILED" -eq 0 ]
