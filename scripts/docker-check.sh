#!/usr/bin/env bash
# The Docker acceptance check (CI's docker job runs exactly this):
#   build every image -> source + dashboard up -> both pipelines in containers -> parity -> smoke tests
#   (template explainer; AI provider unreachable with a fake key that must never be logged; a fake LLM) ->
#   non-root / read-only / no secrets in images -> optionally the Airflow DAG end to end -> down -v.
# Env: DC (compose command, default "docker compose"), END (last business date, default from the source version),
#      AIRFLOW=1 (also run the DAG in Airflow), SKIP_BUILD=1, SOURCE_PORT / DASHBOARD_PORT / AIRFLOW_PORT,
#      FAKE_LLM_URL (how the pipeline container reaches the fake LLM), KEEP_LOGS=dir.
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT" || exit 1
read -r -a DC <<<"${DC:-docker compose}"
D=${DOCKER:-docker}
C=("${DC[@]}" --profile pipeline --profile ai-test --profile airflow)
export SOURCE_PORT=${SOURCE_PORT:-18140} DASHBOARD_PORT=${DASHBOARD_PORT:-18142} AIRFLOW_PORT=${AIRFLOW_PORT:-18144}
SV=${SOURCE_VERSION:-1.1}
END=${END:-$(python3 -c "import sys; sys.path.insert(0, 'source'); from bazaarbox_source.generate import HORIZONS; print(HORIZONS['$SV'])")}
IMGS=(pipewright-source:local pipewright-sql:local pipewright-pyspark:local pipewright-dashboard:local)
[ "${AIRFLOW:-0}" = 1 ] && IMGS+=(pipewright-airflow:local)
FAKE_LLM_URL=${FAKE_LLM_URL:-http://fake-llm:18146}
SECRET="sk-pipewright-test-$RANDOM$RANDOM"
OUT=$(mktemp -d "${TMPDIR:-/tmp}/pipewright-docker.XXXXXX")
RC=0
fail() { echo "  FAILED: $*"; RC=1; }
step() { echo "==== $*"; }
run() { "${C[@]}" run --rm -T "$@"; }

"${C[@]}" down -v --remove-orphans >/dev/null 2>&1
if [ "${SKIP_BUILD:-0}" != 1 ]; then
  step "build"
  svcs=(source pipeline-sql pipeline-pyspark dashboard)
  [ "${AIRFLOW:-0}" = 1 ] && svcs+=(airflow)
  "${C[@]}" build "${svcs[@]}" >"$OUT/build.log" 2>&1 || { tail -30 "$OUT/build.log"; fail build; }
fi
for i in "${IMGS[@]}"; do
  printf '  %-28s %-9s uid %-6s layers %s\n' "$i" "$($D image ls --format '{{.Size}}' "$i")" \
    "$($D run --rm --entrypoint id "$i" -u 2>/dev/null)" "$($D history -q "$i" | wc -l)"
  [ "$($D run --rm --entrypoint id "$i" -u 2>/dev/null)" != 0 ] || fail "$i runs as root"
done

step "source + dashboard"
"${C[@]}" up -d --wait --wait-timeout 180 source dashboard >"$OUT/up.log" 2>&1 || { cat "$OUT/up.log"; fail "up"; }

step "pipelines in containers (read-only root fs, non-root): 2026-06-01 .. $END"
for t in sql pyspark; do
  start=$(date +%s)
  run "pipeline-$t" run --start 2026-06-01 --end "$END" >"$OUT/pipeline-$t.log" 2>&1 || fail "pipeline-$t"
  echo "  $t: $(grep -E 'dq:|published' "$OUT/pipeline-$t.log" | tr '\n' ' ' | cut -c1-200) ($(( $(date +%s) - start ))s)"
done
run parity >"$OUT/parity.log" 2>&1 || fail parity
sed 's/^/  /' "$OUT/parity.log"

step "smoke: template explainer"
EXPLAIN="${DC[*]} --profile pipeline run --rm -T pipeline-sql explain --report /app/contract/fixture/incident-report.json"
EXPLAIN_CMD="$EXPLAIN" EXPECT_SOURCE=template scripts/smoke-test.sh "http://127.0.0.1:$SOURCE_PORT" "http://127.0.0.1:$DASHBOARD_PORT" || fail smoke

step "smoke: AI provider unreachable (must fall back to the template; the fake key must never be logged)"
EXPLAIN_CMD="${DC[*]} --profile pipeline run --rm -T -e AI_PROVIDER=openai -e AI_BASE_URL=http://10.255.255.1:9/v1 -e AI_MODEL=x -e AI_TIMEOUT=2 -e AI_API_KEY=$SECRET pipeline-sql explain --report /app/contract/fixture/incident-report.json"
EXPLAIN_CMD="$EXPLAIN_CMD" EXPECT_SOURCE="template (provider unavailable" scripts/smoke-test.sh \
  "http://127.0.0.1:$SOURCE_PORT" "http://127.0.0.1:$DASHBOARD_PORT" >"$OUT/smoke-fallback.log" 2>&1 || fail smoke-fallback
tail -1 "$OUT/smoke-fallback.log" | sed 's/^/  /'

step "smoke: a (fake) OpenAI-compatible model answers within the guardrails"
"${C[@]}" up -d fake-llm >/dev/null 2>&1
sleep 2
EXPLAIN_CMD="${DC[*]} --profile pipeline run --rm -T -e AI_PROVIDER=openai -e AI_BASE_URL=$FAKE_LLM_URL -e AI_MODEL=fake -e AI_API_KEY=$SECRET pipeline-sql explain --report /app/contract/fixture/incident-report.json"
EXPLAIN_CMD="$EXPLAIN_CMD" EXPECT_SOURCE=llm scripts/smoke-test.sh "http://127.0.0.1:$SOURCE_PORT" "http://127.0.0.1:$DASHBOARD_PORT" \
  >"$OUT/smoke-llm.log" 2>&1 || fail smoke-llm
tail -1 "$OUT/smoke-llm.log" | sed 's/^/  /'

if [ "${AIRFLOW:-0}" = 1 ]; then
  step "Airflow: the daily DAG end to end (06:00 IST on the day after $END)"
  "${C[@]}" up -d --wait --wait-timeout 300 airflow >"$OUT/airflow-up.log" 2>&1 || { tail -20 "$OUT/airflow-up.log"; fail airflow-up; }
  next=$(date -d "$END + 1 day" +%F)
  "${C[@]}" exec -T airflow airflow dags test pipewright_daily "${next}T00:30:00+00:00" >"$OUT/airflow-dag.log" 2>&1 || fail airflow-dag
  if grep -q "state=success" "$OUT/airflow-dag.log"; then echo "  DAG run for business date $END: success"; else fail "DAG did not succeed"; fi
fi

step "hardening"
for s in source dashboard; do
  id=$("${C[@]}" ps -q "$s")
  echo "  $s: read-only root fs $($D inspect -f '{{.HostConfig.ReadonlyRootfs}}' "$id"), user $("${C[@]}" exec -T "$s" id -u)"
  [ "$($D inspect -f '{{.HostConfig.ReadonlyRootfs}}' "$id")" = true ] || fail "$s is not read-only"
done
n=$({ "${C[@]}" logs --no-color 2>&1; cat "$OUT"/*.log; } | grep -c "$SECRET")
echo "  lines containing the fake API key in logs and outputs: $n"; [ "$n" = 0 ] || fail "secret leaked"
for i in "${IMGS[@]}"; do
  s=$($D image inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$i" | grep -v '^GPG_KEY=' | grep -ciE '(KEY|TOKEN|SECRET|PASSWORD)=.+')
  [ "$s" = 0 ] || fail "$i has secret-like env vars"
done
echo "  secret-like env vars in images: none found unless reported above"

[ "$RC" = 0 ] || "${C[@]}" logs --no-color --tail 80 >"$OUT/compose-logs.txt" 2>&1
"${C[@]}" down -v --remove-orphans >/dev/null 2>&1
if [ -n "${KEEP_LOGS:-}" ]; then mkdir -p "$KEEP_LOGS" && cp "$OUT"/* "$KEEP_LOGS"/; fi
rm -rf "$OUT"
echo "docker check: $([ "$RC" = 0 ] && echo PASSED || echo FAILED)"
exit $RC
