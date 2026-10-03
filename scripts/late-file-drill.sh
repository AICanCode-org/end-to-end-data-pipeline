#!/usr/bin/env bash
# 1.1 drill: the marketing-spend file for 2026-09-20 arrives late. Replays the morning of 21 Sep with `--now`:
#   05:45 IST  monitor: the spend drop is missing (warn alert), API data not due yet
#   06:00 IST  the daily run waits: check-drops exits 3 (Airflow retries every 15 min)
#   06:45 IST  monitor: SLA missed + orders not ingested (error alerts, exit 1)
#   ~09:00 IST the file arrives; the run publishes (incremental build); monitor is green again
#   scripts/late-file-drill.sh        Env: PY (default python3), PORT (default 18141), TRACK (default sql), KEEP=1
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY=${PY:-python3}
PORT=${PORT:-18141}
TRACK=${TRACK:-sql}
DAY=2026-09-20
WORK=$(mktemp -d "${TMPDIR:-/tmp}/pipewright-drill.XXXXXX")
(cd "$ROOT/source" && $PY -m bazaarbox_source generate --out "$WORK/source" --version 1.1 \
  --check "$ROOT/contract/source-manifest-1.1.json") >/dev/null
(cd "$ROOT/source" && SOURCE_NOW=follow SOURCE_QUIET=1 exec $PY -m bazaarbox_source serve --data "$WORK/source" --port "$PORT") &
STUB=$!
trap 'kill $STUB 2>/dev/null || true; [ "${KEEP:-0}" = 1 ] || rm -rf "$WORK"' EXIT
for _ in $(seq 50); do curl -sf "http://127.0.0.1:$PORT/health" >/dev/null && break; sleep 0.2; done
pw() {
  PIPEWRIGHT_HOME=$ROOT SOURCE_API_URL=http://127.0.0.1:$PORT PIPEWRIGHT_DROPS_DIR=$WORK/source/drops PIPEWRIGHT_PAGE_SIZE=500 \
    PIPEWRIGHT_LAKE_DIR=$WORK/lake PIPEWRIGHT_WAREHOUSE=$WORK/warehouse/pipewright.duckdb PIPEWRIGHT_TRACK=$TRACK \
    PYTHONPATH=$ROOT/tracks/pyspark${PYTHONPATH:+:$PYTHONPATH} bash -c "cd \"$WORK\" && exec $PY -m pipewright $*"
}
expect() { # expect <exit code> <command...>
  local want=$1 rc=0; shift
  "$@" || rc=$?
  [ "$rc" = "$want" ] || { echo "DRILL FAILED: expected exit $want, got $rc: $*"; exit 1; }
}
echo "== the days before: backfill 2026-06-01 .. 2026-09-19 ($TRACK)"
pw backfill --start 2026-06-01 --end 2026-09-19 | grep -E "published"
echo "== 05:45 IST: the watch DAG runs"
expect 0 pw monitor --date $DAY --now 2026-09-21T05:45+05:30 | grep -E "FAIL|PENDING"
echo "== 06:00 IST: the daily run starts and waits for the drop"
expect 3 pw check-drops --date $DAY
echo "== 06:45 IST: the watch DAG again"
expect 1 pw monitor --date $DAY --now 2026-09-21T06:45+05:30 | grep -E "FAIL"
echo "== ~09:00 IST: the file arrives; the next retry of check_drops succeeds"
cp -r "$WORK/source/late/." "$WORK/source/drops/"
expect 0 pw check-drops --date $DAY
pw run --date $DAY | grep -E "transform|dq:|published"
echo "== 09:15 IST: monitor"
expect 0 pw monitor --date $DAY --now 2026-09-21T09:15+05:30 | grep -cE " PASS " | sed 's/^/   checks passing: /'
echo "== alerts raised during the drill (warehouse/reports/alerts.jsonl)"
sed 's/^/   /' "$WORK/warehouse/reports/alerts.jsonl"
echo "DRILL OK"
