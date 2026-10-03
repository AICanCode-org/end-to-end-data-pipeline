#!/usr/bin/env bash
# 1.1: incremental builds must give exactly the tables of a full rebuild. For each track: backfill up to FROM-1
# (a full build), then run FROM..END one day at a time (incremental builds), then compare every gold table with a
# --full-refresh build of the same lake.
#   scripts/incremental-check.sh [FROM] [END]     default 2026-09-26 .. 2026-09-30 (crosses nothing special, fast)
# Env: PY (default python3), PORT (default 18141), TRACKS (default "sql pyspark"), KEEP=1 keeps the work dir.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
FROM=${1:-2026-09-26}
END=${2:-2026-09-30}
PY=${PY:-python3}
PORT=${PORT:-18141}
TRACKS=${TRACKS:-sql pyspark}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/pipewright-incremental.XXXXXX")
(cd "$ROOT/source" && $PY -m bazaarbox_source generate --out "$WORK/source" --version 1.1 \
  --check "$ROOT/contract/source-manifest-1.1.json") >/dev/null
if [ -d "$WORK/source/late" ]; then cp -r "$WORK/source/late/." "$WORK/source/drops/"; fi
(cd "$ROOT/source" && SOURCE_NOW=follow SOURCE_QUIET=1 exec $PY -m bazaarbox_source serve --data "$WORK/source" --port "$PORT") &
STUB=$!
trap 'kill $STUB 2>/dev/null || true; [ "${KEEP:-0}" = 1 ] || rm -rf "$WORK"' EXIT
for _ in $(seq 50); do curl -sf "http://127.0.0.1:$PORT/health" >/dev/null && break; sleep 0.2; done
before=$($PY -c "import datetime as d; print(d.date.fromisoformat('$FROM') - d.timedelta(days=1))")
rc=0
for t in $TRACKS; do
  pw() {
    PIPEWRIGHT_HOME=$ROOT SOURCE_API_URL=http://127.0.0.1:$PORT PIPEWRIGHT_DROPS_DIR=$WORK/source/drops PIPEWRIGHT_PAGE_SIZE=500 \
      PIPEWRIGHT_LAKE_DIR=$WORK/$t/lake PIPEWRIGHT_WAREHOUSE=$WORK/$t/warehouse/pipewright.duckdb PIPEWRIGHT_TRACK=$t \
      PYTHONPATH=$ROOT/tracks/pyspark${PYTHONPATH:+:$PYTHONPATH} bash -c "cd \"$WORK\" && exec $PY -m pipewright $*"
  }
  echo "== $t: backfill 2026-06-01 .. $before (full build)"
  pw backfill --start 2026-06-01 --end "$before" | grep -E "transform|published"
  d=$FROM
  while [[ "$d" < "$END" || "$d" == "$END" ]]; do
    pw run --date "$d" | grep -E "transform" | sed 's/^/   /'
    grep -q '"incremental": true' "$WORK/$t/warehouse/reports/last-transform.json" || { echo "   $d was not incremental"; rc=1; }
    d=$($PY -c "import datetime as x; print(x.date.fromisoformat('$d') + x.timedelta(days=1))")
  done
  cp "$WORK/$t/warehouse/pipewright.duckdb" "$WORK/$t-incremental.duckdb"
  echo "== $t: full refresh as of $END"
  pw run --date "$END" --full-refresh | grep -E "transform"
  echo "== $t: incremental vs full refresh (all gold tables, row by row)"
  $PY "$ROOT/scripts/parity.py" compare "$WORK/$t-incremental.duckdb" "$WORK/$t/warehouse/pipewright.duckdb" | sed 's/^/   /' || rc=1
done
[ "$rc" = 0 ] && echo "INCREMENTAL CHECK OK" || echo "INCREMENTAL CHECK FAILED"
exit $rc
