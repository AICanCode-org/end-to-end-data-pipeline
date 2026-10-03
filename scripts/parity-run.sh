#!/usr/bin/env bash
# Cross-track parity on the full data: generate the source (and check it against the committed manifest), run the
# whole date range through BOTH tracks, then compare each with the golden files and with each other.
#   scripts/parity-run.sh [VERSION]        VERSION 1.0 (default); needs both tracks' requirements installed
# Env: PY (python, default python3), PORT (stub port, default 18141), TRACKS (default "sql pyspark"), KEEP=1 keeps the work dir.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
VER=${1:-1.0}
PY=${PY:-python3}
PORT=${PORT:-18141}
TRACKS=${TRACKS:-sql pyspark}
END=$($PY -c "import sys; sys.path.insert(0, '$ROOT/source'); from bazaarbox_source.generate import HORIZONS; print(HORIZONS['$VER'])")
WORK=$(mktemp -d "${TMPDIR:-/tmp}/pipewright-parity.XXXXXX")
[ "${KEEP:-0}" = 1 ] || trap 'rm -rf "$WORK"' EXIT
(cd "$ROOT/source" && $PY -m bazaarbox_source generate --out "$WORK/source" --version "$VER" \
  --check "$ROOT/contract/source-manifest-$VER.json")
(cd "$ROOT/source" && SOURCE_NOW=follow SOURCE_QUIET=1 exec $PY -m bazaarbox_source serve --data "$WORK/source" --port "$PORT") &
STUB=$!
trap 'kill $STUB 2>/dev/null || true; [ "${KEEP:-0}" = 1 ] || rm -rf "$WORK"' EXIT
for _ in $(seq 50); do curl -sf "http://127.0.0.1:$PORT/health" >/dev/null && break; sleep 0.2; done
rc=0
for t in $TRACKS; do
  echo "== $t track: 2026-06-01 .. $END"
  start=$(date +%s)
  PIPEWRIGHT_HOME=$ROOT SOURCE_API_URL=http://127.0.0.1:$PORT PIPEWRIGHT_DROPS_DIR=$WORK/source/drops PIPEWRIGHT_PAGE_SIZE=500 \
    PIPEWRIGHT_LAKE_DIR=$WORK/$t/lake PIPEWRIGHT_WAREHOUSE=$WORK/$t/warehouse/pipewright.duckdb PIPEWRIGHT_TRACK=$t \
    PYTHONPATH=$ROOT/tracks/pyspark${PYTHONPATH:+:$PYTHONPATH} bash -c "cd \"$WORK\" && exec $PY -m pipewright run --start 2026-06-01 --end $END" | tail -3
  echo "   $(( $(date +%s) - start ))s; compare with contract/golden/v$VER:"
  $PY "$ROOT/scripts/parity.py" compare "$WORK/$t/warehouse/pipewright.duckdb" "$ROOT/contract/golden/v$VER" | sed 's/^/   /' || rc=1
done
read -r -a T <<<"$TRACKS"
if [ "${#T[@]}" -ge 2 ]; then
  echo "== ${T[0]} vs ${T[1]} (all gold tables, row by row)"
  $PY "$ROOT/scripts/parity.py" compare "$WORK/${T[0]}/warehouse/pipewright.duckdb" "$WORK/${T[1]}/warehouse/pipewright.duckdb" | sed 's/^/   /' || rc=1
fi
[ "$rc" = 0 ] && echo "PARITY RUN OK" || echo "PARITY RUN FAILED"
exit $rc
