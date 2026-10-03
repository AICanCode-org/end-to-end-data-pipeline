#!/usr/bin/env bash
# 1.1 backward-compatibility checks against a checkout of v1.0.0 (CI: `git worktree add ../v1.0.0 v1.0.0`).
#   A. the 1.1 pipeline on 1.0 data gives exactly the 1.0 golden reports        (old data, new code: compatible)
#   B. the 1.0 dashboard renders a 1.1 warehouse (extra columns, ops.duckdb)     (new data, old reader: compatible)
#   C. the 1.0 pipeline on 1.1 data refuses to publish (rto / influencer are unknown to contract 1.0.0) and the
#      dashboard keeps the last good day: the September incident (docs/postmortem-2026-09.md), contained by the gate
#   scripts/backcompat-check.sh OLD_TREE      Env: PY (default python3), PORT (default 18141), TRACK (default sql)
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
OLD=$(cd "${1:?usage: backcompat-check.sh OLD_TREE (a checkout of v1.0.0)}" && pwd)
PY=${PY:-python3}
PORT=${PORT:-18141}
TRACK=${TRACK:-sql}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/pipewright-backcompat.XXXXXX")
STUB=
trap '[ -n "$STUB" ] && kill "$STUB" 2>/dev/null; [ "${KEEP:-0}" = 1 ] || rm -rf "$WORK"' EXIT
serve() { # serve <version>: generate (checked against the committed manifest) and serve that source
  [ -n "$STUB" ] && { kill "$STUB" 2>/dev/null || true; wait "$STUB" 2>/dev/null || true; }
  (cd "$ROOT/source" && $PY -m bazaarbox_source generate --out "$WORK/source-$1" --version "$1" \
    --check "$ROOT/contract/source-manifest-$1.json") >/dev/null
  if [ -d "$WORK/source-$1/late" ]; then cp -r "$WORK/source-$1/late/." "$WORK/source-$1/drops/"; fi
  (cd "$ROOT/source" && SOURCE_NOW=follow SOURCE_QUIET=1 exec $PY -m bazaarbox_source serve --data "$WORK/source-$1" --port "$PORT") &
  STUB=$!
  for _ in $(seq 50); do curl -sf "http://127.0.0.1:$PORT/health" >/dev/null && break; sleep 0.2; done
}
pw() { # pw <tree> <name> <version> <args...>: run that tree's pipewright on that source
  local tree=$1 name=$2 ver=$3; shift 3
  PIPEWRIGHT_HOME=$tree SOURCE_API_URL=http://127.0.0.1:$PORT PIPEWRIGHT_DROPS_DIR=$WORK/source-$ver/drops PIPEWRIGHT_PAGE_SIZE=500 \
    PIPEWRIGHT_LAKE_DIR=$WORK/$name/lake PIPEWRIGHT_WAREHOUSE=$WORK/$name/warehouse/pipewright.duckdb PIPEWRIGHT_TRACK=$TRACK \
    PYTHONPATH=$tree/pipewright:$tree/tracks/pyspark${PYTHONPATH:+:$PYTHONPATH} bash -c "cd \"$WORK\" && exec $PY -m pipewright $*"
}
rc=0
echo "== A. 1.1 pipeline on 1.0 data ($TRACK) vs contract/golden/v1.0"
serve 1.0
pw "$ROOT" new-on-10 1.0 run --start 2026-06-01 --end 2026-08-31 | grep -E "published"
# the 1.0 golden files have the 1.0 columns: compare those (the appended 1.1 columns are new by definition)
PARITY_CONTRACT=$ROOT/contract/history/data-contract-1.0.0.yaml \
  $PY "$ROOT/scripts/parity.py" compare "$WORK/new-on-10/warehouse/pipewright.duckdb" "$ROOT/contract/golden/v1.0" | sed 's/^/   /' || rc=1

echo "== B. 1.0 dashboard on a 1.1 warehouse"
serve 1.1
pw "$ROOT" new-on-11 1.1 run --start 2026-06-01 --end 2026-09-30 | grep -E "published"
pw "$ROOT" new-on-11 1.1 monitor --date 2026-09-30 --now 2026-10-01T07:00+05:30 >/dev/null || true
PIPEWRIGHT_HOME=$OLD PIPEWRIGHT_WAREHOUSE=$WORK/new-on-11/warehouse/pipewright.duckdb AI_PROVIDER=template \
  $PY - "$OLD/dashboard/app.py" <<'PYEOF' || rc=1
import sys
from streamlit.testing.v1 import AppTest
at = AppTest.from_file(sys.argv[1], default_timeout=120).run()
assert not at.exception, at.exception
assert len(at.tabs) == 6 and at.metric[0].value.startswith("₹"), "1.0 dashboard did not render the 1.1 warehouse"
print(f"   OK: the 1.0 dashboard renders the 1.1 warehouse ({len(at.dataframe)} tables, revenue {at.metric[0].value})")
PYEOF

echo "== C. 1.0 pipeline on 1.1 data: publishes August, refuses September"
pw "$OLD" old-on-11 1.1 run --start 2026-06-01 --end 2026-08-31 | grep -E "published"
set +e
pw "$OLD" old-on-11 1.1 run --start 2026-09-01 --end 2026-09-30 > "$WORK/old-on-11.log" 2>&1
code=$?
set -e
grep -E "dq:|not published|track failed" "$WORK/old-on-11.log" | sed 's/^/   /'
published=$($PY -c "import duckdb,sys; print(duckdb.connect(sys.argv[1], read_only=True).execute('SELECT run_date FROM ops.build_info').fetchone()[0])" \
  "$WORK/old-on-11/warehouse/pipewright.duckdb")
if [ "$code" = 0 ]; then echo "   FAIL: the 1.0 pipeline published 1.1 data"; rc=1
elif [ "$published" != 2026-08-31 ]; then echo "   FAIL: published date is $published"; rc=1
else echo "   OK: exit $code, nothing published, the dashboard still shows $published"; fi
[ "$rc" = 0 ] && echo "BACKCOMPAT OK" || echo "BACKCOMPAT FAILED"
exit $rc
