#!/usr/bin/env bash
# Smoke test a running Pipewright stack:  scripts/smoke-test.sh [SOURCE_URL] [DASHBOARD_URL]
#   defaults http://127.0.0.1:18140 and http://127.0.0.1:18142
# EXPLAIN_CMD    a command that prints `pipewright explain --report contract/fixture/incident-report.json`
#                (e.g. "docker compose --profile pipeline run --rm -T pipeline-sql explain --report ...")
# EXPECT_SOURCE  template | llm: which explainer source EXPLAIN_CMD must report (default: either)
# EXPECT_RUN_DATE the business date the dashboard must show (default: any)
set -euo pipefail
SRC=${1:-http://127.0.0.1:18140}
DASH=${2:-http://127.0.0.1:18142}
TOKEN=${SOURCE_API_TOKEN:-bazaarbox-dev-token}
pass=0
fail=0
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pipewright-smoke.XXXXXX")
trap 'rm -rf "$TMP"' EXIT

check() { # check NAME COMMAND...
  local name=$1
  shift
  if "$@" >"$TMP/out" 2>&1; then
    pass=$((pass + 1))
    echo "ok   $name"
  else
    fail=$((fail + 1))
    echo "FAIL $name: $(head -c 300 "$TMP/out")"
  fi
}

status_is() { # status_is CODE URL [curl args...]
  local code=$1 url=$2 got
  shift 2
  got=$(curl -s -o "$TMP/body" -w '%{http_code}' "$@" "$url")
  [ "$got" = "$code" ] || { echo "HTTP $got for $url: $(head -c 200 "$TMP/body")"; return 1; }
}

body_check() { # body_check PYTHON_EXPR (d = the JSON body of the last status_is)
  python3 -c "import json,sys; d=json.load(open(sys.argv[1])); assert $1, d" "$TMP/body"
}

ok_json() { # ok_json URL PYTHON_EXPR [curl args...]
  local url=$1 expr=$2
  shift 2
  status_is 200 "$url" "$@" && body_check "$expr"
}

auth=(-H "Authorization: Bearer $TOKEN")
check "source health" ok_json "$SRC/health" "d['status'] == 'ok'"
check "source rejects a missing token (401)" status_is 401 "$SRC/v1/orders"
check "401 is problem+json" body_check "d['status'] == 401 and 'title' in d"
check "orders page" ok_json "$SRC/v1/orders?updated_since=2026-08-01T00:00:00%2B05:30&updated_before=2026-08-02T00:00:00%2B05:30&limit=5" \
  "len(d['data']) == 5 and d['next_cursor']" "${auth[@]}"
check "limit above 500 is 400" status_is 400 "$SRC/v1/orders?updated_since=2026-06-01T00:00:00Z&updated_before=2026-06-02T00:00:00Z&limit=501" "${auth[@]}"
check "dashboard health" status_is 200 "$DASH/_stcore/health"
check "dashboard page" status_is 200 "$DASH/"
if [ -n "${EXPECT_RUN_DATE:-}" ]; then
  check "dashboard data is as of $EXPECT_RUN_DATE" bash -c "${RUN_DATE_CMD:?RUN_DATE_CMD must print the published run date} | grep -qx '$EXPECT_RUN_DATE'"
fi
if [ -n "${EXPLAIN_CMD:-}" ]; then
  explain() {
    bash -c "$EXPLAIN_CMD" >"$TMP/explain" 2>&1 || { cat "$TMP/explain"; return 1; }
    if ! grep -q "^Source: ${EXPECT_SOURCE:-}" "$TMP/explain" || ! grep -q "status_accepted" "$TMP/explain"; then
      head -c 600 "$TMP/explain"
      return 1
    fi
  }
  check "incident explainer (source ${EXPECT_SOURCE:-any})" explain
fi
echo "smoke: $pass passed, $fail failed ($SRC, $DASH)"
[ "$fail" -eq 0 ]
