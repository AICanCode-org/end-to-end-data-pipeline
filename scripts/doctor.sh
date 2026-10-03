#!/usr/bin/env bash
# Phase 0 environment check: what is installed and what is missing.
# Usage: scripts/doctor.sh            (Linux, macOS, WSL2)
set -u
ok()   { printf '  \033[32m✔\033[0m %-26s %s\n' "$1" "$2"; }
miss() { printf '  \033[31m✘\033[0m %-26s %s\n' "$1" "$2"; MISSING=$((MISSING + 1)); }
warn() { printf '  \033[33m!\033[0m %-26s %s\n' "$1" "$2"; }
MISSING=0
ver() { "$@" 2>&1 | head -1; }

echo "Every track"
if command -v git >/dev/null; then ok git "$(ver git --version)"; else miss git "https://git-scm.com/downloads"; fi
if command -v make >/dev/null; then ok make "$(ver make --version)"; else miss make "Ubuntu/WSL: sudo apt install make · macOS: xcode-select --install"; fi
if command -v python3.12 >/dev/null; then
  ok "python 3.12" "$(ver python3.12 --version)"
  if python3.12 -m venv --help >/dev/null 2>&1; then ok "python venv" "available"; else miss "python venv" "Ubuntu/WSL: sudo apt install python3.12-venv"; fi
else
  miss "python 3.12" "make setup uses python3.12 (dbt, PySpark and Airflow all support it): https://www.python.org/downloads/"
fi
if command -v curl >/dev/null; then ok curl "$(ver curl --version | cut -c1-40)"; else miss curl "used by the scripts to wait for the source stub"; fi
if command -v shellcheck >/dev/null; then ok shellcheck "$(ver shellcheck --version | sed -n 2p)"; else warn shellcheck "make lint uses it: sudo apt install shellcheck · brew install shellcheck"; fi

echo; echo "Python · PySpark track (and the parity check, which runs both tracks)"
if command -v java >/dev/null; then
  ok java "$(ver java -version)"
else
  miss java "PySpark needs Java 17 or 21: sudo apt install openjdk-21-jre-headless · brew install openjdk@21"
fi

echo; echo "From Phase 6"
if command -v docker >/dev/null; then
  ok docker "$(ver docker --version)"
  if docker compose version >/dev/null 2>&1; then ok "docker compose" "$(ver docker compose version)"; else warn "docker compose" "the compose plugin is needed from Phase 6"; fi
else
  warn docker "needed from Phase 6: https://docs.docker.com/get-docker/"
fi
mem=$(awk '/MemTotal/ {printf "%.0f", $2 / 1048576}' /proc/meminfo 2>/dev/null || true)
[ -n "$mem" ] && { if [ "$mem" -ge 8 ]; then ok memory "${mem} GB"; else warn memory "${mem} GB: run one track at a time and skip the Airflow profile (see README › Low RAM)"; fi; }

echo
if grep -qi microsoft /proc/version 2>/dev/null; then
  echo "Detected WSL: keep the repo inside the Linux file system (~/code), not /mnt/c (much faster)."
fi
if [ "$MISSING" -eq 0 ]; then echo "All required tools found."; else echo "$MISSING required tool(s) missing."; exit 1; fi
