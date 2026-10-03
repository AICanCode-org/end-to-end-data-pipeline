# Pipewright: every command you need, the same ones CI runs.   make help
SHELL := /bin/bash
# Inside .venv when it exists (make setup), else the active Python (CI).
PY ?= $(if $(wildcard .venv/bin/python),$(abspath .venv/bin/python),python3)
AIRFLOW_PY ?= .venv-airflow/bin/python
TRACK ?= sql
AIRFLOW_CONSTRAINTS := https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt

.PHONY: help setup setup-airflow install-pipewright install-sql install-pyspark install-dashboard \
        lint lint-pipewright lint-sql lint-pyspark test test-pipewright test-sql test-pyspark test-orchestration test-dashboard parity \
        source run dashboard up down check

help: ## list the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-20s %s\n", $$1, $$2}'

setup: ## create .venv with everything except Airflow (Python 3.12; the PySpark track also needs Java 17+)
	python3.12 -m venv .venv
	$(MAKE) install-sql install-pyspark install-dashboard PY=$(abspath .venv/bin/python)

install-pipewright: ## the shared package (editable) + its test tools, into $(PY)
	$(PY) -m pip install -r pipewright/requirements-dev.txt -e pipewright

install-sql: install-pipewright ## + dbt-duckdb and sqlfluff
	$(PY) -m pip install -r tracks/sql/requirements-dev.txt

install-pyspark: install-pipewright ## + PySpark (needs Java 17+ on PATH)
	$(PY) -m pip install -r tracks/pyspark/requirements-dev.txt

install-dashboard: install-pipewright ## + Streamlit
	$(PY) -m pip install -r dashboard/requirements-dev.txt

setup-airflow: ## a separate .venv-airflow for the DAG tests (Airflow's pins never mix with dbt's)
	python3.12 -m venv .venv-airflow
	.venv-airflow/bin/pip install -r orchestration/requirements-dev.txt --constraint $(AIRFLOW_CONSTRAINTS)

lint: lint-pipewright lint-sql lint-pyspark ## everything: ruff, sqlfluff, shellcheck

lint-pipewright: ## ruff on the shared Python (package, source stub, scripts, DAG, dashboard) + shellcheck
	$(PY) -m ruff check --exclude tracks .
	shellcheck scripts/*.sh

lint-sql: ## sqlfluff on the dbt models and tests
	cd tracks/sql && PIPEWRIGHT_DUCKDB=:memory: $(PY) -m sqlfluff lint models tests  # dbt templater: compile only, no file

lint-pyspark: ## ruff on the PySpark track
	$(PY) -m ruff check tracks/pyspark

test-pipewright: ## shared package: ingestion, checks, publishing, runner, explainer (coverage >= 85%)
	cd pipewright && $(PY) -m pytest --cov

test-sql: ## SQL track: silver + gold vs the expected files, dbt data + unit tests
	cd tracks/sql && $(PY) -m pytest pytests

test-pyspark: ## PySpark track: same expected files, plus unit tests (coverage >= 85%)
	cd tracks/pyspark && PYTHONPATH=.$${PYTHONPATH:+:$$PYTHONPATH} $(PY) -m pytest --cov

test-orchestration: ## the Airflow DAG (needs make setup-airflow)
	cd orchestration && $(abspath $(AIRFLOW_PY)) -m pytest tests -q

test-dashboard: ## the Streamlit dashboard
	cd dashboard && $(PY) -m pytest --cov

test: test-pipewright test-sql test-pyspark test-dashboard ## every suite except the DAG

parity: ## full data through both tracks: identical gold tables, equal to contract/golden
	PY=$(PY) scripts/parity-run.sh 1.0

source: ## generate the source data and serve the API stub on :18140 (Ctrl-C to stop)
	cd source && $(PY) -m bazaarbox_source generate --out ../source-data --check ../contract/source-manifest-1.0.json
	cd source && SOURCE_NOW=follow $(PY) -m bazaarbox_source serve --data ../source-data --port 18140

run: ## the whole backfill with TRACK=sql|pyspark against the running stub (make source in another terminal)
	PIPEWRIGHT_TRACK=$(TRACK) PYTHONPATH=tracks/pyspark$${PYTHONPATH:+:$$PYTHONPATH} $(PY) -m pipewright run --start 2026-06-01 --end 2026-08-31

dashboard: ## the dashboard on :18142, reading warehouse/pipewright.duckdb
	$(PY) -m streamlit run dashboard/app.py --server.port 18142

up: ## Docker: source + dashboard; then: docker compose --profile pipeline run --rm pipeline-sql run --start ... --end ...
	docker compose up -d --build

down: ## Docker: stop everything and delete the volumes
	docker compose --profile pipeline --profile airflow down -v --remove-orphans

check: lint test parity ## what CI checks, in one go
