# Airflow 3 (standalone: scheduler + API server + DAG processor + triggerer in one container, LocalExecutor) with
# the pipewright CLI (SQL track) in its own virtualenv, so Airflow's pins never fight dbt's.
#   docker compose --profile airflow up -d      (UI on http://localhost:18144, local use only)
FROM python:3.12-slim
ARG AIRFLOW_VERSION=3.3.2
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    AIRFLOW_HOME=/opt/airflow/home PATH=/opt/airflow/venv/bin:$PATH PIPEWRIGHT_BIN=/opt/pipewright/bin/pipewright \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false DO_NOT_TRACK=1 \
    PIPEWRIGHT_HOME=/app PIPEWRIGHT_TRACK=sql SOURCE_API_URL=http://source:8000 PIPEWRIGHT_DROPS_DIR=/source-data/drops \
    PIPEWRIGHT_LAKE_DIR=/data/lake PIPEWRIGHT_WAREHOUSE=/data/warehouse/pipewright.duckdb
RUN --mount=type=bind,source=orchestration/requirements.txt,target=/tmp/airflow.txt \
    --mount=type=bind,source=tracks/sql/requirements.txt,target=/tmp/sql.txt \
    python -m venv /opt/airflow/venv \
 && /opt/airflow/venv/bin/pip install -r /tmp/airflow.txt "apache-airflow-providers-postgres" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt" \
 && python -m venv /opt/pipewright && /opt/pipewright/bin/pip install -r /tmp/sql.txt \
 && useradd --uid 10001 --create-home --shell /usr/sbin/nologin pipewright \
 && mkdir -p /data /source-data /opt/airflow/home && chown 10001:10001 /data /source-data /opt/airflow/home
COPY contract /app/contract
COPY pipewright /app/pipewright
COPY tracks/sql /app/tracks/sql
COPY orchestration/dags /opt/airflow/dags
RUN /opt/pipewright/bin/pip install --no-deps /app/pipewright && rm -rf /app/pipewright/build /app/pipewright/*.egg-info
WORKDIR /app
USER 10001
EXPOSE 8080
ENTRYPOINT ["airflow"]
CMD ["standalone"]
