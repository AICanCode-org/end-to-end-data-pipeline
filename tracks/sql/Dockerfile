# Pipewright, SQL track: the shared `pipewright` CLI + dbt-duckdb. Build from the repo root:
#   docker build -f tracks/sql/Dockerfile -t pipewright-sql:local .
#   docker compose --profile pipeline run --rm pipeline-sql run --start 2026-06-01 --end 2026-08-31
# Three layers on purpose: dependencies (cached), code, and the code installed without dependencies.
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 HOME=/tmp \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false DO_NOT_TRACK=1 \
    PIPEWRIGHT_HOME=/app PIPEWRIGHT_TRACK=sql SOURCE_API_URL=http://source:8000 PIPEWRIGHT_DROPS_DIR=/source-data/drops \
    PIPEWRIGHT_LAKE_DIR=/data/lake PIPEWRIGHT_WAREHOUSE=/data/warehouse/pipewright.duckdb
RUN --mount=type=bind,source=tracks/sql/requirements.txt,target=/tmp/requirements.txt \
    pip install -r /tmp/requirements.txt \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin pipewright \
 && mkdir -p /data /source-data && chown 10001:10001 /data /source-data
COPY contract /app/contract
COPY pipewright /app/pipewright
COPY scripts/parity.py /app/scripts/parity.py
COPY tracks/sql /app/tracks/sql
RUN pip install --no-deps /app/pipewright && rm -rf /app/pipewright/build /app/pipewright/*.egg-info
WORKDIR /app
USER 10001
ENTRYPOINT ["pipewright"]
CMD ["--help"]
