# Pipewright, PySpark track: the shared `pipewright` CLI + PySpark in local mode (Java 21, no cluster).
#   docker build -f tracks/pyspark/Dockerfile -t pipewright-pyspark:local .
# apt uses an HTTPS mirror (plain-HTTP mirrors are blocked on some networks); if apt hangs on yours, pick another
# HTTPS mirror: DEBIAN_MIRROR=https://cloudfront.debian.net docker compose build pipeline-pyspark
FROM python:3.12-slim
ARG DEBIAN_MIRROR=https://deb.debian.org
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 HOME=/tmp \
    PIPEWRIGHT_HOME=/app PIPEWRIGHT_TRACK=pyspark SOURCE_API_URL=http://source:8000 PIPEWRIGHT_DROPS_DIR=/source-data/drops \
    PIPEWRIGHT_LAKE_DIR=/data/lake PIPEWRIGHT_WAREHOUSE=/data/warehouse/pipewright.duckdb \
    PYTHONPATH=/app/tracks/pyspark SPARK_DRIVER_MEMORY=1g SPARK_LOCAL_DIRS=/tmp
RUN --mount=type=bind,source=tracks/pyspark/requirements.txt,target=/tmp/requirements.txt \
    sed -i "s|http://deb.debian.org|${DEBIAN_MIRROR}|g" /etc/apt/sources.list.d/debian.sources \
 && apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps \
 && rm -rf /var/lib/apt/lists/* \
 && pip install -r /tmp/requirements.txt \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin pipewright \
 && mkdir -p /data /source-data && chown 10001:10001 /data /source-data
COPY contract /app/contract
COPY pipewright /app/pipewright
COPY scripts/parity.py /app/scripts/parity.py
COPY tracks/pyspark/pipewright_spark /app/tracks/pyspark/pipewright_spark
RUN pip install --no-deps /app/pipewright && rm -rf /app/pipewright/build /app/pipewright/*.egg-info
WORKDIR /app
USER 10001
ENTRYPOINT ["pipewright"]
CMD ["--help"]
