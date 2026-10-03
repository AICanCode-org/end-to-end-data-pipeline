# Pipewright dashboard (Streamlit), reading the published warehouse read-only.
#   docker build -f dashboard/Dockerfile -t pipewright-dashboard:local .
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 HOME=/tmp \
    PIPEWRIGHT_HOME=/app PIPEWRIGHT_WAREHOUSE=/data/warehouse/pipewright.duckdb \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false STREAMLIT_SERVER_HEADLESS=true
RUN --mount=type=bind,source=dashboard/requirements.txt,target=/tmp/requirements.txt \
    pip install -r /tmp/requirements.txt \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin pipewright \
 && mkdir -p /data && chown 10001:10001 /data
COPY contract /app/contract
COPY pipewright /app/pipewright
COPY dashboard/app.py dashboard/data.py /app/dashboard/
RUN pip install --no-deps /app/pipewright && rm -rf /app/pipewright/build /app/pipewright/*.egg-info
WORKDIR /app
USER 10001
EXPOSE 8501
HEALTHCHECK --interval=10s --timeout=3s --retries=10 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8501/_stcore/health', timeout=2)"]
ENTRYPOINT ["streamlit", "run", "/app/dashboard/app.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
