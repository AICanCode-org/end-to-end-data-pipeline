# BazaarBox source systems: the orders/customers REST API stub, plus the daily file drops, generated at build time
# (seeded, so every build is byte-identical: the build fails unless the output matches the committed manifest).
# The image serves the world after everything arrived: the late file of the 1.1 drill is already in the drops
# (the drill itself: scripts/late-file-drill.sh).
#   docker build -f source/Dockerfile -t pipewright-source:local .
FROM python:3.12-slim
ARG SOURCE_VERSION=1.1
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 SOURCE_QUIET=1
COPY source/bazaarbox_source /app/source/bazaarbox_source
COPY contract/source-manifest-*.json /app/contract/
WORKDIR /app/source
RUN python -m bazaarbox_source generate --out /srv/source-data --version "$SOURCE_VERSION" \
      --check "/app/contract/source-manifest-$SOURCE_VERSION.json" \
 && if [ -d /srv/source-data/late ]; then cp -r /srv/source-data/late/. /srv/source-data/drops/; fi \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin pipewright \
 && chown -R 10001:10001 /srv/source-data
USER 10001
EXPOSE 8000
HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"]
ENTRYPOINT ["python", "-m", "bazaarbox_source"]
CMD ["serve", "--data", "/srv/source-data", "--host", "0.0.0.0", "--port", "8000"]
