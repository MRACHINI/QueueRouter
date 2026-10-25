# Pinned by digest (Dependabot updates it): the image that was tested and scanned is the one that ships.
FROM python:3.14-slim@sha256:35caba83af2fe6da9a5d922f9b0156f21d1246a48278c58a08bd253303ba42e8

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin queuerouter \
    && mkdir /data && chown 10001:10001 /data && chmod 700 /data

# Database, secret key and backups. Mount a volume here.
ENV DATA_DIR=/data
VOLUME /data

WORKDIR /srv
COPY requirements.txt .
# pip isn't needed at runtime; removing it also drops its own bundled (often older) certifi copy.
# Every package, dependencies included, is pinned with its hashes (requirements.in lists the direct ones).
RUN pip install --require-hashes -r requirements.txt && pip uninstall -y pip
COPY app ./app
COPY docker/entrypoint.sh /usr/local/bin/queuerouter-entrypoint
RUN chmod 755 /usr/local/bin/queuerouter-entrypoint

# Starts as root only to fix /data ownership, then drops to PUID:PGID (default 10001) with no
# capabilities before the app runs. Run with `user:` to skip that step entirely.
ENV PUID=10001 PGID=10001
EXPOSE 8000

# While starting, check every 2 s, so a reverse proxy that waits for "healthy" (Traefik) switches
# over within seconds; after that every 30 s.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --start-interval=2s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3)"]

# One process, one worker: the queue and slot counts live in memory.
ENTRYPOINT ["queuerouter-entrypoint"]
CMD ["uvicorn", "app.main:build_app", "--factory", "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "1", "--no-server-header", "--no-access-log", "--timeout-graceful-shutdown", "50"]
