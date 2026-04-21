FROM python:3.14-slim

# supercronic — drop-in cron for containers, no init system needed
# Pin to a specific release for reproducibility
ARG SUPERCRONIC_VERSION=0.2.44
ARG SUPERCRONIC_SHA1=6eb0a8e1e6673675dc67668c1a9b6409f79c37bc

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        libssl-dev \
        gosu \
    && curl -fsSL \
        "https://github.com/aptible/supercronic/releases/download/v${SUPERCRONIC_VERSION}/supercronic-linux-amd64" \
        -o /usr/local/bin/supercronic \
    && echo "${SUPERCRONIC_SHA1}  /usr/local/bin/supercronic" | sha1sum -c - \
    && chmod +x /usr/local/bin/supercronic \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install uv for dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install Python deps first (better layer caching)
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --no-install-project --frozen

# Copy app files
COPY src/ ./
RUN chmod +x run.sh entrypoint.sh

# /data is the persistent volume mount point
# CSVs go to /data/csv, SQLite to /data/smt_energy.db, logs to /data/logs
ARG APP_VERSION=dev
ENV APP_VERSION=${APP_VERSION}
ENV SMT_OUTPUT_DIR=/data/csv
ENV SMT_DB_PATH=/data/smt_energy.db
ENV PATH="/app/.venv/bin:$PATH"

VOLUME ["/data"]

EXPOSE 8080

CMD ["/app/entrypoint.sh"]
