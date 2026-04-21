FROM python:3.12-slim

# supercronic — drop-in cron for containers, no init system needed
# Pin to a specific release for reproducibility
ARG SUPERCRONIC_VERSION=0.2.29
ARG SUPERCRONIC_SHA1=cd48d45c4b10f3f0bfdd3a57d054cd05ac96812b

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        libssl-dev \
    && curl -fsSL \
        "https://github.com/aptible/supercronic/releases/download/v${SUPERCRONIC_VERSION}/supercronic-linux-amd64" \
        -o /usr/local/bin/supercronic \
    && echo "${SUPERCRONIC_SHA1}  /usr/local/bin/supercronic" | sha1sum -c - \
    && chmod +x /usr/local/bin/supercronic \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (better layer caching)
COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY src/ ./
RUN chmod +x run.sh entrypoint.sh

# /data is the persistent volume mount point
# CSVs go to /data/csv, SQLite to /data/smt_energy.db, logs to /data/logs
ENV SMT_OUTPUT_DIR=/data/csv
ENV SMT_DB_PATH=/data/smt_energy.db

VOLUME ["/data"]

EXPOSE 8080

CMD ["/app/entrypoint.sh"]