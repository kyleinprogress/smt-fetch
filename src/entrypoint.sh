#!/bin/sh
# entrypoint.sh — container startup: initial fetch, then run dashboard + cron
set -e

# ---------------------------------------------------------------------------
# PUID/PGID support (linuxserver.io convention)
# ---------------------------------------------------------------------------
PUID=${PUID:-1000}
PGID=${PGID:-1000}

# Create group and user if not root
if [ "$PUID" != "0" ]; then
    groupadd -o -g "$PGID" appuser 2>/dev/null || true
    useradd -o -u "$PUID" -g "$PGID" -d /app -s /bin/sh appuser 2>/dev/null || true
    chown -R "$PUID:$PGID" /data /app
    echo "Running as UID=$PUID GID=$PGID"
    EXEC_CMD="gosu appuser"
else
    echo "Running as root"
    EXEC_CMD=""
fi

# Ensure data directories exist
mkdir -p /data/csv /data/logs
chown -R "$PUID:$PGID" /data

# Run initial fetch + import + weather on startup
echo "=== $(date '+%Y-%m-%d %H:%M:%S') Running initial data sync ==="
$EXEC_CMD /app/run.sh || echo "Initial sync had errors (non-fatal, continuing)"

# Start dashboard in background, restart if it crashes
echo "=== $(date '+%Y-%m-%d %H:%M:%S') Starting dashboard on port ${SMT_DASH_PORT:-8080} ==="
while true; do
    $EXEC_CMD python /app/dashboard.py
    echo "=== $(date '+%Y-%m-%d %H:%M:%S') Dashboard exited, restarting in 5s ==="
    sleep 5
done &

# Run supercronic in foreground (PID 1 signal handler)
echo "=== $(date '+%Y-%m-%d %H:%M:%S') Starting supercronic ==="
exec supercronic /app/crontab
