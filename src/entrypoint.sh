#!/bin/sh
# entrypoint.sh — container startup: initial fetch, then run dashboard + cron
set -e

# Ensure data directories exist
mkdir -p /data/csv /data/logs

# Run initial fetch + import + weather on startup
echo "=== $(date '+%Y-%m-%d %H:%M:%S') Running initial data sync ==="
/app/run.sh || echo "Initial sync had errors (non-fatal, continuing)"

# Start dashboard in background, restart if it crashes
echo "=== $(date '+%Y-%m-%d %H:%M:%S') Starting dashboard on port ${SMT_DASH_PORT:-8080} ==="
while true; do
    python /app/dashboard.py
    echo "=== $(date '+%Y-%m-%d %H:%M:%S') Dashboard exited, restarting in 5s ==="
    sleep 5
done &

# Run supercronic in foreground (PID 1 signal handler)
echo "=== $(date '+%Y-%m-%d %H:%M:%S') Starting supercronic ==="
exec supercronic /app/crontab
