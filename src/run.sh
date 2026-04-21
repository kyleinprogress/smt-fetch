#!/bin/sh
# run.sh — daily job: fetch yesterday's interval data, then import to SQLite
set -e

cd /app

echo "=== $(date '+%Y-%m-%d %H:%M:%S') Starting SMT fetch ==="
python smt_fetch.py

echo "=== $(date '+%Y-%m-%d %H:%M:%S') Importing to SQLite ==="
python db.py

echo "=== $(date '+%Y-%m-%d %H:%M:%S') Fetching weather data ==="
python weather.py

echo "=== $(date '+%Y-%m-%d %H:%M:%S') Done ==="