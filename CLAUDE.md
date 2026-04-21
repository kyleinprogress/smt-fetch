# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**smt-fetch** is a daily downloader for Smart Meter Texas (SMT) 15-minute interval electricity usage data. It fetches consumption (C) and generation (G) data from SMT's API, saves daily CSVs (96 rows per day, one per 15-min interval), and imports them into SQLite for analysis. Includes a web dashboard for visualizing usage, costs, weather correlation, and battery sizing analysis.

## Commands

```bash
# Setup (local dev)
uv sync                    # create .venv and install dependencies
cp .env.example .env       # then fill in credentials and coordinates

# Fetch data
uv run python src/smt_fetch.py                                          # fetch yesterday
SMT_DAYS_BACK=2 SMT_FORCE_REFETCH=true uv run python src/smt_fetch.py  # force re-fetch 2 days back

# Database
uv run python src/db.py                   # import new CSVs to SQLite
uv run python src/db.py --backfill        # re-import all CSVs (upsert-safe)
uv run python src/db.py --stats           # print database summary
uv run python src/db.py --list-providers  # show energy providers and rates
uv run python src/db.py --add-provider --name "Provider" --start 2026-01-01 --energy-rate 0.07 --tdu-rate 0.05

# Weather
uv run python src/weather.py              # backfill weather for all dates missing data

# Dashboard
uv run python src/dashboard.py            # serves at http://localhost:8080

# Docker
docker compose up --build                 # runs fetch+import+weather on start, then daily via supercronic
```

## Project Structure

All application code lives in `src/` (copied into `/app` in the Docker image):

- **`smt_fetch.py`** — Async script that authenticates via `smart_meter_texas` package, calls the `/adhoc/intervalsynch` endpoint directly, parses the response into flat rows, and writes one CSV per day.
- **`db.py`** — SQLite import utility with upsert logic. Creates `interval_usage`, `hourly_weather`, and `energy_provider` tables plus `daily_usage` and `hourly_avg` views. Also provides CLI for managing energy providers.
- **`weather.py`** — Fetches hourly temperature data from Open-Meteo (free, no API key). Backfills automatically for dates with energy data but no weather data.
- **`dashboard.py`** — aiohttp.web server exposing JSON API endpoints over SQLite. Serves the dashboard at `/`.
- **`dashboard.html`** — Self-contained SPA with Chart.js. Light/dark theme via `prefers-color-scheme`. Desktop-optimized layout.
- **`run.sh`** — Runs smt_fetch.py, db.py, weather.py sequentially. Used by cron.
- **`entrypoint.sh`** — Docker container startup: creates data dirs, runs initial sync, starts dashboard (with auto-restart), then runs supercronic in foreground.

## Environment Variables

Required: `SMT_USERNAME`, `SMT_PASSWORD`, `SMT_ESIID`, `SMT_LATITUDE`, `SMT_LONGITUDE`

Optional: `SMT_OUTPUT_DIR` (default: `./data`), `SMT_DB_PATH` (default: `<output_dir>/smt_energy.db`), `SMT_DAYS_BACK` (default: `1`), `SMT_FORCE_REFETCH`, `SMT_LOG_LEVEL` (default: `INFO`), `SMT_DASH_PORT` (default: `8080`), `SMT_FREE_START` (default: `21`), `SMT_FREE_END` (default: `6`)

## Key Design Details

- The SMT API expects dates as `MM/DD/YYYY`; CSVs and SQLite use ISO format (`YYYY-MM-DD`).
- Energy values in the API response may have quality flag suffixes like `-E` (estimated) or `-A` (actual), which are stripped from the kWh value and stored separately.
- TOU windows (`SMT_FREE_START`/`SMT_FREE_END`) are for scenario analysis ("what if I switch to a TOU plan?"), not tied to the provider. Providers just track flat-rate billing.
- Weather data uses `timezone=America/Chicago` to return local time, not UTC.
- No test suite or linter is configured.
