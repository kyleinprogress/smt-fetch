#!/usr/bin/env python3
"""
weather.py — Fetch hourly weather data from Open-Meteo for dates with energy data.
Backfills missing weather data automatically. Idempotent — skips dates already fetched.

Usage:
    uv run python weather.py                    # backfill all missing dates
    uv run python weather.py --date 2026-03-15  # fetch a specific date
"""

import argparse
import asyncio
import logging
import os

import aiohttp
from dotenv import load_dotenv

from db import get_connection, init_db

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

LATITUDE = os.environ["SMT_LATITUDE"]
LONGITUDE = os.environ["SMT_LONGITUDE"]

LOG_LEVEL = os.environ.get("SMT_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
)
log = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

def get_dates_needing_weather(conn) -> list[str]:
    """Find dates with energy data but incomplete weather data."""
    rows = conn.execute("""
        SELECT DISTINCT iu.date FROM interval_usage iu
        WHERE iu.date NOT IN (
            SELECT date FROM hourly_weather GROUP BY date HAVING COUNT(*) >= 24
        )
        ORDER BY iu.date
    """).fetchall()
    return [r["date"] for r in rows]


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def _parse_response(data: dict, requested_dates: set[str]) -> list[dict]:
    """Parse Open-Meteo response into records, filtered to requested dates."""
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    records = []
    for ts, temp in zip(times, temps):
        if temp is None:
            continue
        # ts format: "2026-02-12T00:00"
        date_str = ts[:10]
        if date_str not in requested_dates:
            continue
        hour = int(ts[11:13])
        records.append({"date": date_str, "hour": hour, "temperature_f": temp})
    return records


async def fetch_weather(session: aiohttp.ClientSession, dates: list[str]) -> list[dict]:
    """Fetch hourly temps from Open-Meteo for the given dates."""
    start_date = min(dates)
    end_date = max(dates)
    requested = set(dates)
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m",
        "temperature_unit": "fahrenheit",
        "timezone": "America/Chicago",
    }

    log.info(f"Fetching weather from Open-Meteo ({start_date} to {end_date})…")

    # Try archive API first
    async with session.get(ARCHIVE_URL, params=params) as resp:
        if resp.status == 200:
            data = await resp.json()
            records = _parse_response(data, requested)
            if records:
                return records
            log.info("Archive returned no data, trying forecast API…")
        else:
            log.warning(f"Archive API returned {resp.status}, trying forecast API…")

    # Fallback to forecast API for recent dates
    params["past_days"] = 7
    async with session.get(FORECAST_URL, params=params) as resp:
        if resp.status != 200:
            log.warning(f"Forecast API returned {resp.status}")
            return []
        data = await resp.json()
        return _parse_response(data, requested)


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def save_weather(conn, records: list[dict]) -> int:
    """Upsert weather records into hourly_weather table."""
    conn.executemany("""
        INSERT INTO hourly_weather (date, hour, temperature_f)
        VALUES (:date, :hour, :temperature_f)
        ON CONFLICT(date, hour) DO UPDATE SET
            temperature_f = excluded.temperature_f,
            fetched_at = datetime('now')
    """, records)
    conn.commit()
    return len(records)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(specific_date: str | None = None) -> None:
    conn = get_connection()
    init_db(conn)

    if specific_date:
        dates = [specific_date]
    else:
        dates = get_dates_needing_weather(conn)
        if not dates:
            log.info("Weather data up to date.")
            conn.close()
            return

    log.info(f"Need weather data for {len(dates)} date(s).")

    async with aiohttp.ClientSession() as session:
        records = await fetch_weather(session, dates)

    if not records:
        log.warning("No weather data returned.")
        conn.close()
        return

    count = save_weather(conn, records)
    unique_dates = len({r["date"] for r in records})
    log.info(f"Saved {count} hourly records across {unique_dates} date(s).")
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Fetch weather data from Open-Meteo")
    parser.add_argument("--date", type=str, help="Fetch a specific date (YYYY-MM-DD)")
    args = parser.parse_args()
    asyncio.run(run(specific_date=args.date))


if __name__ == "__main__":
    main()
