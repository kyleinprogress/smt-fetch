#!/usr/bin/env python3
"""
smt_fetch.py — Smart Meter Texas 15-minute interval data downloader
Intended to run as a daily cron job, fetching the previous day's usage.

Uses the smart_meter_texas PyPI package for auth + SSL context setup,
then calls the /adhoc/intervalsynch endpoint directly to get all interval
data (both consumption and generation) rather than just the generation
subset returned by Meter.get_15min().
"""

import asyncio
import csv
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from smart_meter_texas import Account, Client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

USERNAME = os.environ["SMT_USERNAME"]
PASSWORD = os.environ["SMT_PASSWORD"]
ESIID = os.environ["SMT_ESIID"]

OUTPUT_DIR = Path(os.environ.get("SMT_OUTPUT_DIR", Path(__file__).parent / "data"))
DAYS_BACK = int(os.environ.get("SMT_DAYS_BACK", "1"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL = os.environ.get("SMT_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

async def fetch_intervals(target_date: date) -> list[dict]:
    """
    Authenticate via the smart_meter_texas package (handles SMT's custom SSL
    cert context), then call /adhoc/intervalsynch directly to get all 15-min
    interval records for the given date.
    """
    # SMT's interval endpoint expects MM/DD/YYYY
    date_str = target_date.strftime("%m/%d/%Y")
    log.info("Authenticating to Smart Meter Texas…")

    account = Account(USERNAME, PASSWORD)

    async with aiohttp.ClientSession() as websession:
        # Pass None for ssl_context — Client._init_ssl_context() will build
        # the correct context (fetches the SMT intermediate CA cert, etc.)
        client = Client(websession, account, None)
        await client.authenticate()
        log.info("Authentication successful.")

        log.info(f"Fetching 15-min interval data for {date_str}…")

        json_response = await client.request(
            "/adhoc/intervalsynch",
            json={
                "startDate": date_str,
                "endDate": date_str,
                "reportFormat": "JSON",
                "ESIID": [ESIID],
                "versionDate": None,
                "readDate": None,
                "versionNum": None,
                "dataType": None,
            },
        )

    log.debug(f"Raw response: {str(json_response)[:600]}")

    try:
        energy_data = json_response["data"]["energyData"]
    except (KeyError, TypeError):
        error_code = json_response.get("data", {}).get("errorCode")
        error_msg = json_response.get("data", {}).get("errorMessage", "")
        if error_code == "1" and "TDSP" in error_msg:
            log.warning(
                f"Data not yet available from TDSP for {date_str}. "
                "Try again later or increase SMT_DAYS_BACK."
            )
        else:
            log.warning(f"Unexpected response structure: {json_response}")
        return []

    rows = parse_energy_data(energy_data, target_date)
    log.info(f"Parsed {len(rows)} interval records for {target_date.isoformat()}.")
    return rows


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_energy_data(energy_data: list, target_date: date) -> list[dict]:
    """
    Parse the energyData list from the intervalsynch response into flat rows.

    Each entry in energyData has:
      - RT: read type ("C" = consumption, "G" = generation)
      - RD: comma-separated kWh values for each 15-min slot, e.g. "0.123,0.456,..."
            Values may be suffixed with "-E" (estimated) or "-A" (actual).

    We produce one row per 15-min interval per read type.
    96 intervals per day (00:00–23:45 in 15-min steps).
    """
    rows = []
    date_str = target_date.isoformat()
    slots_per_day = 96  # 24h * 4

    for entry in energy_data:
        rt = entry.get("RT", "C")           # C = consumption, G = generation
        rd = entry.get("RD", "")

        values = [v for v in rd.split(",") if v]

        for idx, raw_val in enumerate(values):
            if idx >= slots_per_day:
                break

            # Strip quality flag suffix (-E estimated, -A actual, etc.)
            kwh_str = raw_val.split("-")[0]
            try:
                kwh = float(kwh_str)
            except ValueError:
                kwh = 0.0

            # Calculate interval times from slot index
            total_minutes = idx * 15
            hh = total_minutes // 60
            mm = total_minutes % 60
            interval_start = f"{date_str}T{hh:02d}:{mm:02d}:00"

            end_minutes = (idx + 1) * 15
            e_hh = end_minutes // 60
            e_mm = end_minutes % 60
            interval_end = f"{date_str}T{e_hh:02d}:{e_mm:02d}:00"

            rows.append({
                "date": date_str,
                "interval_start": interval_start,
                "interval_end": interval_end,
                "read_type": rt,
                "consumption_kwh": kwh,
                "quality_flag": raw_val.split("-")[1] if "-" in raw_val else "",
                "esiid": ESIID,
                "raw_interval_index": idx,
            })

    return rows


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

FIELDNAMES = [
    "date",
    "interval_start",
    "interval_end",
    "read_type",          # C = consumption, G = generation
    "consumption_kwh",
    "quality_flag",       # A = actual, E = estimated (blank if absent)
    "esiid",
    "raw_interval_index",
]


def save_csv(rows: list[dict], target_date: date) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filename = OUTPUT_DIR / f"smt_interval_{target_date.isoformat()}.csv"
    file_existed = filename.exists()

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    action = "Overwrote" if file_existed else "Wrote"
    log.info(f"{action} {len(rows)} rows → {filename}")
    return filename


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(days_back: int = DAYS_BACK) -> None:
    force = os.environ.get("SMT_FORCE_REFETCH", "").lower() in ("1", "true", "yes")
    failed_dates = []
    succeeded = 0

    for offset in range(days_back, 0, -1):
        target_date = date.today() - timedelta(days=offset)

        existing = OUTPUT_DIR / f"smt_interval_{target_date.isoformat()}.csv"
        if existing.exists() and not force:
            with open(existing) as f:
                row_count = sum(1 for _ in f) - 1
            if row_count >= 50:
                log.info(
                    f"Data for {target_date} already exists ({row_count} rows). "
                    "Set SMT_FORCE_REFETCH=true to overwrite."
                )
                continue

        if offset < days_back:
            await asyncio.sleep(2)

        rows = await fetch_intervals(target_date)

        if not rows:
            log.warning(f"No data returned for {target_date}.")
            failed_dates.append(target_date)
            continue

        save_csv(rows, target_date)
        succeeded += 1

    if failed_dates:
        log.warning(f"Failed to fetch data for: {', '.join(d.isoformat() for d in failed_dates)}")
        if not succeeded:
            sys.exit(1)

    log.info("Done.")


if __name__ == "__main__":
    asyncio.run(run())