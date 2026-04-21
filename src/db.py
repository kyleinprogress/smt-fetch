#!/usr/bin/env python3
"""
db.py — Import SMT interval CSVs into SQLite.

Can be run standalone to import all CSVs in the data directory,
or called as a module after smt_fetch.py writes a new file.

Usage:
    uv run python db.py                  # import all CSVs not yet in DB
    uv run python db.py --file <path>    # import a specific CSV
    uv run python db.py --backfill       # re-import everything (safe, uses upsert)
"""

import argparse
import csv
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

OUTPUT_DIR = Path(os.environ.get("SMT_OUTPUT_DIR", Path(__file__).parent / "data"))
DB_PATH = Path(os.environ.get("SMT_DB_PATH", OUTPUT_DIR / "smt_energy.db"))

LOG_LEVEL = os.environ.get("SMT_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS interval_usage (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    date                TEXT    NOT NULL,           -- YYYY-MM-DD
    interval_start      TEXT    NOT NULL,           -- YYYY-MM-DDTHH:MM:SS
    interval_end        TEXT    NOT NULL,
    read_type           TEXT    NOT NULL DEFAULT 'C', -- C=consumption, G=generation
    consumption_kwh     REAL    NOT NULL,
    quality_flag        TEXT,                       -- A=actual, E=estimated
    esiid               TEXT    NOT NULL,
    raw_interval_index  INTEGER,
    imported_at         TEXT    DEFAULT (datetime('now')),
    UNIQUE(interval_start, read_type, esiid)        -- safe to re-import
);

CREATE INDEX IF NOT EXISTS idx_date       ON interval_usage(date);
CREATE INDEX IF NOT EXISTS idx_start      ON interval_usage(interval_start);
CREATE INDEX IF NOT EXISTS idx_read_type  ON interval_usage(read_type);
CREATE INDEX IF NOT EXISTS idx_esiid      ON interval_usage(esiid);

-- Handy view: daily totals
CREATE VIEW IF NOT EXISTS daily_usage AS
SELECT
    date,
    esiid,
    read_type,
    ROUND(SUM(consumption_kwh), 4)          AS total_kwh,
    COUNT(*)                                AS intervals,
    MIN(interval_start)                     AS first_interval,
    MAX(interval_end)                       AS last_interval
FROM interval_usage
GROUP BY date, esiid, read_type;

-- Handy view: hourly averages (useful for load profile analysis)
-- First sums 15-min intervals into hourly totals per day, then averages across days
CREATE VIEW IF NOT EXISTS hourly_avg AS
SELECT
    hour_of_day,
    day_type,
    read_type,
    esiid,
    ROUND(AVG(hour_kwh), 4)                AS avg_kwh,
    ROUND(MAX(hour_kwh), 4)                AS max_kwh,
    COUNT(*)                               AS sample_count
FROM (
    SELECT
        date,
        SUBSTR(interval_start, 12, 2)           AS hour_of_day,
        CASE CAST(STRFTIME('%w', date) AS INT)
            WHEN 0 THEN 'weekend'
            WHEN 6 THEN 'weekend'
            ELSE 'weekday'
        END                                     AS day_type,
        read_type,
        esiid,
        SUM(consumption_kwh)                    AS hour_kwh
    FROM interval_usage
    GROUP BY date, hour_of_day, day_type, read_type, esiid
)
GROUP BY hour_of_day, day_type, read_type, esiid
ORDER BY hour_of_day;

-- Weather data (hourly temps from Open-Meteo)
CREATE TABLE IF NOT EXISTS hourly_weather (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT    NOT NULL,           -- YYYY-MM-DD
    hour            INTEGER NOT NULL,           -- 0-23
    temperature_f   REAL    NOT NULL,
    fetched_at      TEXT    DEFAULT (datetime('now')),
    UNIQUE(date, hour)
);

CREATE INDEX IF NOT EXISTS idx_weather_date ON hourly_weather(date);

-- Energy provider contracts and rates
CREATE TABLE IF NOT EXISTS energy_provider (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT    NOT NULL,
    contract_start      TEXT    NOT NULL,           -- YYYY-MM-DD
    contract_end        TEXT,                       -- YYYY-MM-DD, NULL = current/active
    energy_charge_kwh   REAL    NOT NULL,           -- $/kWh base energy rate
    tdu_charge_kwh      REAL    NOT NULL,           -- $/kWh TDU delivery charge
    created_at          TEXT    DEFAULT (datetime('now')),
    UNIQUE(name, contract_start)
);
"""


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # safe for concurrent reads
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------

def import_csv(csv_path: Path, conn: sqlite3.Connection) -> int:
    """
    Upsert all rows from a CSV file into interval_usage.
    Returns the number of rows inserted or replaced.
    """
    inserted = 0
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        log.warning(f"Empty CSV: {csv_path}")
        return 0

    now = datetime.now(timezone.utc).isoformat()

    conn.executemany(
        """
        INSERT INTO interval_usage
            (date, interval_start, interval_end, read_type,
             consumption_kwh, quality_flag, esiid, raw_interval_index, imported_at)
        VALUES
            (:date, :interval_start, :interval_end, :read_type,
             :consumption_kwh, :quality_flag, :esiid, :raw_interval_index, :imported_at)
        ON CONFLICT(interval_start, read_type, esiid)
        DO UPDATE SET
            consumption_kwh    = excluded.consumption_kwh,
            quality_flag       = excluded.quality_flag,
            raw_interval_index = excluded.raw_interval_index,
            imported_at        = excluded.imported_at
        """,
        [{**row, "imported_at": now} for row in rows],
    )
    conn.commit()
    inserted = len(rows)
    log.info(f"Imported {inserted} rows from {csv_path.name}")
    return inserted


def import_all(conn: sqlite3.Connection, force: bool = False) -> int:
    """
    Import all CSVs in OUTPUT_DIR.
    If force=False, skips dates that already have >= 96 rows in the DB.
    """
    csv_files = sorted(OUTPUT_DIR.glob("smt_interval_*.csv"))
    if not csv_files:
        log.warning(f"No CSV files found in {OUTPUT_DIR}")
        return 0

    total = 0
    for csv_path in csv_files:
        # Extract date from filename: smt_interval_YYYY-MM-DD.csv
        date_str = csv_path.stem.replace("smt_interval_", "")

        if not force:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM interval_usage WHERE date = ?",
                (date_str,),
            ).fetchone()
            if row["cnt"] >= 96:
                log.debug(f"Skipping {date_str} — already in DB ({row['cnt']} rows)")
                continue

        total += import_csv(csv_path, conn)

    log.info(f"Total rows imported: {total}")
    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def seed_providers(conn: sqlite3.Connection) -> None:
    """Seed default provider if table is empty."""
    row = conn.execute("SELECT COUNT(*) as cnt FROM energy_provider").fetchone()
    if row["cnt"] == 0:
        conn.execute("""
            INSERT INTO energy_provider (name, contract_start, energy_charge_kwh, tdu_charge_kwh)
            VALUES ('Abundance Energy', '2026-01-26', 0.0717, 0.0558330)
        """)
        conn.commit()
        log.info("Seeded default provider: Abundance Energy")


def add_provider(conn: sqlite3.Connection, name: str, start: str,
                 energy_rate: float, tdu_rate: float, end: str | None = None) -> None:
    conn.execute("""
        INSERT INTO energy_provider (name, contract_start, contract_end, energy_charge_kwh, tdu_charge_kwh)
        VALUES (?, ?, ?, ?, ?)
    """, (name, start, end, energy_rate, tdu_rate))
    conn.commit()
    total = energy_rate + tdu_rate
    log.info(f"Added provider: {name} (start={start}, rate=${total:.4f}/kWh)")


def list_providers(conn: sqlite3.Connection) -> None:
    rows = conn.execute("""
        SELECT name, contract_start, contract_end, energy_charge_kwh, tdu_charge_kwh
        FROM energy_provider ORDER BY contract_start
    """).fetchall()
    if not rows:
        print("No providers configured.")
        return
    print(f"\n── Energy Providers {'─'*40}")
    for r in rows:
        total = r["energy_charge_kwh"] + r["tdu_charge_kwh"]
        end = r["contract_end"] or "active"
        print(f"  {r['name']}")
        print(f"    Contract:    {r['contract_start']} → {end}")
        print(f"    Energy rate: ${r['energy_charge_kwh']:.4f}/kWh")
        print(f"    TDU rate:    ${r['tdu_charge_kwh']:.4f}/kWh")
        print(f"    Total rate:  ${total:.4f}/kWh")
        print()


def main():
    parser = argparse.ArgumentParser(description="Import SMT interval CSVs to SQLite")
    parser.add_argument("--file", type=Path, help="Import a specific CSV file")
    parser.add_argument("--backfill", action="store_true",
                        help="Re-import all CSVs regardless of what's in the DB")
    parser.add_argument("--stats", action="store_true",
                        help="Print summary stats after import")

    # Provider management
    parser.add_argument("--add-provider", action="store_true",
                        help="Add a new energy provider")
    parser.add_argument("--list-providers", action="store_true",
                        help="List all energy providers")
    parser.add_argument("--name", type=str, help="Provider name")
    parser.add_argument("--start", type=str, help="Contract start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="Contract end date (YYYY-MM-DD)")
    parser.add_argument("--energy-rate", type=float, help="Energy charge per kWh")
    parser.add_argument("--tdu-rate", type=float, help="TDU delivery charge per kWh")

    args = parser.parse_args()

    conn = get_connection()
    init_db(conn)
    seed_providers(conn)

    if args.list_providers:
        list_providers(conn)
        conn.close()
        return

    if args.add_provider:
        if not all([args.name, args.start, args.energy_rate is not None, args.tdu_rate is not None]):
            parser.error("--add-provider requires --name, --start, --energy-rate, and --tdu-rate")
        add_provider(conn, args.name, args.start, args.energy_rate, args.tdu_rate, args.end)
        conn.close()
        return

    if args.file:
        import_csv(args.file, conn)
    elif not args.stats:
        import_all(conn, force=args.backfill)

    if args.stats:
        print_stats(conn)

    conn.close()


def print_stats(conn: sqlite3.Connection) -> None:
    row = conn.execute("""
        SELECT
            COUNT(DISTINCT date)        AS days,
            COUNT(*)                    AS total_intervals,
            MIN(date)                   AS earliest,
            MAX(date)                   AS latest,
            ROUND(SUM(consumption_kwh), 2) AS total_kwh
        FROM interval_usage
        WHERE read_type = 'C'
    """).fetchone()
    print(f"\n── SMT Energy Database ({'─'*30}")
    print(f"  Days in DB:        {row['days']}")
    print(f"  Date range:        {row['earliest']} → {row['latest']}")
    print(f"  Total intervals:   {row['total_intervals']}")
    print(f"  Total consumption: {row['total_kwh']} kWh")
    print()


if __name__ == "__main__":
    main()