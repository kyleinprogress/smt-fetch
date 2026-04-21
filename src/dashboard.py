#!/usr/bin/env python3
"""
dashboard.py — aiohttp web server for SMT energy data dashboard.

Usage:
    uv run python dashboard.py
"""

import importlib.metadata
import logging
import os
from pathlib import Path

from aiohttp import web
from dotenv import load_dotenv

import asyncio
from db import get_connection
from report import generate_report

load_dotenv()

LOG_LEVEL = os.environ.get("SMT_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
)
log = logging.getLogger(__name__)

HERE = Path(__file__).parent

DASH_HOST = os.environ.get("SMT_DASH_HOST", "0.0.0.0")
DASH_PORT = int(os.environ.get("SMT_DASH_PORT", "8080"))
FREE_START = int(os.environ.get("SMT_FREE_START", "21"))
FREE_END = int(os.environ.get("SMT_FREE_END", "6"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rows_to_dicts(rows):
    return [dict(r) for r in rows]


def json_response(data):
    return web.json_response(data)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

def _get_version():
    # Prefer APP_VERSION env var (set by Docker build arg from CI)
    version = os.environ.get("APP_VERSION")
    if version and version != "dev":
        return version
    try:
        return importlib.metadata.version("smt-fetch")
    except importlib.metadata.PackageNotFoundError:
        return "dev"


async def index(request):
    html = (HERE / "dashboard.html").read_text()
    html = html.replace("{{VERSION}}", f"v{_get_version()}")
    return web.Response(
        text=html,
        content_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


async def api_summary(request):
    conn = get_connection()
    try:
        totals = conn.execute("""
            SELECT
                COUNT(DISTINCT date)                    AS days_collected,
                MIN(date)                               AS date_min,
                MAX(date)                               AS date_max,
                ROUND(SUM(consumption_kwh), 2)          AS total_kwh,
                ROUND(MAX(consumption_kwh), 4)          AS peak_interval_kwh
            FROM interval_usage
            WHERE read_type = 'C'
        """).fetchone()

        peak = conn.execute("""
            SELECT interval_start
            FROM interval_usage
            WHERE read_type = 'C'
            ORDER BY consumption_kwh DESC
            LIMIT 1
        """).fetchone()

        daily = conn.execute("""
            SELECT
                ROUND(AVG(total_kwh), 2) AS avg_daily_kwh,
                ROUND(MIN(total_kwh), 2) AS min_daily_kwh,
                ROUND(MAX(total_kwh), 2) AS max_daily_kwh
            FROM daily_usage
            WHERE read_type = 'C'
        """).fetchone()

        return json_response({
            "days_collected": totals["days_collected"],
            "date_range": [totals["date_min"], totals["date_max"]],
            "total_kwh": totals["total_kwh"],
            "avg_daily_kwh": daily["avg_daily_kwh"],
            "min_daily_kwh": daily["min_daily_kwh"],
            "max_daily_kwh": daily["max_daily_kwh"],
            "peak_interval_kwh": totals["peak_interval_kwh"],
            "peak_interval_time": peak["interval_start"] if peak else None,
        })
    finally:
        conn.close()


async def api_daily(request):
    date_from = request.query.get("from")
    date_to = request.query.get("to")

    conn = get_connection()
    try:
        sql = "SELECT date, total_kwh, intervals FROM daily_usage WHERE read_type = 'C'"
        params = {}
        if date_from:
            sql += " AND date >= :from_date"
            params["from_date"] = date_from
        if date_to:
            sql += " AND date <= :to_date"
            params["to_date"] = date_to
        sql += " ORDER BY date"

        rows = conn.execute(sql, params).fetchall()
        return json_response(rows_to_dicts(rows))
    finally:
        conn.close()


async def api_hourly_profile(request):
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT hour_of_day, day_type, avg_kwh, max_kwh, sample_count
            FROM hourly_avg
            WHERE read_type = 'C'
            ORDER BY hour_of_day, day_type
        """).fetchall()
        return json_response(rows_to_dicts(rows))
    finally:
        conn.close()


async def api_battery_analysis(request):
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                date,
                ROUND(SUM(CASE
                    WHEN CAST(SUBSTR(interval_start, 12, 2) AS INT) >= :free_start
                      OR CAST(SUBSTR(interval_start, 12, 2) AS INT) < :free_end
                    THEN consumption_kwh ELSE 0 END), 4) AS free_kwh,
                ROUND(SUM(CASE
                    WHEN CAST(SUBSTR(interval_start, 12, 2) AS INT) >= :free_end
                     AND CAST(SUBSTR(interval_start, 12, 2) AS INT) < :free_start
                    THEN consumption_kwh ELSE 0 END), 4) AS paid_kwh
            FROM interval_usage
            WHERE read_type = 'C'
            GROUP BY date
            ORDER BY date
        """, {"free_start": FREE_START, "free_end": FREE_END}).fetchall()

        daily = rows_to_dicts(rows)
        paid_values = [d["paid_kwh"] for d in daily]
        free_values = [d["free_kwh"] for d in daily]

        if paid_values:
            avg_paid = round(sum(paid_values) / len(paid_values), 1)
            max_paid = round(max(paid_values), 1)
            sorted_paid = sorted(paid_values)
            p95_paid = round(sorted_paid[int(len(sorted_paid) * 0.95)], 1)
        else:
            avg_paid = max_paid = p95_paid = 0.0

        avg_free = round(sum(free_values) / len(free_values), 1) if free_values else 0.0

        return json_response({
            "free_window": {"start": FREE_START, "end": FREE_END},
            "paid_window": {"start": FREE_END, "end": FREE_START},
            "avg_paid_kwh": avg_paid,
            "max_paid_kwh": max_paid,
            "p95_paid_kwh": p95_paid,
            "avg_free_kwh": avg_free,
            "daily_breakdown": daily,
        })
    finally:
        conn.close()


async def api_intervals(request):
    date = request.query.get("date")
    if not date:
        return web.json_response({"error": "date query parameter required"}, status=400)

    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT interval_start, interval_end, consumption_kwh,
                   quality_flag, raw_interval_index
            FROM interval_usage
            WHERE read_type = 'C' AND date = :date
            ORDER BY raw_interval_index
        """, {"date": date}).fetchall()
        return json_response(rows_to_dicts(rows))
    finally:
        conn.close()


async def api_cost_daily(request):
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                du.date,
                du.total_kwh,
                ep.name AS provider_name,
                ep.energy_charge_kwh,
                ep.tdu_charge_kwh,
                ROUND(ep.energy_charge_kwh + ep.tdu_charge_kwh, 6) AS total_rate,
                ROUND(du.total_kwh * (ep.energy_charge_kwh + ep.tdu_charge_kwh), 2) AS daily_cost
            FROM daily_usage du
            LEFT JOIN energy_provider ep
                ON du.date >= ep.contract_start
                AND (ep.contract_end IS NULL OR du.date <= ep.contract_end)
            WHERE du.read_type = 'C'
            ORDER BY du.date
        """).fetchall()
        return json_response(rows_to_dicts(rows))
    finally:
        conn.close()


async def api_cost_summary(request):
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT
                du.total_kwh,
                ROUND(du.total_kwh * (ep.energy_charge_kwh + ep.tdu_charge_kwh), 2) AS daily_cost,
                ep.name AS provider_name,
                ep.energy_charge_kwh,
                ep.tdu_charge_kwh
            FROM daily_usage du
            LEFT JOIN energy_provider ep
                ON du.date >= ep.contract_start
                AND (ep.contract_end IS NULL OR du.date <= ep.contract_end)
            WHERE du.read_type = 'C'
            ORDER BY du.date
        """).fetchall()

        if not rows:
            return json_response({"total_cost": 0, "avg_daily_cost": 0})

        costs = [r["daily_cost"] or 0 for r in rows]
        current = rows[-1]

        return json_response({
            "total_cost": round(sum(costs), 2),
            "avg_daily_cost": round(sum(costs) / len(costs), 2),
            "min_daily_cost": round(min(costs), 2),
            "max_daily_cost": round(max(costs), 2),
            "current_provider": current["provider_name"],
            "current_rate": round((current["energy_charge_kwh"] or 0) + (current["tdu_charge_kwh"] or 0), 4),
        })
    finally:
        conn.close()


async def api_weather_daily(request):
    date_from = request.query.get("from")
    date_to = request.query.get("to")

    conn = get_connection()
    try:
        sql = """
            SELECT date,
                   ROUND(MIN(temperature_f), 1) AS low_f,
                   ROUND(MAX(temperature_f), 1) AS high_f,
                   ROUND(AVG(temperature_f), 1) AS avg_f
            FROM hourly_weather
        """
        params = {}
        wheres = []
        if date_from:
            wheres.append("date >= :from_date")
            params["from_date"] = date_from
        if date_to:
            wheres.append("date <= :to_date")
            params["to_date"] = date_to
        if wheres:
            sql += " WHERE " + " AND ".join(wheres)
        sql += " GROUP BY date ORDER BY date"

        rows = conn.execute(sql, params).fetchall()
        return json_response(rows_to_dicts(rows))
    finally:
        conn.close()


async def api_weather_hourly(request):
    date = request.query.get("date")
    if not date:
        return web.json_response({"error": "date query parameter required"}, status=400)

    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT date, hour, temperature_f
            FROM hourly_weather
            WHERE date = :date
            ORDER BY hour
        """, {"date": date}).fetchall()
        return json_response(rows_to_dicts(rows))
    finally:
        conn.close()


async def api_report(request):
    loop = asyncio.get_event_loop()
    pdf_bytes = await loop.run_in_executor(None, generate_report)
    return web.Response(
        body=pdf_bytes,
        content_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="energy_report.pdf"'},
    )


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

def create_app():
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/api/summary", api_summary)
    app.router.add_get("/api/daily", api_daily)
    app.router.add_get("/api/hourly-profile", api_hourly_profile)
    app.router.add_get("/api/battery-analysis", api_battery_analysis)
    app.router.add_get("/api/intervals", api_intervals)
    app.router.add_get("/api/cost/daily", api_cost_daily)
    app.router.add_get("/api/cost/summary", api_cost_summary)
    app.router.add_get("/api/weather/daily", api_weather_daily)
    app.router.add_get("/api/weather/hourly", api_weather_hourly)
    app.router.add_get("/api/report", api_report)
    return app


if __name__ == "__main__":
    log.info(f"Starting dashboard on {DASH_HOST}:{DASH_PORT}")
    web.run_app(create_app(), host=DASH_HOST, port=DASH_PORT)
