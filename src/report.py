#!/usr/bin/env python3
"""
report.py — Generate a PDF energy usage report from SMT data.

Usage:
    uv run python report.py                     # writes energy_report.pdf
    uv run python report.py -o custom_name.pdf  # custom output path
"""

import argparse
import io
import logging
import os
from datetime import date, datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Image,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from db import get_connection
from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.environ.get("SMT_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data queries
# ---------------------------------------------------------------------------

def fetch_summary(conn) -> dict:
    totals = conn.execute("""
        SELECT
            COUNT(DISTINCT date)                    AS days_collected,
            MIN(date)                               AS date_min,
            MAX(date)                               AS date_max,
            ROUND(SUM(consumption_kwh), 2)          AS total_kwh
        FROM interval_usage
        WHERE read_type = 'C'
    """).fetchone()

    daily = conn.execute("""
        SELECT
            ROUND(AVG(total_kwh), 2) AS avg_daily_kwh,
            ROUND(MIN(total_kwh), 2) AS min_daily_kwh,
            ROUND(MAX(total_kwh), 2) AS max_daily_kwh
        FROM daily_usage
        WHERE read_type = 'C'
    """).fetchone()

    return {
        "days_collected": totals["days_collected"],
        "date_min": totals["date_min"],
        "date_max": totals["date_max"],
        "total_kwh": totals["total_kwh"],
        "avg_daily_kwh": daily["avg_daily_kwh"],
        "min_daily_kwh": daily["min_daily_kwh"],
        "max_daily_kwh": daily["max_daily_kwh"],
    }


def fetch_hourly_profile(conn) -> list[dict]:
    rows = conn.execute("""
        SELECT hour_of_day, day_type, avg_kwh
        FROM hourly_avg
        WHERE read_type = 'C'
        ORDER BY hour_of_day, day_type
    """).fetchall()
    return [dict(r) for r in rows]


def fetch_daily_usage(conn) -> list[dict]:
    rows = conn.execute("""
        SELECT d.date, d.total_kwh,
               MIN(w.temperature_f) AS low_f,
               MAX(w.temperature_f) AS high_f
        FROM daily_usage d
        LEFT JOIN hourly_weather w ON d.date = w.date
        WHERE d.read_type = 'C'
        GROUP BY d.date, d.total_kwh
        ORDER BY d.date
    """).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Chart rendering
# ---------------------------------------------------------------------------

def render_hourly_chart(hourly_data: list[dict]) -> bytes:
    weekday = [0.0] * 24
    weekend = [0.0] * 24

    for row in hourly_data:
        hour = int(row["hour_of_day"])
        if row["day_type"] == "weekday":
            weekday[hour] = row["avg_kwh"]
        else:
            weekend[hour] = row["avg_kwh"]

    hours = list(range(24))
    bar_width = 0.38

    fig, ax = plt.subplots(figsize=(7, 3.5))
    ax.bar([h - bar_width / 2 for h in hours], weekday, bar_width,
           label="Weekday", color="#3b82f6", edgecolor="none")
    ax.bar([h + bar_width / 2 for h in hours], weekend, bar_width,
           label="Weekend", color="#f59e0b", edgecolor="none")

    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Average kWh")
    ax.set_xticks(hours)
    ax.set_xticklabels([f"{h:02d}" for h in hours], fontsize=7)
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1f"))
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    ax.set_xlim(-0.6, 23.6)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# PDF composition
# ---------------------------------------------------------------------------

GREY_LIGHT = colors.HexColor("#f3f4f6")
GREY_MED = colors.HexColor("#e5e7eb")
BLUE_HEADER = colors.HexColor("#1e3a5f")
WHITE = colors.white


def build_pdf(summary: dict, hourly_png: bytes, daily_rows: list[dict],
              output) -> None:
    doc = SimpleDocTemplate(
        output,
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ReportTitle", parent=styles["Title"], fontSize=20, spaceAfter=4,
    )
    subtitle_style = ParagraphStyle(
        "ReportSubtitle", parent=styles["Normal"], fontSize=10,
        textColor=colors.grey, spaceAfter=16,
    )
    section_style = ParagraphStyle(
        "SectionHeader", parent=styles["Heading2"], fontSize=13,
        spaceBefore=16, spaceAfter=8, textColor=BLUE_HEADER,
    )

    elements = []

    # Title
    elements.append(Paragraph("Energy Usage Report", title_style))
    elements.append(Paragraph(
        f"Data from {summary['date_min']} to {summary['date_max']}  "
        f"&bull;  Generated {date.today().isoformat()}",
        subtitle_style,
    ))

    # Summary table
    elements.append(Paragraph("Summary", section_style))
    summary_data = [
        ["Date Range", f"{summary['date_min']}  to  {summary['date_max']}"],
        ["Total Days", str(summary["days_collected"])],
        ["Total Consumption", f"{summary['total_kwh']:,.1f} kWh"],
        ["Avg Daily Usage", f"{summary['avg_daily_kwh']:,.1f} kWh"],
        ["Min Daily Usage", f"{summary['min_daily_kwh']:,.1f} kWh"],
        ["Max Daily Usage", f"{summary['max_daily_kwh']:,.1f} kWh"],
    ]
    col_widths = [2.0 * inch, 4.5 * inch]
    summary_table = Table(summary_data, colWidths=col_widths)
    summary_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), GREY_LIGHT),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.5, GREY_MED),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
    ]))
    elements.append(summary_table)

    # Hourly chart
    elements.append(Paragraph("Average Hourly Load Profile", section_style))
    chart_buf = io.BytesIO(hourly_png)
    chart_img = Image(chart_buf, width=7 * inch, height=3.5 * inch)
    elements.append(chart_img)

    # Monthly usage table
    elements.append(Paragraph("Monthly Usage", section_style))
    monthly_header = ["Month", "Days", "Total kWh", "Avg kWh/Day"]
    monthly_data = [monthly_header]

    # Aggregate daily rows by month
    months = {}
    for row in daily_rows:
        month_key = row["date"][:7]
        if month_key not in months:
            months[month_key] = {"total": 0.0, "days": 0}
        months[month_key]["total"] += row["total_kwh"]
        months[month_key]["days"] += 1

    for month_key in sorted(months):
        m = months[month_key]
        avg = m["total"] / m["days"] if m["days"] else 0
        monthly_data.append([
            month_key, str(m["days"]),
            f"{m['total']:,.1f}", f"{avg:,.1f}",
        ])

    monthly_col_widths = [1.8 * inch, 1.0 * inch, 2.0 * inch, 1.7 * inch]
    monthly_table = Table(monthly_data, colWidths=monthly_col_widths)
    monthly_style_cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), BLUE_HEADER),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 10),
        ("FONTSIZE", (0, 1), (-1, -1), 9),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.5, GREY_MED),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
    ]
    for i in range(1, len(monthly_data)):
        if i % 2 == 0:
            monthly_style_cmds.append(("BACKGROUND", (0, i), (-1, i), GREY_LIGHT))
    monthly_table.setStyle(TableStyle(monthly_style_cmds))
    elements.append(monthly_table)

    # Daily usage table with monthly summaries
    elements.append(Paragraph("Daily Usage", section_style))

    header = ["Date", "Day", "kWh"]
    table_data = [header]
    row_styles = []  # (row_index, is_summary)

    current_month = None
    month_total = 0.0
    month_days = 0
    month_start_row = 1  # data rows start at index 1

    for row in daily_rows:
        d = datetime.strptime(row["date"], "%Y-%m-%d")
        row_month = d.strftime("%Y-%m")
        kwh = row["total_kwh"]

        if current_month and row_month != current_month:
            # Insert monthly summary row
            avg = month_total / month_days if month_days else 0
            table_data.append([
                f"{current_month} Summary",
                f"{month_days} days",
                f"{month_total:,.1f}  (avg {avg:,.1f}/day)",
            ])
            row_styles.append(len(table_data) - 1)
            month_total = 0.0
            month_days = 0

        current_month = row_month
        month_total += kwh
        month_days += 1

        day_name = d.strftime("%a")
        low = row.get("low_f")
        high = row.get("high_f")
        if low is not None and high is not None:
            day_info = f"{day_name}  ({low:.0f}–{high:.0f}°F)"
        else:
            day_info = day_name
        table_data.append([row["date"], day_info, f"{kwh:,.1f}"])

    # Final month summary
    if current_month and month_days:
        avg = month_total / month_days
        table_data.append([
            f"{current_month} Summary",
            f"{month_days} days",
            f"{month_total:,.1f}  (avg {avg:,.1f}/day)",
        ])
        row_styles.append(len(table_data) - 1)

    col_widths_daily = [1.8 * inch, 1.2 * inch, 3.5 * inch]
    daily_table = Table(table_data, colWidths=col_widths_daily, repeatRows=1)

    style_commands = [
        # Header
        ("BACKGROUND", (0, 0), (-1, 0), BLUE_HEADER),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 10),
        # All rows
        ("FONTSIZE", (0, 1), (-1, -1), 9),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.5, GREY_MED),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        # Right-align kWh column
        ("ALIGN", (2, 0), (2, -1), "RIGHT"),
    ]

    # Alternating row colors (skip header and summary rows)
    for i in range(1, len(table_data)):
        if i in row_styles:
            # Monthly summary row styling
            style_commands.append(("BACKGROUND", (0, i), (-1, i), GREY_MED))
            style_commands.append(("FONTNAME", (0, i), (-1, i), "Helvetica-Bold"))
        elif i % 2 == 0:
            style_commands.append(("BACKGROUND", (0, i), (-1, i), GREY_LIGHT))

    daily_table.setStyle(TableStyle(style_commands))
    elements.append(daily_table)

    doc.build(elements)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_report(output_path=None) -> bytes | None:
    conn = get_connection()
    try:
        summary = fetch_summary(conn)
        hourly = fetch_hourly_profile(conn)
        daily = fetch_daily_usage(conn)
    finally:
        conn.close()

    hourly_png = render_hourly_chart(hourly)

    if output_path:
        build_pdf(summary, hourly_png, daily, str(output_path))
        log.info(f"Report written to {output_path}")
        return None
    else:
        buf = io.BytesIO()
        build_pdf(summary, hourly_png, daily, buf)
        return buf.getvalue()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate PDF energy usage report")
    parser.add_argument("-o", "--output", default="energy_report.pdf",
                        help="Output file path (default: energy_report.pdf)")
    args = parser.parse_args()

    generate_report(output_path=args.output)


if __name__ == "__main__":
    main()
