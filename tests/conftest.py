import sys, os, sqlite3, io, csv, pytest
from pathlib import Path
from datetime import date

# Add src to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

@pytest.fixture
def test_db():
    """In-memory SQLite with schema initialized."""
    # Need to import here after path setup
    from db import DDL
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(DDL)
    conn.commit()
    yield conn
    conn.close()

@pytest.fixture
def seeded_db(test_db, tmp_path):
    """test_db with sample interval data for 2 days (a weekday and a weekend day)."""
    # Monday 2026-03-02 and Saturday 2026-03-07
    days = [("2026-03-02", "weekday"), ("2026-03-07", "weekend")]
    for day_date, _ in days:
        for idx in range(96):
            total_minutes = idx * 15
            hh = total_minutes // 60
            mm = total_minutes % 60
            end_minutes = (idx + 1) * 15
            e_hh = end_minutes // 60
            e_mm = end_minutes % 60
            # Vary consumption: higher during afternoon (idx 40-72 = 10am-6pm)
            kwh = 1.5 if 40 <= idx <= 71 else 0.5
            test_db.execute("""
                INSERT INTO interval_usage
                (date, interval_start, interval_end, read_type, consumption_kwh,
                 quality_flag, esiid, raw_interval_index)
                VALUES (?, ?, ?, 'C', ?, 'A', 'TEST_ESIID', ?)
            """, (
                day_date,
                f"{day_date}T{hh:02d}:{mm:02d}:00",
                f"{day_date}T{e_hh:02d}:{e_mm:02d}:00",
                kwh, idx
            ))
    test_db.commit()

    # Also seed weather data for one of the days
    for hour in range(24):
        temp = 50.0 + hour  # simple increasing temp
        test_db.execute("""
            INSERT INTO hourly_weather (date, hour, temperature_f)
            VALUES (?, ?, ?)
        """, ("2026-03-02", hour, temp))
    test_db.commit()

    return test_db

@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file matching the smt_fetch output format."""
    csv_path = tmp_path / "smt_interval_2026-03-02.csv"
    fieldnames = ["date", "interval_start", "interval_end", "read_type",
                  "consumption_kwh", "quality_flag", "esiid", "raw_interval_index"]
    rows = []
    for idx in range(96):
        total_minutes = idx * 15
        hh = total_minutes // 60
        mm = total_minutes % 60
        end_minutes = (idx + 1) * 15
        e_hh = end_minutes // 60
        e_mm = end_minutes % 60
        rows.append({
            "date": "2026-03-02",
            "interval_start": f"2026-03-02T{hh:02d}:{mm:02d}:00",
            "interval_end": f"2026-03-02T{e_hh:02d}:{e_mm:02d}:00",
            "read_type": "C",
            "consumption_kwh": "1.234",
            "quality_flag": "A",
            "esiid": "TEST_ESIID",
            "raw_interval_index": str(idx),
        })
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return csv_path
