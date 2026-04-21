import sqlite3
import pytest
from pathlib import Path

from db import init_db, import_csv, seed_providers, add_provider


class TestInitDb:
    def test_creates_tables(self, test_db):
        """init_db creates all expected tables."""
        tables = test_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        assert "interval_usage" in table_names
        assert "hourly_weather" in table_names
        assert "energy_provider" in table_names

    def test_creates_views(self, test_db):
        """init_db creates the expected views."""
        views = test_db.execute(
            "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        ).fetchall()
        view_names = [v["name"] for v in views]
        assert "daily_usage" in view_names
        assert "hourly_avg" in view_names

    def test_idempotent(self, test_db):
        """Running init_db twice doesn't error (IF NOT EXISTS)."""
        init_db(test_db)  # Already initialized by fixture, run again
        tables = test_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        assert len(tables) >= 3


class TestImportCsv:
    def test_imports_correct_row_count(self, test_db, sample_csv):
        """Importing a 96-row CSV returns 96."""
        count = import_csv(sample_csv, test_db)
        assert count == 96

    def test_data_in_db_after_import(self, test_db, sample_csv):
        """Imported rows are queryable in the database."""
        import_csv(sample_csv, test_db)
        row = test_db.execute(
            "SELECT COUNT(*) as cnt FROM interval_usage"
        ).fetchone()
        assert row["cnt"] == 96

    def test_upsert_no_duplicates(self, test_db, sample_csv):
        """Re-importing the same CSV doesn't duplicate rows."""
        import_csv(sample_csv, test_db)
        import_csv(sample_csv, test_db)
        row = test_db.execute(
            "SELECT COUNT(*) as cnt FROM interval_usage"
        ).fetchone()
        assert row["cnt"] == 96


class TestDailyUsageView:
    def test_aggregates_correctly(self, seeded_db):
        """daily_usage view sums intervals correctly."""
        rows = seeded_db.execute(
            "SELECT date, total_kwh, intervals FROM daily_usage WHERE read_type = 'C' ORDER BY date"
        ).fetchall()
        assert len(rows) == 2
        # Day 1: 64 intervals at 0.5 + 32 at 1.5 = 32 + 48 = 80
        assert rows[0]["intervals"] == 96
        assert abs(rows[0]["total_kwh"] - 80.0) < 0.01

    def test_each_day_has_96_intervals(self, seeded_db):
        """Each day should have 96 intervals."""
        rows = seeded_db.execute(
            "SELECT intervals FROM daily_usage WHERE read_type = 'C'"
        ).fetchall()
        for row in rows:
            assert row["intervals"] == 96


class TestHourlyAvgView:
    def test_returns_hourly_totals(self, seeded_db):
        """hourly_avg returns per-hour totals (sum of 4 intervals), not per-interval averages."""
        rows = seeded_db.execute(
            "SELECT hour_of_day, avg_kwh FROM hourly_avg WHERE read_type = 'C' AND day_type = 'weekday' ORDER BY hour_of_day"
        ).fetchall()
        assert len(rows) == 24
        # Hour 00 has 4 intervals at 0.5 each = 2.0 total
        assert abs(rows[0]["avg_kwh"] - 2.0) < 0.01
        # Hour 12 has 4 intervals at 1.5 each = 6.0 total
        assert abs(rows[12]["avg_kwh"] - 6.0) < 0.01

    def test_weekday_weekend_separation(self, seeded_db):
        """View separates weekday and weekend data."""
        day_types = seeded_db.execute(
            "SELECT DISTINCT day_type FROM hourly_avg WHERE read_type = 'C'"
        ).fetchall()
        types = {r["day_type"] for r in day_types}
        assert types == {"weekday", "weekend"}


class TestProviders:
    def test_seed_providers(self, test_db):
        """seed_providers inserts default provider when table is empty."""
        seed_providers(test_db)
        row = test_db.execute("SELECT COUNT(*) as cnt FROM energy_provider").fetchone()
        assert row["cnt"] == 1

    def test_seed_providers_idempotent(self, test_db):
        """seed_providers doesn't duplicate when called twice."""
        seed_providers(test_db)
        seed_providers(test_db)
        row = test_db.execute("SELECT COUNT(*) as cnt FROM energy_provider").fetchone()
        assert row["cnt"] == 1

    def test_add_provider(self, test_db):
        """add_provider inserts a new provider."""
        add_provider(test_db, "Test Energy", "2026-01-01", 0.08, 0.05)
        row = test_db.execute(
            "SELECT * FROM energy_provider WHERE name = 'Test Energy'"
        ).fetchone()
        assert row is not None
        assert row["energy_charge_kwh"] == 0.08
        assert row["tdu_charge_kwh"] == 0.05
