import os
import pytest
from datetime import date

# Set env vars before importing (module-level code reads these)
os.environ.setdefault("SMT_ESIID", "TEST_ESIID")
os.environ.setdefault("SMT_USERNAME", "test")
os.environ.setdefault("SMT_PASSWORD", "test")

import smt_fetch
from smt_fetch import parse_energy_data


class TestParseEnergyData:
    def test_basic_parsing(self):
        """Parses valid interval data into correct row structure."""
        data = [{"RT": "C", "RD": "1.5-A,2.0-A,0.75-E"}]
        rows = parse_energy_data(data, date(2026, 3, 2))
        assert len(rows) == 3
        assert rows[0]["date"] == "2026-03-02"
        assert rows[0]["read_type"] == "C"
        assert rows[0]["consumption_kwh"] == 1.5
        assert rows[0]["interval_start"] == "2026-03-02T00:00:00"
        assert rows[0]["interval_end"] == "2026-03-02T00:15:00"
        assert rows[0]["raw_interval_index"] == 0

    def test_quality_flags_stripped(self):
        """Quality flags are extracted and stored separately."""
        data = [{"RT": "C", "RD": "1.0-A,2.0-E,3.0"}]
        rows = parse_energy_data(data, date(2026, 3, 2))
        assert rows[0]["quality_flag"] == "A"
        assert rows[1]["quality_flag"] == "E"
        assert rows[2]["quality_flag"] == ""

    def test_full_day_96_intervals(self):
        """A full day of data produces 96 rows."""
        values = ",".join(["1.0-A"] * 96)
        data = [{"RT": "C", "RD": values}]
        rows = parse_energy_data(data, date(2026, 3, 2))
        assert len(rows) == 96
        # Last interval should be 23:45-00:00
        assert rows[95]["interval_start"] == "2026-03-02T23:45:00"
        assert rows[95]["interval_end"] == "2026-03-02T24:00:00"

    def test_excess_values_capped_at_96(self):
        """More than 96 values are truncated."""
        values = ",".join(["1.0"] * 100)
        data = [{"RT": "C", "RD": values}]
        rows = parse_energy_data(data, date(2026, 3, 2))
        assert len(rows) == 96

    def test_malformed_value_defaults_to_zero(self):
        """Non-numeric kWh values default to 0.0."""
        data = [{"RT": "C", "RD": "abc,1.5-A"}]
        rows = parse_energy_data(data, date(2026, 3, 2))
        assert rows[0]["consumption_kwh"] == 0.0
        assert rows[1]["consumption_kwh"] == 1.5

    def test_empty_input(self):
        """Empty energy_data returns empty list."""
        assert parse_energy_data([], date(2026, 3, 2)) == []

    def test_empty_rd_field(self):
        """Empty RD field returns no rows for that entry."""
        data = [{"RT": "C", "RD": ""}]
        rows = parse_energy_data(data, date(2026, 3, 2))
        assert len(rows) == 0

    def test_multiple_read_types(self):
        """Consumption and generation entries are both parsed."""
        data = [
            {"RT": "C", "RD": "1.0-A,2.0-A"},
            {"RT": "G", "RD": "0.5-A,0.3-A"},
        ]
        rows = parse_energy_data(data, date(2026, 3, 2))
        assert len(rows) == 4
        c_rows = [r for r in rows if r["read_type"] == "C"]
        g_rows = [r for r in rows if r["read_type"] == "G"]
        assert len(c_rows) == 2
        assert len(g_rows) == 2

    def test_interval_time_calculation(self):
        """Interval timestamps are correctly calculated from index."""
        data = [{"RT": "C", "RD": ",".join(["1.0"] * 8)}]
        rows = parse_energy_data(data, date(2026, 1, 15))
        # Index 4 = 01:00, Index 5 = 01:15
        assert rows[4]["interval_start"] == "2026-01-15T01:00:00"
        assert rows[4]["interval_end"] == "2026-01-15T01:15:00"
        assert rows[5]["interval_start"] == "2026-01-15T01:15:00"


class TestRunExitBehavior:
    """run() should exit non-zero only when *all* fetches fail (issue #18)."""

    @pytest.fixture
    def fake_row(self):
        return {
            "date": "2026-04-21",
            "interval_start": "2026-04-21T00:00:00",
            "interval_end": "2026-04-21T00:15:00",
            "read_type": "C",
            "consumption_kwh": 1.0,
            "quality_flag": "A",
            "esiid": "TEST_ESIID",
            "raw_interval_index": 0,
        }

    @pytest.fixture
    def isolated_output(self, tmp_path, monkeypatch):
        monkeypatch.setattr(smt_fetch, "OUTPUT_DIR", tmp_path)
        return tmp_path

    def _run_with_fetch(self, monkeypatch, fetch_impl, days_back):
        async def fake_fetch(target_date):
            return fetch_impl(target_date)
        monkeypatch.setattr(smt_fetch, "fetch_intervals", fake_fetch)
        import asyncio
        asyncio.run(smt_fetch.run(days_back=days_back))

    def test_partial_failure_does_not_exit(self, monkeypatch, isolated_output, fake_row):
        """Some dates succeed, some fail → exit 0 so downstream pipeline runs."""
        call_count = {"n": 0}

        def fetch(target_date):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return []  # first date fails
            return [{**fake_row, "date": target_date.isoformat()}]

        # Should not raise SystemExit
        self._run_with_fetch(monkeypatch, fetch, days_back=3)
        # At least one CSV got written
        csvs = list(isolated_output.glob("smt_interval_*.csv"))
        assert len(csvs) >= 1

    def test_total_failure_exits_nonzero(self, monkeypatch, isolated_output):
        """All dates fail → exit 1 so cron / orchestration can alert."""
        def fetch(target_date):
            return []

        with pytest.raises(SystemExit) as exc:
            self._run_with_fetch(monkeypatch, fetch, days_back=2)
        assert exc.value.code == 1

    def test_all_succeed_no_exit(self, monkeypatch, isolated_output, fake_row):
        """All dates succeed → no exit, all CSVs written."""
        def fetch(target_date):
            return [{**fake_row, "date": target_date.isoformat()}]

        self._run_with_fetch(monkeypatch, fetch, days_back=2)
        csvs = list(isolated_output.glob("smt_interval_*.csv"))
        assert len(csvs) == 2
