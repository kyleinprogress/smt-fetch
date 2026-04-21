import os
import pytest
from datetime import date

# Set env vars before importing (module-level code reads these)
os.environ.setdefault("SMT_ESIID", "TEST_ESIID")
os.environ.setdefault("SMT_USERNAME", "test")
os.environ.setdefault("SMT_PASSWORD", "test")

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
