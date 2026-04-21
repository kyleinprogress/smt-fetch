import os

# Set env vars before importing (module-level code reads these)
os.environ.setdefault("SMT_LATITUDE", "30.0")
os.environ.setdefault("SMT_LONGITUDE", "-97.0")

from weather import _parse_response


class TestParseResponse:
    def test_valid_response(self):
        """Parses a valid Open-Meteo response correctly."""
        data = {
            "hourly": {
                "time": ["2026-03-02T00:00", "2026-03-02T01:00", "2026-03-02T02:00"],
                "temperature_2m": [45.0, 43.5, 42.0],
            }
        }
        records = _parse_response(data, {"2026-03-02"})
        assert len(records) == 3
        assert records[0] == {"date": "2026-03-02", "hour": 0, "temperature_f": 45.0}
        assert records[1]["hour"] == 1
        assert records[2]["temperature_f"] == 42.0

    def test_filters_to_requested_dates(self):
        """Only returns records for requested dates."""
        data = {
            "hourly": {
                "time": ["2026-03-02T00:00", "2026-03-03T00:00", "2026-03-04T00:00"],
                "temperature_2m": [45.0, 50.0, 55.0],
            }
        }
        records = _parse_response(data, {"2026-03-02", "2026-03-04"})
        assert len(records) == 2
        dates = {r["date"] for r in records}
        assert dates == {"2026-03-02", "2026-03-04"}

    def test_skips_none_temps(self):
        """Skips entries with None temperature values."""
        data = {
            "hourly": {
                "time": ["2026-03-02T00:00", "2026-03-02T01:00"],
                "temperature_2m": [45.0, None],
            }
        }
        records = _parse_response(data, {"2026-03-02"})
        assert len(records) == 1

    def test_empty_response(self):
        """Handles empty hourly data."""
        data = {"hourly": {"time": [], "temperature_2m": []}}
        records = _parse_response(data, {"2026-03-02"})
        assert records == []

    def test_missing_hourly_key(self):
        """Handles response with no hourly key."""
        records = _parse_response({}, {"2026-03-02"})
        assert records == []
