import sqlite3
import pytest
from unittest.mock import patch, MagicMock
from aiohttp.test_utils import TestServer, TestClient

from dashboard import create_app


@pytest.fixture
def patched_app(seeded_db):
    """Create app with get_connection patched to return test DB.

    Wraps the connection so that handler calls to conn.close() are no-ops,
    since sqlite3.Connection.close is a read-only C-level attribute.
    """
    # Create a thin wrapper that delegates everything except close()
    wrapper = MagicMock(wraps=seeded_db)
    wrapper.close = MagicMock()  # no-op close
    # Ensure execute/executemany/etc. return real results (wraps handles this)
    # But row_factory needs to be explicitly forwarded for sqlite3.Row support
    wrapper.row_factory = seeded_db.row_factory

    with patch("dashboard.get_connection", return_value=wrapper), \
         patch("report.get_connection", return_value=wrapper):
        app = create_app()
        yield app


@pytest.fixture
async def client(patched_app):
    """Create test client using aiohttp test utilities."""
    server = TestServer(patched_app)
    client = TestClient(server)
    await client.start_server()
    yield client
    await client.close()


class TestDashboardEndpoints:
    @pytest.mark.asyncio
    async def test_index_returns_html(self, client):
        resp = await client.get("/")
        assert resp.status == 200
        assert "text/html" in resp.headers["Content-Type"]

    @pytest.mark.asyncio
    async def test_summary_returns_json(self, client):
        resp = await client.get("/api/summary")
        assert resp.status == 200
        data = await resp.json()
        assert "days_collected" in data
        assert "avg_daily_kwh" in data
        assert "total_kwh" in data
        assert data["days_collected"] == 2

    @pytest.mark.asyncio
    async def test_daily_returns_list(self, client):
        resp = await client.get("/api/daily")
        assert resp.status == 200
        data = await resp.json()
        assert isinstance(data, list)
        assert len(data) == 2

    @pytest.mark.asyncio
    async def test_daily_date_filter(self, client):
        resp = await client.get("/api/daily?from=2026-03-07&to=2026-03-07")
        data = await resp.json()
        assert len(data) == 1
        assert data[0]["date"] == "2026-03-07"

    @pytest.mark.asyncio
    async def test_intervals_returns_96_rows(self, client):
        resp = await client.get("/api/intervals?date=2026-03-02")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 96

    @pytest.mark.asyncio
    async def test_intervals_requires_date(self, client):
        resp = await client.get("/api/intervals")
        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_hourly_profile(self, client):
        resp = await client.get("/api/hourly-profile")
        assert resp.status == 200
        data = await resp.json()
        assert isinstance(data, list)
        assert len(data) > 0
        # Should have hour_of_day and day_type fields
        assert "hour_of_day" in data[0]
        assert "day_type" in data[0]

    @pytest.mark.asyncio
    async def test_weather_daily(self, client):
        resp = await client.get("/api/weather/daily")
        assert resp.status == 200
        data = await resp.json()
        assert isinstance(data, list)

    @pytest.mark.asyncio
    async def test_report_returns_pdf(self, client):
        resp = await client.get("/api/report")
        assert resp.status == 200
        assert resp.headers["Content-Type"] == "application/pdf"
        body = await resp.read()
        assert body[:4] == b'%PDF'

    @pytest.mark.asyncio
    async def test_intervals_invalid_date(self, client):
        resp = await client.get("/api/intervals?date=not-a-date")
        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_intervals_sql_injection(self, client):
        resp = await client.get("/api/intervals?date='; DROP TABLE interval_usage;--")
        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_daily_invalid_date(self, client):
        resp = await client.get("/api/daily?from=bad&to=2026-03-07")
        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_weather_hourly_invalid_date(self, client):
        resp = await client.get("/api/weather/hourly?date=abc")
        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_report_rate_limited(self, client):
        # First call should succeed (but previous test already called it)
        # Force the cooldown by calling twice rapidly
        import dashboard
        dashboard._last_report_time = 0  # reset
        resp1 = await client.get("/api/report")
        assert resp1.status == 200
        resp2 = await client.get("/api/report")
        assert resp2.status == 429
