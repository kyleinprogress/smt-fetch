import io
import matplotlib
matplotlib.use("Agg")

from report import render_hourly_chart, build_pdf


class TestRenderHourlyChart:
    def test_returns_valid_png(self):
        """render_hourly_chart produces valid PNG bytes."""
        hourly_data = []
        for h in range(24):
            hourly_data.append({"hour_of_day": f"{h:02d}", "day_type": "weekday", "avg_kwh": 2.0 + h * 0.1})
            hourly_data.append({"hour_of_day": f"{h:02d}", "day_type": "weekend", "avg_kwh": 1.5 + h * 0.1})

        png = render_hourly_chart(hourly_data)
        assert isinstance(png, bytes)
        assert png[:4] == b'\x89PNG'
        assert len(png) > 1000  # Sanity check it's a real image

    def test_handles_empty_data(self):
        """Doesn't crash with empty hourly data (all zeros)."""
        png = render_hourly_chart([])
        assert png[:4] == b'\x89PNG'


class TestBuildPdf:
    def test_produces_valid_pdf(self):
        """build_pdf creates a valid PDF in a BytesIO buffer."""
        summary = {
            "days_collected": 30,
            "date_min": "2026-02-01",
            "date_max": "2026-03-02",
            "total_kwh": 2500.0,
            "avg_daily_kwh": 83.3,
            "min_daily_kwh": 45.0,
            "max_daily_kwh": 120.0,
        }
        # Generate a real chart PNG for the PDF
        hourly_data = []
        for h in range(24):
            hourly_data.append({"hour_of_day": f"{h:02d}", "day_type": "weekday", "avg_kwh": 2.0})
            hourly_data.append({"hour_of_day": f"{h:02d}", "day_type": "weekend", "avg_kwh": 1.5})
        hourly_png = render_hourly_chart(hourly_data)

        daily_rows = [
            {"date": "2026-02-01", "total_kwh": 80.0, "low_f": 35.0, "high_f": 55.0},
            {"date": "2026-02-02", "total_kwh": 85.0, "low_f": 38.0, "high_f": 60.0},
            {"date": "2026-03-01", "total_kwh": 90.0, "low_f": 45.0, "high_f": 65.0},
        ]

        buf = io.BytesIO()
        build_pdf(summary, hourly_png, daily_rows, buf)
        pdf_bytes = buf.getvalue()

        assert pdf_bytes[:4] == b'%PDF'
        assert len(pdf_bytes) > 1000

    def test_handles_no_weather_data(self):
        """build_pdf works when daily rows have no weather data."""
        summary = {
            "days_collected": 1,
            "date_min": "2026-02-01",
            "date_max": "2026-02-01",
            "total_kwh": 80.0,
            "avg_daily_kwh": 80.0,
            "min_daily_kwh": 80.0,
            "max_daily_kwh": 80.0,
        }
        hourly_png = render_hourly_chart([])
        daily_rows = [{"date": "2026-02-01", "total_kwh": 80.0, "low_f": None, "high_f": None}]

        buf = io.BytesIO()
        build_pdf(summary, hourly_png, daily_rows, buf)
        assert buf.getvalue()[:4] == b'%PDF'
