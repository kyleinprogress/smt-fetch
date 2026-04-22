# SMT Fetch

Daily downloader for Smart Meter Texas 15-minute interval usage data.
Saves one CSV per day and maintains a SQLite database for easy querying.

> [!NOTE]
> **AI Usage**
> 
> This project was built with AI (Claude Code) as a side project to experiment with AI-assisted development. While I didn't write the code myself, I was actively involved by planning the architecture, reviewing changes, and testing behavior rather than just accepting whatever the model produced. That said, I'm one person reviewing AI-generated code, so there may still be security issues or bugs I didn't catch. My goal for the project was an easy, automated way to start tracking daily power usage to better understand my needs before evaluating solar and battery backup options. PRs addressing any issues are welcome. I **strongly** recommend running this only on your local network — it was not designed or hardened for exposure to the open internet.

## Quick start (Docker)

Sign up for an account with [Smart Meter Texas](https://www.smartmetertexas.com/home) and link to ESID. Once you have that, fill those values into the .env file so it can begin querying your data.

```bash
cp .env.example .env
nano .env        # fill in SMT_USERNAME, SMT_PASSWORD, SMT_ESIID

docker compose pull
docker compose up -d
```

On first start the container runs `run.sh` immediately (fetch yesterday + import to SQLite), then supercronic takes over for the 9:15 AM daily job. 

>[!NOTE]
>I have found that my full 15 min data is typically a day behind. There is an endpoint that can query current usage, but it's limited to a small number of daily updates. And since the goal is track usage over time, not "real time" usage, I'm using the trailing 1 day download. It also makes getting historical data much easier.

---

## Project structure

```
smt-fetch/
├── smt_fetch.py        # Fetches interval data → writes CSV
├── db.py               # Imports CSVs → SQLite (with upsert)
├── run.sh              # Runs both in sequence (used by cron)
├── crontab             # supercronic schedule
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

**Data layout (inside the container / mounted volume):**
```
data/
├── csv/
│   ├── smt_interval_2026-04-12.csv
│   └── ...
├── smt_energy.db       # SQLite database
└── logs/
    └── smt_cron.log
```

---



## Backfill historical data

SMT keeps ~24 months of interval data. Run a backfill loop before switching
to daily mode. Do this from the host (not in Docker) or temporarily via
`docker compose run`:

```bash
# From host (with uv):
for i in $(seq 2 60); do
  SMT_DAYS_BACK=$i uv run python smt_fetch.py
  sleep 2
done
uv run python db.py --backfill --stats

# Or via Docker (one-off):
docker compose run --rm smt-fetch sh -c "
  for i in \$(seq 2 60); do
    SMT_DAYS_BACK=\$i python smt_fetch.py
    sleep 2
  done
  python db.py --backfill --stats
"
```

---

## Energy providers

Track your retail electricity provider and rates to calculate daily costs.
On first run, `db.py` seeds Abundance Energy as the default provider.

```bash
# List all providers
uv run python db.py --list-providers

# Add a new provider (when you switch)
uv run python db.py --add-provider \
  --name "New Energy Co" \
  --start 2027-01-15 \
  --energy-rate 0.065 \
  --tdu-rate 0.052

# Add a provider with a known end date
uv run python db.py --add-provider \
  --name "Old Provider" \
  --start 2025-06-01 \
  --end 2026-01-25 \
  --energy-rate 0.08 \
  --tdu-rate 0.055
```

When switching providers, add the new one and optionally update the old one's
end date directly in SQLite:

```sql
UPDATE energy_provider SET contract_end = '2027-01-14' WHERE name = 'Abundance Energy';
```

The dashboard uses `contract_start` and `contract_end` to determine which
provider's rates apply to each day's cost calculation.

---

## Querying the database

```bash
# Open the DB
sqlite3 data/smt_energy.db

# Daily totals
SELECT * FROM daily_usage ORDER BY date DESC LIMIT 7;

# Average hourly load profile (weekdays vs weekends)
SELECT * FROM hourly_avg WHERE read_type = 'C' ORDER BY day_type, hour_of_day;

# Highest consumption 15-min intervals ever
SELECT date, interval_start, consumption_kwh
FROM interval_usage
WHERE read_type = 'C'
ORDER BY consumption_kwh DESC
LIMIT 20;

# Monthly totals
SELECT
  SUBSTR(date, 1, 7) AS month,
  ROUND(SUM(consumption_kwh), 2) AS total_kwh,
  COUNT(DISTINCT date) AS days
FROM interval_usage
WHERE read_type = 'C'
GROUP BY month
ORDER BY month DESC;

# Peak hour by month (useful for TOU rate planning)
SELECT
  SUBSTR(date, 1, 7)            AS month,
  SUBSTR(interval_start, 12, 2) AS hour,
  ROUND(AVG(consumption_kwh), 4) AS avg_kwh
FROM interval_usage
WHERE read_type = 'C'
GROUP BY month, hour
ORDER BY month DESC, avg_kwh DESC;
```

---

## Adjusting the cron schedule

Edit `crontab`. The default is 14:15 UTC (≈ 9:15 AM CDT). Adjust for
daylight saving or your preferred time:

```
# 9:15 AM CDT (UTC-5 in summer)
15 14 * * * /app/run.sh >> /data/logs/smt_cron.log 2>&1

# 9:15 AM CST (UTC-6 in winter)
15 15 * * * /app/run.sh >> /data/logs/smt_cron.log 2>&1
```

After changing, rebuild: `docker compose up --build -d`

---

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `SMT_USERNAME` | ✓ | — | SMT login email |
| `SMT_PASSWORD` | ✓ | — | SMT password |
| `SMT_ESIID` | ✓ | — | ESIID from electric bill |
| `SMT_DAYS_BACK` | | `1` | Days back to fetch (1 = yesterday) |
| `SMT_OUTPUT_DIR` | | `./data` | Directory for CSV files |
| `SMT_DB_PATH` | | `$SMT_OUTPUT_DIR/smt_energy.db` | SQLite file path |
| `SMT_LOG_LEVEL` | | `INFO` | DEBUG / INFO / WARNING |
| `SMT_FORCE_REFETCH` | | `false` | Set to `true` to overwrite existing CSVs |
