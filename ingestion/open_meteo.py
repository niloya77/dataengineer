import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw" / "weather"

DEFAULT_MONTHS = 2

# Recent days are filled with forecast data and replaced by reanalysis about
# five days later. A stored month counts as final (and is skipped) only if it
# was downloaded this long after the last hour it contains.
FINAL_AFTER = timedelta(days=7)

VARIABLES = ["temperature_2m", "wind_speed_100m", "shortwave_radiation", "cloud_cover"]

# Representative points: name -> (latitude, longitude)
LOCATIONS = {
    "north_sea": (54.0, 6.6),           # offshore wind farms
    "schleswig_holstein": (54.3, 9.7),  # onshore wind
    "lower_saxony": (52.8, 8.5),        # onshore wind
    "bavaria": (48.5, 11.5),            # solar
    "baden_wuerttemberg": (48.6, 9.0),  # solar
    "ruhr": (51.5, 7.2),                # demand centre
}


def make_session():
    """HTTP session that retries transient errors with backoff."""
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def month_starts(n, today):
    """Return the first day of the last n months, oldest first (current month included)."""
    starts = []
    month = today.replace(day=1)
    for _ in range(n):
        starts.append(month)
        month = (month - timedelta(days=1)).replace(day=1)
    return starts[::-1]


def month_end(month_start, today):
    """Last day of the month, capped at today."""
    next_month = (month_start + timedelta(days=32)).replace(day=1)
    return min(next_month - timedelta(days=1), today)


def fetch_month(session, month_start, end):
    """Return hourly weather for all locations in one request (one response per location)."""
    params = {
        "latitude": ",".join(str(lat) for lat, _ in LOCATIONS.values()),
        "longitude": ",".join(str(lon) for _, lon in LOCATIONS.values()),
        "start_date": month_start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(VARIABLES),
        "wind_speed_unit": "ms",
        "timezone": "GMT",
    }
    response = session.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def to_dataframe(responses):
    """Convert the per-location responses into one table."""
    frames = []
    for location, data in zip(LOCATIONS, responses):
        df = pd.DataFrame(data["hourly"])
        df["timestamp_utc"] = pd.to_datetime(df.pop("time"), utc=True)
        df["location"] = location
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    df["ingested_at"] = datetime.now(timezone.utc)
    return df[["timestamp_utc", "location", *VARIABLES, "ingested_at"]]


def month_path(month_start):
    return RAW_DIR / f"{month_start:%Y-%m}.parquet"


def is_final(path):
    """True if the file exists and was downloaded well after the period it covers."""
    if not path.exists():
        return False
    df = pd.read_parquet(path, columns=["timestamp_utc", "ingested_at"])
    return df["ingested_at"].min() >= df["timestamp_utc"].max() + FINAL_AFTER


def save_parquet(df, month_start):
    """Write one month to its own file. Re-running overwrites it (idempotent)."""
    path = month_path(month_start)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest Open-Meteo hourly weather into Parquet.")
    parser.add_argument("--months", type=int, default=DEFAULT_MONTHS,
                        help="number of most recent months to cover (default: %(default)s)")
    return parser.parse_args()


def ingest(months=DEFAULT_MONTHS):
    """Download the last `months` months for all locations. Returns (saved, skipped)."""
    session = make_session()
    today = datetime.now(timezone.utc).date()
    saved = skipped = 0
    for month_start in month_starts(months, today):
        if is_final(month_path(month_start)):
            skipped += 1
            continue
        responses = fetch_month(session, month_start, month_end(month_start, today))
        save_parquet(to_dataframe(responses), month_start)
        saved += 1
    return saved, skipped


def main():
    args = parse_args()
    saved, skipped = ingest(args.months)
    print(f"weather: saved {saved} months, skipped {skipped} already final")


if __name__ == "__main__":
    main()
