from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

BASE_URL = "https://www.smard.de/app/chart_data"
RESOLUTION = "hour"
RAW_DIR = Path("data/raw")

# Signals to ingest: name -> (SMARD filter id, region)
SIGNALS = {
    "price": (4169, "DE-LU"),
    "solar": (4068, "DE"),
    "wind_onshore": (4067, "DE"),
    "wind_offshore": (1225, "DE"),
    "load": (410, "DE"),
}


def fetch_index(filter_id, region):
    """Return the list of available weekly chunk timestamps."""
    url = f"{BASE_URL}/{filter_id}/{region}/index_{RESOLUTION}.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()["timestamps"]


def fetch_chunk(filter_id, region, chunk_ts):
    """Return the [timestamp, value] rows for one weekly chunk."""
    url = f"{BASE_URL}/{filter_id}/{region}/{filter_id}_{region}_{RESOLUTION}_{chunk_ts}.json"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()["series"]


def to_dataframe(series, signal):
    """Convert raw SMARD rows into a table."""
    df = pd.DataFrame(series, columns=["timestamp_ms", "value"])
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df["signal"] = signal
    df["ingested_at"] = datetime.now(timezone.utc)
    return df[["timestamp_utc", "signal", "value", "ingested_at"]]


def save_parquet(df, signal, chunk_ts):
    """Write one chunk to its own file. Re-running overwrites it (idempotent)."""
    out_dir = RAW_DIR / signal
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{chunk_ts}.parquet"
    df.to_parquet(path, index=False)
    return path


def main():
    for signal, (filter_id, region) in SIGNALS.items():
        timestamps = fetch_index(filter_id, region)
        # The last two chunks: the current week and the one before it
        for chunk_ts in timestamps[-2:]:
            series = fetch_chunk(filter_id, region, chunk_ts)
            df = to_dataframe(series, signal)
            path = save_parquet(df, signal, chunk_ts)
            print(f"{signal}: saved {len(df)} rows to {path}")


if __name__ == "__main__":
    main()
