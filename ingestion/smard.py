import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.smard.de/app/chart_data"
RESOLUTION = "hour"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw" / "smard"

DEFAULT_WEEKS = 2

# SMARD keeps filling in a week after it starts. A stored chunk counts as final
# (and is skipped) only if it was downloaded this long after its week ended.
FINAL_AFTER = timedelta(days=7)

# Signals to ingest: name -> (SMARD filter id, region)
SIGNALS = {
    "price": (4169, "DE-LU"),
    "solar": (4068, "DE"),
    "wind_onshore": (4067, "DE"),
    "wind_offshore": (1225, "DE"),
    "load": (410, "DE"),
}


def make_session():
    """HTTP session that retries transient errors with backoff."""
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_index(session, filter_id, region):
    """Return the list of available weekly chunk timestamps."""
    url = f"{BASE_URL}/{filter_id}/{region}/index_{RESOLUTION}.json"
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.json()["timestamps"]


def fetch_chunk(session, filter_id, region, chunk_ts):
    """Return the [timestamp, value] rows for one weekly chunk."""
    url = f"{BASE_URL}/{filter_id}/{region}/{filter_id}_{region}_{RESOLUTION}_{chunk_ts}.json"
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.json()["series"]


def to_dataframe(series, signal):
    """Convert raw SMARD rows into a table."""
    df = pd.DataFrame(series, columns=["timestamp_ms", "value"])
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df["signal"] = signal
    df["ingested_at"] = datetime.now(timezone.utc)
    return df[["timestamp_utc", "signal", "value", "ingested_at"]]


def chunk_path(signal, chunk_ts):
    return RAW_DIR / signal / f"{chunk_ts}.parquet"


def is_final(path):
    """True if the file exists and was downloaded well after the period it covers."""
    if not path.exists():
        return False
    df = pd.read_parquet(path, columns=["timestamp_utc", "ingested_at"])
    return df["ingested_at"].min() >= df["timestamp_utc"].max() + FINAL_AFTER


def save_parquet(df, signal, chunk_ts):
    """Write one chunk to its own file. Re-running overwrites it (idempotent)."""
    path = chunk_path(signal, chunk_ts)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest SMARD hourly data into Parquet.")
    parser.add_argument("--weeks", type=int, default=DEFAULT_WEEKS,
                        help="number of most recent weekly chunks to cover (default: %(default)s)")
    return parser.parse_args()


def ingest(weeks=DEFAULT_WEEKS):
    """Download the last `weeks` chunks of every signal. Returns {signal: (saved, skipped)}."""
    session = make_session()
    summary = {}
    for signal, (filter_id, region) in SIGNALS.items():
        timestamps = fetch_index(session, filter_id, region)
        saved = skipped = 0
        for chunk_ts in timestamps[-weeks:]:
            if is_final(chunk_path(signal, chunk_ts)):
                skipped += 1
                continue
            series = fetch_chunk(session, filter_id, region, chunk_ts)
            save_parquet(to_dataframe(series, signal), signal, chunk_ts)
            saved += 1
        summary[signal] = (saved, skipped)
    return summary


def main():
    args = parse_args()
    for signal, (saved, skipped) in ingest(args.weeks).items():
        print(f"{signal}: saved {saved} chunks, skipped {skipped} already final")


if __name__ == "__main__":
    main()
