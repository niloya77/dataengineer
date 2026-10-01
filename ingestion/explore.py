from datetime import datetime, timezone

import requests

BASE_URL = "https://www.smard.de/app/chart_data"
FILTER_ID = 4169  # day-ahead electricity price
REGION = "DE-LU"  # Germany-Luxembourg bidding zone
RESOLUTION = "hour"

# 1. Fetch the index: the list of available weekly chunks
index_url = f"{BASE_URL}/{FILTER_ID}/{REGION}/index_{RESOLUTION}.json"
index_response = requests.get(index_url, timeout=30)
index_response.raise_for_status()
timestamps = index_response.json()["timestamps"]

print(f"Number of weekly chunks: {len(timestamps)}")

# 2. Fetch the second-to-last weekly chunk
chunk_ts = timestamps[-2]
chunk_url = f"{BASE_URL}/{FILTER_ID}/{REGION}/{FILTER_ID}_{REGION}_{RESOLUTION}_{chunk_ts}.json"
chunk_response = requests.get(chunk_url, timeout=30)
chunk_response.raise_for_status()
series = chunk_response.json()["series"]

print(f"Rows in chunk: {len(series)}")

# 3. Print the first 5 rows with readable timestamps
print("\nFirst 5 rows:")
for ts, price in series[:5]:
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    print(f"  {dt:%Y-%m-%d %H:%M} UTC  ->  {price} EUR/MWh")

# 4. Count missing values
missing = sum(1 for _, price in series if price is None)
print(f"\nMissing values: {missing}")
