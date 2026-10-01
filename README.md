# German Electricity Market Pipeline

When is electricity in Germany cheapest and greenest, and how do wind and solar generation affect the price?

## Stack

- **Ingestion:** Python (SMARD, Open-Meteo)
- **Storage:** Parquet
- **Transformation:** dbt + DuckDB (bronze / silver / gold)
- **Orchestration:** Dagster
- **Dashboard:** TBD

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
