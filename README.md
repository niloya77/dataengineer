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

## Run

```bash
python run.py
```

Runs the whole pipeline (SMARD + Open-Meteo ingestion, `dbt build` with tests) and
opens an HTML report with the results at `output/report.html`.

First time only, backfill one year of history before running:

```bash
python ingestion/smard.py --weeks 53
python ingestion/open_meteo.py --months 13
```

Later runs only refresh the recent, not-yet-final weeks and months.

## Orchestration (Dagster)

```bash
dagster dev
```

Opens the Dagster UI at http://localhost:3000. The pipeline is modelled as assets
(raw data → bronze → silver → gold → report); every dbt test shows up as an asset
check. The `daily_pipeline` job runs every day at 07:00 Berlin time while
`dagster dev` is running.
