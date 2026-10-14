"""Render the pipeline results into a self-contained HTML report."""

import json
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE = ROOT / "data" / "warehouse.duckdb"
RUN_RESULTS = ROOT / "transform" / "target" / "run_results.json"
TEMPLATE = Path(__file__).with_name("template.html")
OUTPUT = ROOT / "output" / "report.html"


def query(con, sql):
    """Run a query and return its rows as a list of dicts."""
    rel = con.sql(sql)
    return [dict(zip(rel.columns, row)) for row in rel.fetchall()]


def collect_market_data():
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    try:
        summary = query(con, """
            select
                min(timestamp_local) as first_hour,
                max(timestamp_local) as last_hour,
                count(*) as hours,
                avg(price_eur_mwh) as avg_price,
                avg(renewable_share) as avg_renewable_share,
                count(*) filter (where is_negative_price) as negative_hours,
                corr(price_eur_mwh, renewable_share) as corr_price_renewable
            from gold_hourly_market
        """)[0]
        by_hour = query(con, """
            select
                hour_local as hour,
                avg(price_eur_mwh) as price,
                avg(renewable_share) as renewable_share
            from gold_hourly_market
            group by hour_local
            order by hour_local
        """)
        weekday_hour = query(con, """
            select weekday, hour_local as hour, avg_price_eur_mwh as price,
                   avg_renewable_share as renewable_share, price_rank as rank
            from gold_price_by_weekday_hour
            order by weekday, hour_local
        """)
        buckets = query(con, """
            select renewable_share_bucket as bucket, hours, avg_price_eur_mwh as price,
                   negative_price_hours
            from gold_price_by_renewable_share
            order by renewable_share_bucket
        """)
        recent = query(con, """
            select timestamp_local as ts, price_eur_mwh as price, renewable_share
            from gold_hourly_market
            where timestamp_local > (select max(timestamp_local) from gold_hourly_market) - interval 14 day
            order by timestamp_local
        """)
    finally:
        con.close()
    return {
        "summary": summary,
        "by_hour": by_hour,
        "weekday_hour": weekday_hour,
        "buckets": buckets,
        "recent": recent,
    }


def collect_dbt_results():
    """Summarize the latest dbt run from its run_results.json artifact."""
    if not RUN_RESULTS.exists():
        return None
    results = json.loads(RUN_RESULTS.read_text())["results"]
    models = [r for r in results if r["unique_id"].startswith("model.")]
    tests = [r for r in results if r["unique_id"].startswith("test.")]
    return {
        "models_ok": sum(r["status"] == "success" for r in models),
        "models_total": len(models),
        "tests_ok": sum(r["status"] == "pass" for r in tests),
        "tests_total": len(tests),
        "problems": [
            {"name": r["unique_id"].split(".")[2], "status": r["status"], "message": r.get("message")}
            for r in results
            if r["status"] not in ("success", "pass")
        ],
    }


def build(steps, run_by="run.py"):
    """Write the report and return its path.

    `steps` is the run log from run.py. Under Dagster the steps and dbt tests are
    tracked in the Dagster UI instead, so they are left out of the report.
    """
    data = {
        "generated_at": datetime.now(),
        "run_by": run_by,
        "steps": steps,
        "dbt": collect_dbt_results() if run_by == "run.py" else None,
        "market": collect_market_data() if WAREHOUSE.exists() else None,
    }
    payload = json.dumps(data, default=str).replace("</", "<\\/")
    html = TEMPLATE.read_text().replace("/*__DATA__*/null", payload)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(html)
    return OUTPUT


if __name__ == "__main__":
    print(build(steps=[]))
