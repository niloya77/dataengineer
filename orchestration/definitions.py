"""Dagster definitions: the pipeline as a graph of assets, run daily.

Start the UI with `dagster dev` from the project root, then open http://localhost:3000.
"""

import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, get_asset_key_for_model

from ingestion import open_meteo, smard
from report import build_report

TRANSFORM_DIR = Path(__file__).resolve().parent.parent / "transform"
DBT_EXECUTABLE = str(Path(sys.executable).with_name("dbt"))  # dbt from the same virtualenv

dbt_project = DbtProject(project_dir=TRANSFORM_DIR)
dbt_project.prepare_if_dev()  # compile the dbt manifest when running `dagster dev`


# ---------- ingestion ----------
# The asset keys match the dbt sources (raw.smard, raw.weather), so Dagster
# links them to the bronze models that read them.

@asset(key=["raw", "smard"], group_name="ingestion", compute_kind="python",
       description="Hourly SMARD market data (price, generation, load) as weekly Parquet chunks.")
def raw_smard(context: AssetExecutionContext) -> MaterializeResult:
    summary = smard.ingest()
    for signal, (saved, skipped) in summary.items():
        context.log.info(f"{signal}: saved {saved} chunks, skipped {skipped} already final")
    return MaterializeResult(metadata={
        "chunks_saved": sum(saved for saved, _ in summary.values()),
        "chunks_skipped": sum(skipped for _, skipped in summary.values()),
    })


@asset(key=["raw", "weather"], group_name="ingestion", compute_kind="python",
       description="Hourly Open-Meteo weather for six German locations as monthly Parquet files.")
def raw_weather(context: AssetExecutionContext) -> MaterializeResult:
    saved, skipped = open_meteo.ingest()
    context.log.info(f"weather: saved {saved} months, skipped {skipped} already final")
    return MaterializeResult(metadata={"months_saved": saved, "months_skipped": skipped})


# ---------- transformation ----------
# Every dbt model becomes an asset and every dbt test an asset check.

@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# ---------- report ----------

@asset(
    group_name="report",
    compute_kind="html",
    deps=[
        get_asset_key_for_model([dbt_models], "gold_hourly_market"),
        get_asset_key_for_model([dbt_models], "gold_price_by_weekday_hour"),
        get_asset_key_for_model([dbt_models], "gold_price_by_renewable_share"),
    ],
    description="HTML report built from the gold tables.",
)
def html_report() -> MaterializeResult:
    path = build_report.build(steps=[], run_by="dagster")
    return MaterializeResult(metadata={"path": MetadataValue.path(str(path))})


# ---------- schedule ----------

daily_pipeline = define_asset_job("daily_pipeline", selection=AssetSelection.all())

daily_schedule = ScheduleDefinition(
    job=daily_pipeline,
    cron_schedule="0 7 * * *",  # every day at 07:00
    execution_timezone="Europe/Berlin",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=[raw_smard, raw_weather, dbt_models, html_report],
    jobs=[daily_pipeline],
    schedules=[daily_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=DBT_EXECUTABLE)},
)
