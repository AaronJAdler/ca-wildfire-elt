"""Daily wildfire pipeline. Airflow schedules and retries; the work runs in plain commands.

Each run owns the data interval of one UTC day. A run for August 20 starts just after midnight on
August 21, when that day's snapshot is complete, and behaves the same whenever it is rerun.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

PROJECT_ROOT = os.environ.get("WILDFIRE_PROJECT_ROOT", str(Path(__file__).resolve().parents[2]))
LOGICAL_DATE = "{{ data_interval_start | ds }}"

with DAG(
    dag_id="wildfire_daily",
    # Airflow 3 cron strings run at the interval's start by default. This timetable keeps the
    # classic behavior, where a run starts after the period it processes has ended.
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=datetime(2021, 7, 15, tzinfo=UTC),
    end_date=datetime(2021, 10, 16, tzinfo=UTC),
    catchup=False,  # History is loaded deliberately, with `airflow backfill create`.
    max_active_runs=1,  # DuckDB allows one writer, and the SCD2 snapshot must run in date order.
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
        "cwd": PROJECT_ROOT,
    },
    tags=["wildfire", "elt"],
) as dag:
    extract = BashOperator(
        task_id="extract_to_bronze",
        bash_command=f"uv run --frozen wildfire extract --date {LOGICAL_DATE}",
        skip_on_exit_code=99,  # The source published nothing that day, which is not a failure.
    )

    transform = BashOperator(
        task_id="dbt_build_and_test",
        bash_command=f"uv run --frozen wildfire transform --date {LOGICAL_DATE}",
        # A skipped snapshot day stops here. A failed day blocks later days from running
        # out of order, because the snapshot cannot insert a version into its past.
        depends_on_past=True,
        execution_timeout=timedelta(minutes=10),
    )

    extract >> transform
