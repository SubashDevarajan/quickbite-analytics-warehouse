"""
### QuickBite daily warehouse

Builds the analytics warehouse for one business day (`ds`):

1. **extract_source**: the source system lands its three daily extracts (simulated here).
2. **wait_for_extract**: sensor; waits for every `_SUCCESS` marker, never a half-written file.
3. **load_raw**: replaces that day's partition in the `raw` tables (idempotent).
4. **reconcile_row_counts**: fails the run if any table holds a different number of rows than its file.
5. **dbt_source_freshness**: warns or fails if raw data is stale.
6. **dbt_snapshot**: SCD Type 2 history for customers and restaurants. `depends_on_past`, because history must be built in date order.
7. **dbt_build**: staging, dimensions, incremental fact, marts, and every data test and unit test.

Every step is idempotent, so retries, `airflow tasks clear` and back-fills are safe.
Warehouse-writing tasks share the `warehouse` pool (1 slot): DuckDB allows a single writer.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

from quickbite.landing import extract_complete

log = logging.getLogger(__name__)

DBT = "cd /opt/quickbite/transform && /opt/dbt-venv/bin/dbt"
DBT_VARS = "--vars '{\"run_date\": \"{{ ds }}\"}'"
START_DATE = pendulum.parse(os.getenv("QB_START_DATE", "2026-09-01"), tz="UTC")


def on_failure(context) -> None:
    """Hook for alerting. In production this would post to Slack / PagerDuty."""
    ti = context["task_instance"]
    log.error("ALERT: %s.%s failed for %s (try %s). Logs: %s",
              ti.dag_id, ti.task_id, context["ds"], ti.try_number, ti.log_url)


@dag(
    dag_id="quickbite_daily_warehouse",
    schedule="@daily",
    start_date=START_DATE,
    catchup=True,          # a new deployment back-fills every day since START_DATE
    max_active_runs=1,     # days are processed strictly in order (SCD2 history depends on it)
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": on_failure,
    },
    tags=["quickbite", "dbt", "warehouse"],
    doc_md=__doc__,
)
def quickbite_daily_warehouse():
    @task
    def extract_source(ds: str | None = None) -> dict:
        from datetime import date

        from quickbite import landing

        return landing.write_extract(date.fromisoformat(ds))

    wait_for_extract = PythonSensor(
        task_id="wait_for_extract",
        python_callable=extract_complete,
        op_kwargs={"ds": "{{ ds }}"},
        mode="reschedule",   # free the worker slot between checks
        poke_interval=30,
        timeout=60 * 60,
    )

    @task(pool="warehouse")
    def load_raw(ds: str | None = None) -> dict:
        from quickbite import loader

        return loader.load_day(ds)

    @task
    def reconcile_row_counts(counts: dict) -> None:
        from quickbite import loader

        loader.reconcile(counts)
        log.info("row counts reconciled: %s", counts)

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT} source freshness",
        pool="warehouse",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT} snapshot {DBT_VARS}",
        depends_on_past=True,
        pool="warehouse",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build --exclude resource_type:snapshot {DBT_VARS}",
        pool="warehouse",
    )

    loaded = load_raw()
    extract_source() >> wait_for_extract >> loaded
    reconcile_row_counts(loaded) >> dbt_source_freshness >> dbt_snapshot >> dbt_build


quickbite_daily_warehouse()
