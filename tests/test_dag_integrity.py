"""DAG integrity: the DAG file imports, has no cycles, and is wired as designed.
Runs in CI with Airflow installed; skipped elsewhere."""

from __future__ import annotations

from pathlib import Path

import pytest

# Not just "airflow": the repo's own airflow/ folder imports as an empty namespace package.
pytest.importorskip("airflow.models")

DAG_FOLDER = Path(__file__).resolve().parent.parent / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_pipeline_shape(dagbag):
    dag = dagbag.get_dag("quickbite_daily_warehouse")
    assert dag is not None
    assert dag.max_active_runs == 1 and dag.catchup
    order = ["extract_source", "wait_for_extract", "load_raw", "reconcile_row_counts",
             "dbt_source_freshness", "dbt_snapshot", "dbt_build"]
    for upstream, downstream in zip(order, order[1:], strict=False):
        assert downstream in dag.get_task(upstream).downstream_task_ids, (upstream, downstream)


def test_warehouse_writers_share_the_single_slot_pool(dagbag):
    dag = dagbag.get_dag("quickbite_daily_warehouse")
    for task_id in ["load_raw", "dbt_source_freshness", "dbt_snapshot", "dbt_build"]:
        assert dag.get_task(task_id).pool == "warehouse"
    assert dag.get_task("dbt_snapshot").depends_on_past
