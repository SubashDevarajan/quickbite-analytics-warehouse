"""Landing + loading into DuckDB: idempotency and reconciliation."""

from __future__ import annotations

import importlib
from datetime import date

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

D = date(2026, 9, 10)


@pytest.fixture
def modules(tmp_path, monkeypatch):
    monkeypatch.setenv("QB_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "wh.duckdb"))
    monkeypatch.setenv("WAREHOUSE", "duckdb")
    from quickbite import config, landing, loader

    for m in (config, landing, loader):
        importlib.reload(m)
    return landing, loader


def test_load_is_idempotent(modules):
    import duckdb

    landing, loader = modules
    landing.write_extract(D)
    assert landing.extract_complete(D.isoformat())

    first = loader.load_day(D.isoformat())
    loader.reconcile(first)
    second = loader.load_day(D.isoformat())  # a retry or a back-fill of the same day
    assert first == second

    con = duckdb.connect(loader.SETTINGS.duckdb_path, read_only=True)
    n = con.execute("select count(*) from raw.orders").fetchone()[0]
    assert n == first["orders"]["file_rows"]


def test_reconcile_detects_mismatch(modules):
    _, loader = modules
    with pytest.raises(loader.ReconciliationError):
        loader.reconcile({"orders": {"file_rows": 10, "loaded_rows": 9}})


def test_sensor_waits_for_marker(modules):
    landing, _ = modules
    assert not landing.extract_complete(D.isoformat())
