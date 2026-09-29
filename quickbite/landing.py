"""Landing zone: where the source system drops its daily files.

Layout (Hive-style partitions, one folder per extract date)::

    landing/<entity>/dt=YYYY-MM-DD/part-000.parquet
    landing/<entity>/dt=YYYY-MM-DD/_SUCCESS

The ``_SUCCESS`` marker is written *last*, so a reader never picks up a
half-written file: the Airflow sensor waits for the marker, not the data.

Local disk by default; ``WAREHOUSE=bigquery`` uploads to ``gs://$GCS_BUCKET``.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path

from quickbite.config import ENTITIES, SETTINGS
from quickbite.generator import extract_day

MARKER = "_SUCCESS"


def _schemas():
    import pyarrow as pa

    ts = pa.timestamp("us", tz="UTC")
    return {
        "restaurants": pa.schema([
            ("restaurant_id", pa.string()), ("name", pa.string()), ("zone", pa.string()),
            ("cuisine", pa.string()), ("commission_pct", pa.decimal128(5, 2)), ("is_active", pa.bool_()),
            ("created_at", ts), ("updated_at", ts), ("_extract_date", pa.date32()),
        ]),
        "customers": pa.schema([
            ("customer_id", pa.string()), ("zone", pa.string()), ("loyalty_tier", pa.string()),
            ("signup_ts", ts), ("updated_at", ts), ("_extract_date", pa.date32()),
        ]),
        "orders": pa.schema([
            ("order_id", pa.string()), ("customer_id", pa.string()), ("restaurant_id", pa.string()),
            ("order_ts", ts), ("status", pa.string()), ("items_count", pa.int32()),
            ("gross_amount", pa.decimal128(10, 2)), ("discount_amount", pa.decimal128(10, 2)),
            ("delivery_fee", pa.decimal128(10, 2)), ("payment_method", pa.string()),
            ("delivered_ts", ts), ("updated_at", ts), ("_extract_date", pa.date32()),
        ]),
    }


def partition(entity: str, d: date) -> str:
    return f"{entity}/dt={d.isoformat()}"


def local_path(entity: str, d: date) -> Path:
    return Path(SETTINGS.landing_dir) / partition(entity, d)


def gcs_uri(entity: str, d: date) -> str:
    return f"gs://{SETTINGS.gcs_bucket}/landing/{partition(entity, d)}/part-000.parquet"


def _utc(v):
    return v.replace(tzinfo=timezone.utc) if isinstance(v, datetime) and v.tzinfo is None else v


def write_extract(d: date) -> dict[str, int]:
    """Generate and land the three extracts for ``d``. Idempotent: re-running
    overwrites the same files with identical content."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    schemas = _schemas()
    counts = {}
    for entity, rows in extract_day(d).items():
        schema = schemas[entity]
        table = pa.Table.from_pylist([{k: _utc(v) for k, v in r.items()} for r in rows], schema=schema)
        folder = local_path(entity, d)
        folder.mkdir(parents=True, exist_ok=True)
        (folder / MARKER).unlink(missing_ok=True)
        tmp = folder / "part-000.parquet.tmp"
        pq.write_table(table, tmp)
        os.replace(tmp, folder / "part-000.parquet")  # atomic on the same filesystem

        if SETTINGS.warehouse == "bigquery":
            _upload_to_gcs(folder, entity, d)
        (folder / MARKER).touch()
        counts[entity] = table.num_rows
    return counts


def _upload_to_gcs(folder: Path, entity: str, d: date) -> None:
    from google.cloud import storage

    bucket = storage.Client(project=SETTINGS.gcp_project).bucket(SETTINGS.gcs_bucket)
    prefix = f"landing/{partition(entity, d)}"
    bucket.blob(f"{prefix}/part-000.parquet").upload_from_filename(str(folder / "part-000.parquet"))
    bucket.blob(f"{prefix}/{MARKER}").upload_from_string(b"")


def extract_complete(ds: str) -> bool:
    """Sensor check: are all extracts for ``ds`` fully landed?"""
    d = date.fromisoformat(ds)
    if SETTINGS.warehouse == "bigquery":
        from google.cloud import storage

        bucket = storage.Client(project=SETTINGS.gcp_project).bucket(SETTINGS.gcs_bucket)
        return all(bucket.blob(f"landing/{partition(e, d)}/{MARKER}").exists() for e in ENTITIES)
    return all((local_path(e, d) / MARKER).exists() for e in ENTITIES)


def file_row_count(entity: str, d: date) -> int:
    import pyarrow.parquet as pq

    return pq.ParquetFile(local_path(entity, d) / "part-000.parquet").metadata.num_rows
