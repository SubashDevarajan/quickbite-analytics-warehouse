"""Runtime configuration, all overridable with environment variables.

WAREHOUSE=duckdb   (default) everything runs locally, free
WAREHOUSE=bigquery landing files go to GCS and tables to BigQuery
"""

from __future__ import annotations

import os
from dataclasses import dataclass

ENTITIES = ("restaurants", "customers", "orders")


@dataclass(frozen=True)
class Settings:
    warehouse: str = os.getenv("WAREHOUSE", "duckdb")
    data_dir: str = os.getenv("QB_DATA_DIR", "data")
    duckdb_path: str = os.getenv("DUCKDB_PATH", os.path.join(os.getenv("QB_DATA_DIR", "data"), "warehouse.duckdb"))
    gcp_project: str = os.getenv("GCP_PROJECT", "")
    gcs_bucket: str = os.getenv("GCS_BUCKET", "")
    bq_location: str = os.getenv("BQ_LOCATION", "asia-south1")

    @property
    def landing_dir(self) -> str:
        return os.path.join(self.data_dir, "landing")


SETTINGS = Settings()
