"""Load landed files into the warehouse ``raw`` layer, idempotently.

Each day's extract replaces exactly that day's partition
(``DELETE WHERE _extract_date = D`` + ``INSERT`` in one transaction).
Running the same day twice, or back-filling it a month later, leaves the
table in the same state as running it once. That is what makes Airflow
retries and backfills safe.

Raw tables keep the source data as-is plus two audit columns:
``_extract_date`` (which file it came from) and ``_loaded_at`` (when).
"""

from __future__ import annotations

import logging
from datetime import date

from quickbite.config import ENTITIES, SETTINGS
from quickbite.landing import file_row_count, gcs_uri, local_path

log = logging.getLogger(__name__)


class ReconciliationError(RuntimeError):
    pass


def load_day(ds: str) -> dict[str, dict[str, int]]:
    d = date.fromisoformat(ds)
    if SETTINGS.warehouse == "bigquery":
        loaded = _load_bigquery(d)
    else:
        loaded = _load_duckdb(d)
    result = {e: {"file_rows": file_row_count(e, d), "loaded_rows": loaded[e]} for e in ENTITIES}
    log.info("loaded %s: %s", ds, result)
    return result


def reconcile(counts: dict[str, dict[str, int]]) -> None:
    """Fail the run if any table does not hold exactly the rows in its file."""
    bad = {e: c for e, c in counts.items() if c["file_rows"] != c["loaded_rows"]}
    if bad:
        raise ReconciliationError(f"row counts do not match the landed files: {bad}")


# -------------------------------------------------------------------- DuckDB
def _load_duckdb(d: date) -> dict[str, int]:
    import duckdb

    con = duckdb.connect(SETTINGS.duckdb_path)
    try:
        con.execute("SET TimeZone = 'UTC'")
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        loaded = {}
        for entity in ENTITIES:
            path = str(local_path(entity, d) / "part-000.parquet").replace("'", "''")
            src = f"read_parquet('{path}')"
            con.execute(
                f"CREATE TABLE IF NOT EXISTS raw.{entity} AS "
                f"SELECT *, current_timestamp AS _loaded_at FROM {src} WHERE false"
            )
            con.execute("BEGIN TRANSACTION")
            con.execute(f"DELETE FROM raw.{entity} WHERE _extract_date = ?", [d])
            # BY NAME: match columns by name, so a reordered file cannot shift values.
            con.execute(f"INSERT INTO raw.{entity} BY NAME SELECT *, current_timestamp AS _loaded_at FROM {src}")
            con.execute("COMMIT")
            loaded[entity] = con.execute(
                f"SELECT count(*) FROM raw.{entity} WHERE _extract_date = ?", [d]
            ).fetchone()[0]
        return loaded
    finally:
        con.close()


# ------------------------------------------------------------------ BigQuery
def _load_bigquery(d: date) -> dict[str, int]:
    from google.cloud import bigquery

    client = bigquery.Client(project=SETTINGS.gcp_project, location=SETTINGS.bq_location)
    raw = f"{SETTINGS.gcp_project}.raw"
    loaded = {}
    for entity in ENTITIES:
        tmp = f"{raw}._tmp_{entity}_{d:%Y%m%d}"
        client.load_table_from_uri(
            gcs_uri(entity, d),
            tmp,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                decimal_target_types=["NUMERIC", "BIGNUMERIC"],
            ),
        ).result()
        target = f"`{raw}.{entity}`"
        client.query(
            f"""
            CREATE TABLE IF NOT EXISTS {target}
            PARTITION BY _extract_date
            AS SELECT *, CURRENT_TIMESTAMP() AS _loaded_at FROM `{tmp}` WHERE FALSE;

            BEGIN TRANSACTION;
            DELETE FROM {target} WHERE _extract_date = DATE '{d.isoformat()}';
            INSERT INTO {target} SELECT *, CURRENT_TIMESTAMP() AS _loaded_at FROM `{tmp}`;
            COMMIT TRANSACTION;
            """
        ).result()
        client.delete_table(tmp, not_found_ok=True)
        loaded[entity] = next(iter(client.query(
            f"SELECT COUNT(*) AS n FROM {target} WHERE _extract_date = DATE '{d.isoformat()}'"
        ).result()))["n"]
    return loaded
