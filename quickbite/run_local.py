"""Run the whole daily pipeline for a range of days *without* Airflow.

Same steps, same order as the Airflow DAG:
    extract -> load raw -> reconcile -> dbt snapshot -> dbt build

Used by CI (end-to-end test on DuckDB) and handy for quick local runs:

    python -m quickbite.run_local --start 2026-09-01 --days 5
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

from quickbite import landing, loader
from quickbite.config import SETTINGS

log = logging.getLogger("quickbite.run_local")
DBT_DIR = Path(__file__).resolve().parent.parent / "transform"


def dbt(*args: str, dbt_bin: str = "dbt") -> None:
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": str(DBT_DIR),
        "DUCKDB_PATH": os.path.abspath(SETTINGS.duckdb_path),
        "DBT_TARGET": os.getenv("DBT_TARGET", SETTINGS.warehouse),
    }
    cmd = [dbt_bin, *args]
    log.info("$ %s", " ".join(cmd))
    subprocess.run(cmd, cwd=DBT_DIR, env=env, check=True)


def run_day(d: date, dbt_bin: str = "dbt") -> dict:
    ds = d.isoformat()
    landing.write_extract(d)
    counts = loader.load_day(ds)
    loader.reconcile(counts)
    dbt_vars = f'{{"run_date": "{ds}"}}'
    dbt("snapshot", "--vars", dbt_vars, dbt_bin=dbt_bin)
    dbt("build", "--exclude", "resource_type:snapshot", "--vars", dbt_vars, dbt_bin=dbt_bin)
    return counts


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--start", default=(date.today() - timedelta(days=6)).isoformat())
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--dbt-bin", default=os.getenv("DBT_BIN", "dbt"))
    args = p.parse_args()

    Path(SETTINGS.data_dir).mkdir(parents=True, exist_ok=True)
    if not (DBT_DIR / "dbt_packages").exists():
        dbt("deps", dbt_bin=args.dbt_bin)

    start = date.fromisoformat(args.start)
    for i in range(args.days):
        d = start + timedelta(days=i)
        log.info("===== %s =====", d)
        log.info("counts: %s", run_day(d, args.dbt_bin))
    return 0


if __name__ == "__main__":
    sys.exit(main())
