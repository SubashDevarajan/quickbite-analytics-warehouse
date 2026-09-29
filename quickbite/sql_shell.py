"""Query the local DuckDB warehouse.

    make sql                                   # interactive
    make sql Q="select * from marts.agg_daily_zone_metrics limit 5"

Opens the database read-only. If a pipeline run is writing at that moment,
DuckDB refuses the connection (single writer); wait for the run to finish.
"""

from __future__ import annotations

import sys

import duckdb

from quickbite.config import SETTINGS

con = duckdb.connect(SETTINGS.duckdb_path, read_only=True)
con.execute("SET TimeZone = 'UTC'")


def q(sql: str, n: int = 30) -> None:
    """Run SQL and print the first n rows."""
    con.sql(sql).show(max_rows=n)


if __name__ == "__main__" and len(sys.argv) > 1:
    q(" ".join(sys.argv[1:]))
    sys.exit(0)

print("tables:")
q("select table_schema, table_name from information_schema.tables "
  "where table_schema in ('raw','staging','snapshots','marts') order by 1, 2", n=50)
print('try: q("select * from marts.agg_daily_zone_metrics order by order_date desc limit 10")')
