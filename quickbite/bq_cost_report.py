"""BigQuery cost check: how much does partitioning + clustering save?

BigQuery on-demand pricing charges by bytes scanned. This script makes an
unpartitioned copy of marts.fct_orders (once), then *dry-runs* the same
typical dashboard queries against both tables. Dry runs are free and return
the exact bytes a query would scan.

    WAREHOUSE=bigquery GCP_PROJECT=my-proj python -m quickbite.bq_cost_report

Prints a before/after table for the README. Only for WAREHOUSE=bigquery.
"""

from __future__ import annotations

import argparse

from google.cloud import bigquery

from quickbite.config import SETTINGS

QUERIES = {
    "GMV for one day": (
        "SELECT SUM(net_amount) FROM `{t}` "
        "WHERE order_date = (SELECT MAX(order_date) FROM `{t}`) AND status = 'DELIVERED'"
    ),
    "Last 7 days, one restaurant": (
        "SELECT order_date, COUNT(*) FROM `{t}` "
        "WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND restaurant_id = 'R0007' "
        "GROUP BY order_date"
    ),
    "Full scan (no filter)": "SELECT SUM(net_amount) FROM `{t}`",
}


def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--usd-per-tib", type=float, default=6.25,
                   help="on-demand price per TiB scanned; check current GCP pricing for your region")
    args = p.parse_args()

    client = bigquery.Client(project=SETTINGS.gcp_project, location=SETTINGS.bq_location)
    optimized = f"{SETTINGS.gcp_project}.marts.fct_orders"
    baseline = f"{SETTINGS.gcp_project}.marts.fct_orders_unpartitioned"
    client.query(f"CREATE TABLE IF NOT EXISTS `{baseline}` AS SELECT * FROM `{optimized}`").result()

    dry = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    print(f"{'query':32s} {'unpartitioned':>14s} {'partitioned+clustered':>22s} {'saved':>7s}")
    for name, sql in QUERIES.items():
        before = client.query(sql.format(t=baseline), job_config=dry).total_bytes_processed
        after = client.query(sql.format(t=optimized), job_config=dry).total_bytes_processed
        saved = 1 - after / before if before else 0
        print(f"{name:32s} {human(before):>14s} {human(after):>22s} {saved:>6.0%}")
    print("\nDry-run estimates. Note: clustering savings show up in *billed* bytes after the query runs,"
          " and dry runs report an upper bound for clustered tables.")
    print(f"At ${args.usd_per_tib}/TiB, 1 TB of avoided scanning saves about ${args.usd_per_tib * 0.909:.2f}.")


if __name__ == "__main__":
    main()
