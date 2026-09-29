# Design decisions

## Why SCD Type 2 with dbt snapshots (timestamp strategy)

Restaurants change commission rate; customers get upgraded to GOLD. If the
dimension were overwritten (Type 1), last quarter's commission revenue would
change every time a rate changed, and "revenue from GOLD customers" would
include orders placed while they were BRONZE.

The `timestamp` strategy uses the source's own `updated_at` as `valid_from`,
so history reflects when things changed in the business, not when the
pipeline happened to run. That matters during back-fills.

**Consequence:** snapshots must run in date order. The DAG enforces this with
`max_active_runs=1` and `depends_on_past=True` on `dbt_snapshot`.

**First captured version covers all earlier time.** History before the first
snapshot is unknown, so version 1 gets `valid_from = 1900-01-01`. Without
this, orders placed on the first day, before a customer's first captured
change, would match no version.

## Point-in-time joins instead of joining on the current row

`fct_orders` joins on `natural_key AND order_ts >= valid_from AND order_ts < valid_to`.
The half-open interval `[from, to)` guarantees exactly one match; a data test
(`assert_scd2_versions_do_not_overlap`) guards it.

## Unknown member (-1) for late-arriving dimensions

About half of new restaurants reach the catalogue extract a day after their
first orders. The options:

| Option | Problem |
|---|---|
| Inner join | Orders silently disappear from revenue |
| Fail the run | One late catalogue row blocks the whole company's reporting |
| **Unknown member + re-process** (chosen) | Orders are kept, point to `-1`, and are re-pointed on the next run |

The incremental model re-processes the last `late_arrival_days` (3) of orders
every run, which heals these rows automatically. A test fails if any order
older than the look-back still points at `-1`, so healing that stopped
working would be caught.

## Incremental fact with a look-back window

A full rebuild of `fct_orders` every day is simple but grows linearly in cost.
Incremental processing takes only orders updated since
`max(updated_at) - 3 days` and merges on `order_id`:

- **Late updates:** refunds arrive one or two days after the order.
- **Idempotency:** MERGE (BigQuery) / delete+insert (DuckDB) on `order_id`,
  so re-running a day produces the same table.
- **Trade-off:** a change older than 3 days would be missed. For anything
  older, run `dbt build --full-refresh -s fct_orders`, cheap at this size.

## Idempotent raw loads: replace the partition, never append

`DELETE WHERE _extract_date = D; INSERT ...` in one transaction (BigQuery
uses a multi-statement transaction). Appending would duplicate a day on
every retry. Reconciling file rows against loaded rows after the load
catches partial loads.

## Staging dedup uses the latest version, not DISTINCT

The orders extract is incremental: an order appears in every file where its
state changed, and the export occasionally duplicates rows. `row_number()`
over `order_id` ordered by `updated_at desc, _loaded_at desc` keeps exactly
one row: the latest. A dbt unit test pins this logic.

## dbt in its own virtualenv inside the Airflow image

Airflow pins hundreds of dependencies with a constraints file; dbt has its
own. Installing both in one environment is the most common source of broken
Airflow images. The image keeps dbt in `/opt/dbt-venv` and calls it with
`BashOperator`.

**Alternative considered:** Astronomer Cosmos renders every dbt model as an
Airflow task, which gives per-model retries and visibility. It's a good
choice at larger scale; here, one `dbt build` task keeps the DAG readable.

## DuckDB locally, BigQuery in the cloud, same models

DuckDB gives a zero-cost, zero-setup warehouse that runs in CI. The SQL
differences (interval arithmetic, day of week) are isolated in
`transform/macros/cross_db.sql` using `adapter.dispatch`. BigQuery-only
features (partitioning, clustering, MERGE) are switched on by
`target.type` in model configs.

DuckDB allows one writer per database file, so warehouse tasks share an
Airflow pool with one slot.

## Partitioning and clustering (BigQuery)

`fct_orders` is partitioned by `order_date` (dashboards almost always filter
by date) and clustered by `restaurant_id, customer_id` (the most common
secondary filters). On-demand BigQuery bills by bytes scanned, so a one-day
query reads one partition instead of the whole table. `make cost-report`
measures it with free dry runs.

## Terraform: least privilege

The pipeline's service account gets `objectAdmin` on its own bucket,
`dataEditor` on its four datasets and `jobUser` on the project. It has no
project-wide Editor role. `environment = dev` allows `terraform destroy` to
delete non-empty datasets; `prod` refuses.

## What I'd change at scale

- **Orchestration:** Cosmos (per-model tasks) or dbt Cloud, and data-aware
  scheduling (Airflow datasets) instead of a fixed time.
- **Late data beyond 3 days:** a daily `insert_overwrite` of the last N date
  partitions on BigQuery rather than a MERGE.
- **Contracts:** enforce dbt model contracts on the marts, which consumers
  depend on.
- **Observability:** send run and test results (`run_results.json`) to a
  table and alert on trends: test failure rate, row-count drift, freshness.
