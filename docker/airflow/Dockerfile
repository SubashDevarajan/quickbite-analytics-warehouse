FROM apache/airflow:2.10.5-python3.11

# dbt lives in its own virtualenv: the recommended way to run dbt from
# Airflow without dependency conflicts.
USER root
COPY requirements/dbt.txt /tmp/dbt.txt
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dbt.txt \
 && mkdir -p /opt/quickbite/data \
 && chown -R airflow:0 /opt/quickbite && chmod -R g+rwX /opt/quickbite

USER airflow
COPY requirements/airflow.txt /tmp/airflow.txt
RUN pip install --no-cache-dir -r /tmp/airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"

ENV PYTHONPATH=/opt/quickbite \
    QB_DATA_DIR=/opt/quickbite/data \
    DUCKDB_PATH=/opt/quickbite/data/warehouse.duckdb \
    DBT_PROFILES_DIR=/opt/quickbite/transform
