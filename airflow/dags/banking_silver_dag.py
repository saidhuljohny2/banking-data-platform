import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import timedelta
from google.cloud import storage

# =====================================================
# CONFIG
# =====================================================
PROJECT_ID = "dev-gcp-100"
BQ_LOCATION = "US"

COMPOSER_BUCKET = os.environ["GCS_BUCKET"]
silver_SQL_PATH = f"gs://{COMPOSER_BUCKET}/data/bigquery/silver"

DEFAULT_ARGS = {
    "owner": "data-engineering"
}

# =====================================================
# HELPERS
# =====================================================
def read_sql_from_gcs(gcs_uri: str) -> str:
    client = storage.Client()
    bucket, blob_path = gcs_uri.replace("gs://", "").split("/", 1)
    return client.bucket(bucket).blob(blob_path).download_as_text()

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="banking_silver_dag",
    description="Silver → silver (Business KPIs & Analytics) using BigQuery SQL",
    start_date=days_ago(1),
    schedule_interval=None,          # triggered after Silver
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["banking", "silver", "bigquery", "analytics"],
) as dag:

    # -------------------------------------------------
    # 1. DISCOVER silver SQL FILES
    # -------------------------------------------------
    @task
    def discover_sql_files():
        client = storage.Client()
        bucket_name, prefix = silver_SQL_PATH.replace("gs://", "").split("/", 1)

        blobs = client.list_blobs(bucket_name, prefix=prefix)

        sql_files = []
        for blob in blobs:
            if blob.name.endswith(".sql"):
                sql_files.append({
                    "file": blob.name.split("/")[-1],
                    "gcs_path": f"gs://{bucket_name}/{blob.name}",
                })

        if not sql_files:
            raise ValueError("No SQL files found for silver layer")

        return sql_files

    sql_files = discover_sql_files()

    # -------------------------------------------------
    # 2. EXECUTE silver SQL (PARALLEL – SAFE)
    # -------------------------------------------------
    run_silver_sql = BigQueryInsertJobOperator.partial(
        task_id="run_silver_sql",
        location=BQ_LOCATION,
    ).expand(
        configuration=sql_files.map(
            lambda s: {
                "query": {
                    "query": read_sql_from_gcs(s["gcs_path"]),
                    "useLegacySql": False,
                },
                "labels": {
                    "layer": "silver",
                    "domain": "banking",
                    "env": "dev",
                },
            }
        )
    )

    sql_files >> run_silver_sql
