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
SILVER_SQL_PATH = f"gs://{COMPOSER_BUCKET}/data/bigquery/silver"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
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
    description="Bronze → Silver (SCD Type 2 + Facts) using BigQuery SQL",
    start_date=days_ago(1),
    schedule_interval=None,          # triggered by upstream DAG
    catchup=False,
    max_active_runs=1,               # banking safety
    default_args=DEFAULT_ARGS,
    sla=timedelta(hours=1),          # SLA monitoring
    tags=["banking", "silver", "bigquery", "scd"],
) as dag:

    # -------------------------------------------------
    # 1. DISCOVER & ORDER SQL FILES
    # -------------------------------------------------
    @task
    def discover_sql_files():
        client = storage.Client()
        bucket_name, prefix = SILVER_SQL_PATH.replace("gs://", "").split("/", 1)

        blobs = list(client.list_blobs(bucket_name, prefix=prefix))

        sql_files = []
        for blob in blobs:
            if blob.name.endswith(".sql"):
                sql_files.append({
                    "file": blob.name.split("/")[-1],
                    "gcs_path": f"gs://{bucket_name}/{blob.name}"
                })

        if not sql_files:
            raise ValueError("No SQL files found for Silver layer")

        # 🔥 CRITICAL: enforce deterministic execution order
        sql_files.sort(key=lambda x: x["file"])

        return sql_files

    ordered_sql_files = discover_sql_files()

    # -------------------------------------------------
    # 2. EXECUTE SQL SEQUENTIALLY (BEST PRACTICE)
    # -------------------------------------------------
    @task
    def execute_sql(sql_file: dict):
        sql_text = read_sql_from_gcs(sql_file["gcs_path"])

        return BigQueryInsertJobOperator(
            task_id=f"run_{sql_file['file'].replace('.sql', '')}",
            configuration={
                "query": {
                    "query": sql_text,
                    "useLegacySql": False,
                },
                "labels": {
                    "layer": "silver",
                    "domain": "banking",
                    "env": "dev",
                },
            },
            location=BQ_LOCATION,
        )

    # Build ordered execution chain
    previous_task = None
    for sql_file in ordered_sql_files:
        current_task = execute_sql.override(
            task_id=f"run_{sql_file['file'].replace('.sql', '')}"
        )(sql_file)

        if previous_task:
            previous_task >> current_task

        previous_task = current_task
