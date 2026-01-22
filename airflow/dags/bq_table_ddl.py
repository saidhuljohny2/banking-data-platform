import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime

# =========================================================
# CONFIGURATION
# =========================================================
PROJECT_ID = "dev-gcp-100"
METADATA_DATASET = "banking_metadata"
TABLE_DETAILS = "table_details"

# BigQuery dataset location (MANDATORY in Composer 2)
BQ_LOCATION = "US"

# Composer GCS bucket (auto injected)
COMPOSER_BUCKET = os.environ["GCS_BUCKET"]

# Base path INSIDE the bucket (do NOT prefix gs://)
DDL_BASE_PATH = "data/bigquery/table_ddl"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1
}

# =========================================================
# TASK 1: FETCH TABLES TO CREATE
# =========================================================
def fetch_tables_to_create(**context):
    """
    Reads banking_metadata.table_details and fetches
    only tables marked create_table = TRUE
    """
    bq = BigQueryHook(
        use_legacy_sql=False,
        location=BQ_LOCATION
    )

    query = f"""
        SELECT dataset, table
        FROM `{METADATA_DATASET}.{TABLE_DETAILS}`
        WHERE create_table = TRUE
    """

    records = bq.get_records(query)

    if not records:
        print("ℹ️ No tables marked for creation")
    else:
        print(f"📌 Tables to create: {records}")

    context["ti"].xcom_push(
        key="tables_to_create",
        value=records
    )

# =========================================================
# TASK 2: CREATE TABLES USING DDL FROM GCS
# =========================================================
def create_tables_from_gcs_ddl(**context):
    """
    Reads DDL SQL files from GCS and executes them in BigQuery
    """
    tables = context["ti"].xcom_pull(
        key="tables_to_create"
    )

    if not tables:
        print("✅ No tables to process")
        return

    bq = BigQueryHook(
        use_legacy_sql=False,
        location=BQ_LOCATION
    )

    gcs = GCSHook()

    for dataset, table in tables:
        ddl_object_path = f"{DDL_BASE_PATH}/{dataset}/{table}.sql"

        print("--------------------------------------------------")
        print(f"🚀 Creating table: {dataset}.{table}")
        print(f"📄 DDL Path: gs://{COMPOSER_BUCKET}/{ddl_object_path}")

        # Read DDL from GCS
        ddl_sql = gcs.download(
            bucket_name=COMPOSER_BUCKET,
            object_name=ddl_object_path
        ).decode("utf-8")

        # Execute CREATE TABLE IF NOT EXISTS
        bq.run(ddl_sql)

        # Update create_timestamp after successful creation
        update_sql = f"""
            UPDATE `{METADATA_DATASET}.{TABLE_DETAILS}`
            SET create_timestamp = CURRENT_TIMESTAMP()
            WHERE dataset = '{dataset}'
              AND table = '{table}'
        """
        bq.run(update_sql)

        print(f"✅ Successfully processed {dataset}.{table}")

# =========================================================
# DAG DEFINITION
# =========================================================
with DAG(
    dag_id="bq_metadata_driven_table_creation",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,     # Manual / CI-CD trigger
    catchup=False,
    default_args=default_args,
    tags=["bigquery", "ddl", "metadata", "banking"]
) as dag:

    fetch_tables = PythonOperator(
        task_id="fetch_tables_to_create",
        python_callable=fetch_tables_to_create
    )

    create_tables = PythonOperator(
        task_id="create_tables_from_gcs_ddl",
        python_callable=create_tables_from_gcs_ddl
    )

    fetch_tables >> create_tables
