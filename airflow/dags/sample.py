import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

def print_composer_bucket():
    composer_bucket = os.environ.get("GCS_BUCKET")
    logging.info("COMPOSER_BUCKET = %s", composer_bucket)

    gcs_python_script = f"gs://{composer_bucket}/data/dataflow/cloudsql_cdc_pipeline.py"
    gcs_requirements = f"gs://{composer_bucket}/data/dataflow/requirements.txt"

    logging.info("GCS_PYTHON_SCRIPT = %s", gcs_python_script)
    logging.info("GCS_REQUIREMENTS_FILE = %s", gcs_requirements)

with DAG(
    dag_id="check_composer_bucket_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["debug", "composer"],
) as dag:

    check_bucket = PythonOperator(
        task_id="print_composer_bucket",
        python_callable=print_composer_bucket,
    )
