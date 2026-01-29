import logging
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.empty import EmptyOperator

from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator

from airflow.configuration import conf
from pathlib import Path

# =====================================================
# CONFIG
# =====================================================
PROJECT_ID = "dev-gcp-100"
REGION = "us-central1"

composer_bucket = os.environ.get("GCS_BUCKET")
logging.info("COMPOSER_BUCKET = %s", composer_bucket)

GCS_PYTHON_SCRIPT = f"gs://{composer_bucket}/data/dataflow/cloudsql_cdc_pipeline.py"
logging.info("GCS_PYTHON_SCRIPT = %s", GCS_PYTHON_SCRIPT)

TEMP_LOCATION = "gs://banking-temp-dev/temp/"
STAGING_LOCATION = "gs://banking-temp-dev/staging/"

DEFAULT_ARGS = {
    "owner": "data-engineering"
}

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="banking_ingestion_dag",
    description="Cloud SQL → GCS CDC using Apache Beam on Dataflow",
    start_date=days_ago(1),
    schedule_interval="0 10 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["banking", "cdc", "dataflow"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_dataflow = BeamRunPythonPipelineOperator(
        task_id="cloudsql_cdc_dataflow_job",
        runner=BeamRunnerType.DataflowRunner,
        py_file=GCS_PYTHON_SCRIPT,
        py_interpreter="python3",
        py_system_site_packages=False,
        py_options=[],
        pipeline_options={},
        py_requirements=[
            "apache-beam[gcp]",
            "cloud-sql-python-connector[pymysql]",
            "sqlalchemy",
            "pymysql",
            "pyarrow"
        ],
        dataflow_config={
            "location": REGION,
            "job_name": "cloudsql-cdc-{{ ds_nodash }}",
        },
    )

    end = EmptyOperator(task_id="end")

    start >> run_dataflow >> end
