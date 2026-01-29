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

composer_bucket = os.environ["GCS_BUCKET"]

STREAMING_PIPELINE_PY = f"gs://{composer_bucket}/data/dataflow/streaming_transactions_pipeline.py"
logging.info("STREAMING_PIPELINE_PY = %s", STREAMING_PIPELINE_PY)

TEMP_LOCATION = "gs://banking-temp-dev/temp/"
STAGING_LOCATION = "gs://banking-temp-dev/staging/"

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="banking_streaming_ingestion_dag",
    description="Start Dataflow streaming pipeline for banking transactions",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 0, # fail fast
        "email_on_failure": True,
    },
    tags=["banking", "streaming", "dataflow"],
) as dag:

    start = EmptyOperator(task_id="start")

    start_streaming_job = BeamRunPythonPipelineOperator(
        task_id="start_streaming_dataflow_job",
        runner=BeamRunnerType.DataflowRunner,
        py_file=STREAMING_PIPELINE_PY,
        py_interpreter="python3",
        py_system_site_packages=False,
        py_options=[],
        pipeline_options={},
        py_requirements=[
            "apache-beam[gcp]",
        ],
        dataflow_config={
            "location": REGION,
            "job_name": "banking-streaming-transactions",  # FIXED NAME
        },
    )

    end = EmptyOperator(task_id="end")

    start >> start_streaming_job >> end
