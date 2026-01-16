from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

# =====================================================
# CONFIG
# =====================================================
PROJECT_ID = "dev-gcp-100"
REGION = "us-central1"

DATAFLOW_PY_FILE = "gs://banking-code/dataflow/cloudsql_to_gcs_cdc.py"
REQUIREMENTS_FILE = "gs://banking-code/dataflow/requirements.txt"

TEMP_LOCATION = "gs://banking-temp-dev/temp/"
STAGING_LOCATION = "gs://banking-temp-dev/staging/"

# =====================================================
# DAG DEFINITION
# =====================================================
with DAG(
    dag_id="banking_ingestion_dag",
    description="Cloud SQL → GCS CDC using custom Dataflow (Apache Beam)",
    start_date=days_ago(1),
    schedule_interval="0 1 * * *",   # Daily 1 AM
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": True
    },
    tags=["banking", "ingestion", "dataflow", "cdc"]
) as dag:

    start = EmptyOperator(task_id="start")

    cloudsql_cdc_dataflow = DataflowCreatePythonJobOperator(
        task_id="cloudsql_cdc_dataflow_job",
        py_file=DATAFLOW_PY_FILE,
        job_name="cloudsql-cdc-{{ ds_nodash }}",
        project_id=PROJECT_ID,
        location=REGION,
        options={
            "project": PROJECT_ID,
            "region": REGION,
            "temp_location": TEMP_LOCATION,
            "staging_location": STAGING_LOCATION,
            "runner": "DataflowRunner",
            "save_main_session": True
        },
        py_requirements=[REQUIREMENTS_FILE],
        py_interpreter="python3"
    )

    end = EmptyOperator(task_id="end")

    start >> cloudsql_cdc_dataflow >> end
