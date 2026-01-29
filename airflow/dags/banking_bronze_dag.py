import os
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

# =====================================================
# CONFIG
# =====================================================
PROJECT_ID = "dev-gcp-100"
REGION = "us-east1"

# Unique cluster name (prevents collision)
CLUSTER_NAME = "dev-dataproc-{{ ds_nodash }}"

# Composer bucket
composer_bucket = os.environ["GCS_BUCKET"]
PYSPARK_MAIN = f"gs://{composer_bucket}/data/dataproc/bronze_gcs_to_bq.py"
logging.info("GCS_PYTHON_SCRIPT = %s", PYSPARK_MAIN)

# =====================================================
# DEFAULT ARGS
# =====================================================
DEFAULT_ARGS = {
    "owner": "data-engineering"
}

# =====================================================
# CLUSTER CONFIG
# =====================================================
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {
            "boot_disk_type": "pd-balanced",
            "boot_disk_size_gb": 50,
        },
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {
            "boot_disk_type": "pd-balanced",
            "boot_disk_size_gb": 100,
        },
    },
    "software_config": {
        "image_version": "2.1-debian11",  # stable
    },
    "lifecycle_config": {
        "auto_delete_ttl": {"seconds": 7200}, # Auto-delete safety net (2 hours)
    },
}

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="banking_bronze_dag",
    description="Ephemeral Dataproc cluster for PySpark ingestion",
    schedule_interval=None,     # Manual / CI-CD trigger
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dataproc", "pyspark", "gcs", "bigquery"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    submit_pyspark = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {"main_python_file_uri": PYSPARK_MAIN},
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",  # 🔥 always delete
    )

    create_cluster >> submit_pyspark >> delete_cluster