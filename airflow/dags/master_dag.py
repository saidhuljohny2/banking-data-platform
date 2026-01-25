from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta

# =====================================================
# CONFIG
# =====================================================
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 0,  # master should fail fast
}

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="banking_master_dag",
    description="Master DAG orchestrating Banking Data Platform",
    start_date=days_ago(1),
    schedule_interval="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["banking", "master", "orchestration"],
) as dag:

    start = EmptyOperator(task_id="start")

    # -------------------------------------------------
    # INGESTION
    # -------------------------------------------------
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion_dag",
        trigger_dag_id="banking_ingestion_dag",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    # -------------------------------------------------
    # BRONZE
    # -------------------------------------------------
    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze_dag",
        trigger_dag_id="banking_bronze_dag",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    # -------------------------------------------------
    # SILVER
    # -------------------------------------------------
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="banking_silver_dag",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    # -------------------------------------------------
    # GOLD
    # -------------------------------------------------
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="banking_gold_dag",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    end = EmptyOperator(task_id="end")

    # =====================================================
    # DEPENDENCY CHAIN
    # =====================================================
    (
        start
        >> trigger_ingestion
        >> trigger_bronze
        >> trigger_silver
        >> trigger_gold
        >> end
    )
