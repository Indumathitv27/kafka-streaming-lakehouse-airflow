from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

BRONZE_PATH = "/opt/airflow/data/bronze/orders"

def check_bronze_has_files():
    if not os.path.exists(BRONZE_PATH):
        raise FileNotFoundError(f"Bronze path not found: {BRONZE_PATH}")
    files = [f for f in os.listdir(BRONZE_PATH) if f.endswith(".parquet")]
    if len(files) == 0:
        raise RuntimeError("No bronze parquet files found yet.")
    return f"Found {len(files)} bronze parquet files."

def create_trigger(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write("run")
    return f"Created trigger: {path}"

default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="kafka_streaming_lakehouse_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # manual until verified
    catchup=False,
    tags=["kafka", "spark", "lakehouse"],
) as dag:

    bronze_check = PythonOperator(
        task_id="check_bronze_has_data",
        python_callable=check_bronze_has_files,
    )

    trigger_silver = PythonOperator(
        task_id="trigger_silver_job",
        python_callable=create_trigger,
        op_args=["/opt/airflow/data/_run_silver"],
    )

    trigger_gold = PythonOperator(
        task_id="trigger_gold_job",
        python_callable=create_trigger,
        op_args=["/opt/airflow/data/_run_gold"],
    )

    bronze_check >> trigger_silver >> trigger_gold
