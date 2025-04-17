from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 17),
    'retries': 1
}

with DAG(
    dag_id='clientes_pipeline',
    default_args=default_args,
    description='Pipeline ETL de clientes: raw → processed → gold',
    schedule_interval=None,  # Pode trocar depois por '@daily' ou outro
    catchup=False,
    tags=['etl', 'clientes']
) as dag:

    def run_raw_to_processed():
        subprocess.run(
            ["python3", "/opt/airflow/scripts/clientes/raw_to_processed.py"],
            check=True
        )

    def run_processed_to_gold():
        subprocess.run(
            ["python3", "/opt/airflow/scripts/clientes/processed_to_gold.py"],
            check=True
        )

    raw_to_processed = PythonOperator(
        task_id='raw_to_processed_clientes',
        python_callable=run_raw_to_processed
    )

    processed_to_gold = PythonOperator(
        task_id='processed_to_gold_clientes',
        python_callable=run_processed_to_gold
    )

    raw_to_processed >> processed_to_gold
