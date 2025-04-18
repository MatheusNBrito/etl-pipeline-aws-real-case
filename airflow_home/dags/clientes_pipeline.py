from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "theuzao",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="clientes_pipeline",
    default_args=default_args,
    description="Pipeline de ETL para clientes: raw > processed > gold",
    schedule=None,
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=["etl", "clientes"],
) as dag:  # 👈 já está nomeando como `dag`

    raw_to_processed = BashOperator(
        task_id="raw_to_processed",
        bash_command="python /app/src/main/datapipelines/generate_clientes/commons/etl_steps.py",
    )

    processed_to_gold = BashOperator(
        task_id="processed_to_gold",
        bash_command="python /app/src/main/datapipelines/generate_clientes/commons/etl_steps_gold.py",
    )

    raw_to_processed >> processed_to_gold

dag = dag

