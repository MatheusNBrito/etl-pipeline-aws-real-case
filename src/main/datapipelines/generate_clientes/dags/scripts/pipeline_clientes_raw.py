from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='pipeline_clientes',
    default_args=default_args,
    description='Pipeline de clientes da camada raw para processed',
    schedule_interval='@daily',  # ou None para rodar manualmente
    catchup=False,
    tags=['etl', 'clientes'],
) as dag:

    executa_loader_clientes = BashOperator(
        task_id='executa_loader_clientes',
        bash_command='spark-submit /opt/airflow/scripts/loader.py'
    )

    executa_loader_clientes
