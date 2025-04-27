from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configurações padrão da DAG
default_args = {
    "owner": "MatheusNbrito",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Definição da DAG
with DAG(
    dag_id="clientes_pipeline",
    default_args=default_args,
    description="Pipeline de ETL para clientes: raw > processed > gold",
    schedule=None,
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=["etl", "clientes"],
) as dag:

    # Tarefa: Converter dados de raw para processed
    raw_to_processed = BashOperator(
        task_id="raw_to_processed",
        bash_command="cd /app/src/main && PYTHONPATH=/app/src/main python -m datapipelines.generate_clientes.commons.etl_steps_cloud",
    )

    # Tarefa: Converter dados de processed para gold
    processed_to_gold = BashOperator(
        task_id="processed_to_gold",
        bash_command="cd /app/src/main && PYTHONPATH=/app/src/main python -m datapipelines.generate_clientes.commons.etl_steps_gold_cloud",
    )

    # Definição da ordem de execução das tarefas
    raw_to_processed >> processed_to_gold

