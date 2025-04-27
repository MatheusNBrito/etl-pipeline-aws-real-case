from datapipelines.generate_clientes.commons.spark_session import SparkSessionWrapper
from datapipelines.generate_clientes.commons.etl_steps_gold import (
    load_processed_data,
    join_and_aggregate,
    save_gold_data
)
from datapipelines.logger_config import get_logger

# Inicializa o logger
logger = get_logger("processed_to_gold_clientes")

def main():
    """
    Executa o pipeline de transformação de dados da camada processed para a camada gold.
    """
    logger.info("Iniciando sessão Spark para geração da camada gold.")
    spark_wrapper = SparkSessionWrapper(app_name="GenerateClientesSparkSession")
    spark = spark_wrapper.get_session()

    try:
        logger.info("Carregando dados da camada processed...")
        df_clientes, df_opt, df_enderecos = load_processed_data(spark)

        logger.info("Realizando joins e seleção das colunas para camada gold...")
        df_gold = join_and_aggregate(df_clientes, df_opt, df_enderecos)

        logger.info("Salvando dados da camada gold...")
        save_gold_data(df_gold)

        logger.info("Pipeline processed_to_gold finalizada com sucesso.")
    except Exception as e:
        logger.exception("Erro durante a execução da pipeline processed_to_gold:")
        raise
    finally:
        spark_wrapper.stop()
        logger.info("Sessão Spark finalizada.")

if __name__ == "__main__":
    main()
