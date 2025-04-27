from datapipelines.generate_vendas.commons.spark_session import SparkSessionWrapper
from datapipelines.generate_vendas.commons.etl_steps_gold import (
    load_processed_data,
    aggregate_and_join,
    save_gold_data
)
from datapipelines.logger_config import get_logger

# Inicializa o logger
logger = get_logger("generate_vendas")

def main():
    """
    Executa o pipeline de geração da camada gold para vendas.
    """
    logger.info("Iniciando sessão Spark para geração da camada gold.")
    spark_wrapper = SparkSessionWrapper(app_name="GenerateVendasSparkSession")
    spark = spark_wrapper.get_session()

    try:
        logger.info("Carregando dados da camada processed...")
        df_vendas, df_pedidos, df_itens, df_pedido_venda = load_processed_data(spark)

        logger.info("Executando joins e agregações para geração da camada gold...")
        df_gold = aggregate_and_join(df_vendas, df_pedidos, df_itens, df_pedido_venda)

        logger.info("Salvando a camada gold de vendas...")
        save_gold_data(df_gold)

        logger.info("Pipeline finalizada com sucesso.")
    except Exception as e:
        logger.exception("Erro ao gerar o DataFrame gold de vendas:")
        raise
    finally:
        spark_wrapper.stop()
        logger.info("Sessão Spark finalizada.")

if __name__ == "__main__":
    main()
