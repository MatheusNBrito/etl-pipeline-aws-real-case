from datapipelines.generate_vendas.commons.spark_session import SparkSessionWrapper
from datapipelines.generate_vendas.commons.etl_steps_gold import (
    load_processed_data,
    aggregate_and_join,
    save_gold_data
)
from datapipelines.logger_config import get_logger

logger = get_logger("generate_vendas")

def main():
    logger.info("▶ Iniciando sessão Spark para geração da camada gold.")
    spark_wrapper = SparkSessionWrapper(app_name="GenerateVendasSparkSession")
    spark = spark_wrapper.get_session()

    try:
        logger.info("📥 Carregando dados da camada processed...")
        df_vendas, df_pedidos, df_itens, df_pedido_venda = load_processed_data(spark)

        logger.info("🔄 Iniciando joins e agregações para gerar camada gold...")
        df_gold = aggregate_and_join(df_vendas, df_pedidos, df_itens, df_pedido_venda)

        logger.info("💾 Salvando camada gold de vendas...")
        save_gold_data(df_gold)

        logger.info("✅ Pipeline finalizada com sucesso.")
    except Exception as e:
        logger.exception("❌ Erro ao gerar df_gold_vendas:")
        raise
    finally:
        spark_wrapper.stop()
        logger.info("🛑 Sessão Spark finalizada.")


if __name__ == "__main__":
    main()
