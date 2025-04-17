from datapipelines.generate_vendas.commons.spark_session import SparkSessionWrapper
from datapipelines.generate_vendas.commons.etl_steps_gold import (
    load_processed_data,
    aggregate_and_join,
    save_gold_data
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicia Spark
spark_wrapper = SparkSessionWrapper(app_name="GenerateVendasSparkSession")
spark = spark_wrapper.get_session()

# Executa pipeline
try:
    df_vendas, df_pedidos, df_itens, df_pedido_venda = load_processed_data(spark)
    df_gold = aggregate_and_join(df_vendas, df_pedidos, df_itens, df_pedido_venda)
    save_gold_data(df_gold)
except Exception as e:
    logger.exception("❌ Erro ao gerar df_gold_vendas:")
    raise

spark_wrapper.stop()
