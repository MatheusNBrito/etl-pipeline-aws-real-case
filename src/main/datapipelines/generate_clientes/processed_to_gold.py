from datapipelines.generate_clientes.commons.spark_session import SparkSessionWrapper
from datapipelines.generate_clientes.commons.etl_steps_gold import load_processed_data, join_and_aggregate, save_gold_data

# Inicializa sessão Spark
spark_wrapper = SparkSessionWrapper(app_name="GenerateClientesSparkSession")
spark = spark_wrapper.get_session()

# Pipeline da camada processed para gold
df_clientes, df_clientes_opt, df_enderecos = load_processed_data(spark)
df_gold = join_and_aggregate(df_clientes, df_clientes_opt, df_enderecos)
save_gold_data(df_gold)

spark_wrapper.stop()
