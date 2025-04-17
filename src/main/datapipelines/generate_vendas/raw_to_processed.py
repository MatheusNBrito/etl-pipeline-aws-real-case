from datapipelines.generate_vendas.commons.spark_session import SparkSessionWrapper
from datapipelines.generate_vendas.commons.etl_steps import load_raw_data, apply_transformations, save_processed_data

# Inicia Spark
spark_wrapper = SparkSessionWrapper(app_name="GenerateVendasSparkSession")
spark = spark_wrapper.get_session()

# Executa pipeline
df_raws = load_raw_data(spark)
df_processed = apply_transformations(df_raws)
save_processed_data(df_processed)

spark_wrapper.stop()
