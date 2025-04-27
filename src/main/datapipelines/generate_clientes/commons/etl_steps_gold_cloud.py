from pyspark.sql import DataFrame
from datapipelines.generate_clientes.commons.functions import DataLoader, save_parquet, replace_nulls
from datapipelines.generate_clientes.commons.constants import *
from datapipelines.generate_clientes.config_loader import config
from datapipelines.generate_clientes.commons.spark_session import SparkSessionWrapper
import boto3

def load_processed_data(spark):
    paths = config["input_paths"]["processed_tables"]
    loader = DataLoader(spark)
    return loader.load_processed_data(paths)

def join_and_aggregate(df_clientes: DataFrame, df_clientes_opt: DataFrame, df_enderecos: DataFrame) -> DataFrame:
    df_joined = df_clientes \
        .join(df_clientes_opt, on=CODIGO_CLIENTE, how="left") \
        .join(df_enderecos, on=CODIGO_CLIENTE, how="left")

    df_clientes_gold = df_joined.select(
        CODIGO_CLIENTE,
        DATA_NASCIMENTO,
        IDADE,
        SEXO,
        UF,
        CIDADE,
        ESTADO_CIVIL,
        FLAG_LGPD_CALL,
        FLAG_LGPD_SMS,
        FLAG_LGPD_EMAIL,
        FLAG_LGPD_PUSH
    )
    return df_clientes_gold

def save_gold_data(df: DataFrame):
    output_path = config["output_paths"]["gold_tables"]["CLIENTES_PATH"]
    df = replace_nulls(df)
    save_parquet(df, output_path)
    print("Arquivo Gold enviado para o S3 com sucesso!")

# 🔥 Execução principal
if __name__ == "__main__":
    spark_wrapper = SparkSessionWrapper(app_name="ETLClientesGOLD")
    spark = spark_wrapper.get_session()

    df_clientes, df_clientes_opt, df_enderecos = load_processed_data(spark)
    df_gold = join_and_aggregate(df_clientes, df_clientes_opt, df_enderecos)
    save_gold_data(df_gold)

    spark_wrapper.stop()
