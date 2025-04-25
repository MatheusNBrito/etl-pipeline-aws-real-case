from pyspark.sql import DataFrame
from datapipelines.generate_clientes.commons.functions import DataLoader, save_parquet, replace_nulls
from datapipelines.generate_clientes.commons.constants import *
from datapipelines.generate_clientes.config_loader import config
import boto3


def load_processed_data(spark):
    """Carrega os dados da camada processed (direto do S3)"""
    paths = config["input_paths"]["processed_tables"]

    loader = DataLoader(spark)
    return loader.load_processed_data(paths)


def join_and_aggregate(df_clientes: DataFrame, df_clientes_opt: DataFrame, df_enderecos: DataFrame) -> DataFrame:
    """Faz os joins e gera o DataFrame da camada gold"""
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
    """Salva o DataFrame gold diretamente no S3"""
    output_path = config["output_paths"]["gold_tables"]["CLIENTES_PATH"]
    df = replace_nulls(df)
    save_parquet(df, output_path)

    # Cria o cliente boto3 para o S3
    s3 = boto3.client('s3')
    bucket_name = 'etl-pipeline-aws-dev-bucket'  # Seu bucket S3

    # Faz o upload para o S3
    s3.upload_file(output_path, bucket_name, 'gold/clientes_gold.parquet')

    print("Arquivo Gold enviado para o S3 com sucesso!")
