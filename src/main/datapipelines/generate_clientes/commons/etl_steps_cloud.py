import boto3
from datapipelines.generate_clientes.commons.functions import DataLoader, save_parquet
from datapipelines.generate_clientes.config_loader import config
from datapipelines.generate_clientes.transformations.transform_clientes_opt import transform_clientes_opt
from datapipelines.generate_clientes.transformations.transform_enderecos_clientes import transform_enderecos_clientes
from datapipelines.generate_clientes.transformations.transform_clientes import transform_clientes


def load_raw_data(spark):
    """Carrega os dados da camada raw com base no application.conf (agora usando S3 direto)"""
    raw_paths = config["input_paths"]["raw_tables"]

    loader = DataLoader(spark)
    df_clientes_raw, df_clientes_opt_raw, df_enderecos_clientes_raw = loader.load_raw_data(raw_paths)

    return df_clientes_raw, df_clientes_opt_raw, df_enderecos_clientes_raw


def apply_transformations(df_tuple):
    """Aplica as transformações em cada DataFrame raw"""
    df_clientes, df_clientes_opt, df_enderecos = df_tuple

    df_transformed_clientes = transform_clientes(df_clientes)
    df_transformed_clientes_opt = transform_clientes_opt(df_clientes_opt)
    df_transformed_enderecos = transform_enderecos_clientes(df_enderecos)

    return df_transformed_clientes, df_transformed_clientes_opt, df_transformed_enderecos


def save_processed_data(df_tuple):
    """Salva os dados transformados na camada processed (direto no S3)"""
    df_clientes, df_clientes_opt, df_enderecos = df_tuple
    paths = config["output_paths"]["processed_tables"]

    # Salva localmente em S3 (vai ser criado no diretório temporário primeiro)
    save_parquet(df_clientes, paths["CLIENTES_PATH"])
    save_parquet(df_clientes_opt, paths["CLIENTES_OPT_PATH"])
    save_parquet(df_enderecos, paths["ENDERECOS_CLIENTES_PATH"])

    # Aqui faz o upload para o S3 após salvar o arquivo
    # s3 = boto3.client('s3')
    # bucket_name = 'etl-pipeline-aws-dev-bucket'  # Seu bucket S3

    # # Upload dos arquivos processados para o S3
    # s3.upload_file(paths["CLIENTES_PATH"], bucket_name, 'processed/clientes.parquet')
    # s3.upload_file(paths["CLIENTES_OPT_PATH"], bucket_name, 'processed/clientes_opt.parquet')
    # s3.upload_file(paths["ENDERECOS_CLIENTES_PATH"], bucket_name, 'processed/enderecos_clientes.parquet')

    print("Arquivos processados enviados para o S3 com sucesso!")
