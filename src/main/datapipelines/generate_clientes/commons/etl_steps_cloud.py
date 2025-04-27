import boto3
from datapipelines.generate_clientes.commons.functions import DataLoader, save_parquet
from datapipelines.generate_clientes.config_loader import config
from datapipelines.generate_clientes.transformations.transform_clientes_opt import transform_clientes_opt
from datapipelines.generate_clientes.transformations.transform_enderecos_clientes import transform_enderecos_clientes
from datapipelines.generate_clientes.transformations.transform_clientes import transform_clientes
from datapipelines.generate_clientes.commons.spark_session import SparkSessionWrapper

def load_raw_data(spark):
    """
    Carrega os dados brutos a partir dos caminhos especificados no arquivo de configuração.
    """
    raw_paths = config["input_paths"]["raw_tables"]
    loader = DataLoader(spark)
    df_clientes_raw, df_clientes_opt_raw, df_enderecos_clientes_raw = loader.load_raw_data(raw_paths)
    return df_clientes_raw, df_clientes_opt_raw, df_enderecos_clientes_raw

def apply_transformations(df_tuple):
    """
    Aplica as transformações necessárias aos DataFrames brutos.
    """
    df_clientes, df_clientes_opt, df_enderecos = df_tuple
    df_transformed_clientes = transform_clientes(df_clientes)
    df_transformed_clientes_opt = transform_clientes_opt(df_clientes_opt)
    df_transformed_enderecos = transform_enderecos_clientes(df_enderecos)
    return df_transformed_clientes, df_transformed_clientes_opt, df_transformed_enderecos

def save_processed_data(df_tuple):
    """
    Salva os DataFrames transformados no S3 em formato Parquet.
    """
    df_clientes, df_clientes_opt, df_enderecos = df_tuple
    paths = config["output_paths"]["processed_tables"]
    save_parquet(df_clientes, paths["CLIENTES_PATH"])
    save_parquet(df_clientes_opt, paths["CLIENTES_OPT_PATH"])
    save_parquet(df_enderecos, paths["ENDERECOS_CLIENTES_PATH"])
    print("Arquivos processados enviados para o S3 com sucesso.")

# Execução principal
if __name__ == "__main__":
    # Inicializa sessão Spark
    spark_wrapper = SparkSessionWrapper(app_name="ETLClientesProcessed")
    spark = spark_wrapper.get_session()

    # Carrega dados brutos
    raw_dfs = load_raw_data(spark)

    # Aplica transformações
    transformed_dfs = apply_transformations(raw_dfs)

    # Salva dados processados no S3
    save_processed_data(transformed_dfs)

    # Finaliza sessão Spark
    spark_wrapper.stop()
