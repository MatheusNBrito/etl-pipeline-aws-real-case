import boto3
from datapipelines.generate_clientes.commons.functions import DataLoader, save_parquet
from datapipelines.generate_clientes.config_loader import config
from datapipelines.generate_clientes.transformations.transform_clientes_opt import transform_clientes_opt
from datapipelines.generate_clientes.transformations.transform_enderecos_clientes import transform_enderecos_clientes
from datapipelines.generate_clientes.transformations.transform_clientes import transform_clientes
from datapipelines.generate_clientes.commons.spark_session import SparkSessionWrapper

def load_raw_data(spark):
    raw_paths = config["input_paths"]["raw_tables"]
    loader = DataLoader(spark)
    df_clientes_raw, df_clientes_opt_raw, df_enderecos_clientes_raw = loader.load_raw_data(raw_paths)
    return df_clientes_raw, df_clientes_opt_raw, df_enderecos_clientes_raw

def apply_transformations(df_tuple):
    df_clientes, df_clientes_opt, df_enderecos = df_tuple
    df_transformed_clientes = transform_clientes(df_clientes)
    df_transformed_clientes_opt = transform_clientes_opt(df_clientes_opt)
    df_transformed_enderecos = transform_enderecos_clientes(df_enderecos)
    return df_transformed_clientes, df_transformed_clientes_opt, df_transformed_enderecos

def save_processed_data(df_tuple):
    df_clientes, df_clientes_opt, df_enderecos = df_tuple
    paths = config["output_paths"]["processed_tables"]
    save_parquet(df_clientes, paths["CLIENTES_PATH"])
    save_parquet(df_clientes_opt, paths["CLIENTES_OPT_PATH"])
    save_parquet(df_enderecos, paths["ENDERECOS_CLIENTES_PATH"])
    print("Arquivos processados enviados para o S3 com sucesso!")

# 🔥 Execução principal
if __name__ == "__main__":
    spark_wrapper = SparkSessionWrapper(app_name="ETLClientesProcessed")
    spark = spark_wrapper.get_session()

    raw_dfs = load_raw_data(spark)
    transformed_dfs = apply_transformations(raw_dfs)
    save_processed_data(transformed_dfs)

    spark_wrapper.stop()
