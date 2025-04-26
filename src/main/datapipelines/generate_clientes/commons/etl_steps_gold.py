from pathlib import Path
from pyspark.sql import DataFrame
from datapipelines.generate_clientes.commons.functions import DataLoader, save_parquet, replace_nulls
from datapipelines.generate_clientes.commons.constants import *
from datapipelines.generate_clientes.config_loader import config
from datapipelines.generate_clientes.commons.spark_session import SparkSessionWrapper  

BASE_DIR = Path("/app")

def load_processed_data(spark):
    """Carrega os dados da camada processed"""
    paths = config["input_paths"]["processed_tables"]
    processed_paths = {
        "CLIENTES_PATH": str(BASE_DIR / paths["CLIENTES_PATH"]),
        "CLIENTES_OPT_PATH": str(BASE_DIR / paths["CLIENTES_OPT_PATH"]),
        "ENDERECOS_CLIENTES_PATH": str(BASE_DIR / paths["ENDERECOS_CLIENTES_PATH"]),
    }

    loader = DataLoader(spark)
    return loader.load_processed_data(processed_paths)

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

    # Mostrar informações para debug
    print(f"Número de registros no DataFrame Gold: {df_clientes_gold.count()}")
    df_clientes_gold.show(5)

    return df_clientes_gold

def save_gold_data(df: DataFrame):
    """Salva o DataFrame gold na pasta final"""
    output_path = config["output_paths"]["gold_tables"]["CLIENTES_PATH"]
    full_path = str(BASE_DIR / output_path)
    df = replace_nulls(df)
    save_parquet(df, full_path)

if __name__ == "__main__":
    # ✅ Execução principal (como módulo)
    spark_wrapper = SparkSessionWrapper(app_name="ETLClientesGOLD")
    spark = spark_wrapper.get_session()

    df_clientes, df_clientes_opt, df_enderecos = load_processed_data(spark)
    df_gold = join_and_aggregate(df_clientes, df_clientes_opt, df_enderecos)
    save_gold_data(df_gold)

    spark_wrapper.stop()
