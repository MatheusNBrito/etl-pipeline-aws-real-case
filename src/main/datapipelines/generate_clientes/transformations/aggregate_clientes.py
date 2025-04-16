from os import replace
from pyspark.sql import DataFrame
from pyspark.sql.functions import sum
from pathlib import Path
from datapipelines.generate_clientes.commons.constants import *
from datapipelines.generate_clientes.commons.functions import DataLoader, save_parquet, replace_nulls
from datapipelines.generate_clientes.config_loader import config
from datapipelines.generate_clientes.commons.spark_session import SparkSessionWrapper

# Inicializa a sessão Spark
spark_wrapper = SparkSessionWrapper(app_name="GenerateClientesSparkSession")
spark = spark_wrapper.get_session()


def aggregate_and_join(df_clientes: DataFrame, df_clientes_opt: DataFrame, df_enderecos: DataFrame) -> DataFrame:
    """
    Junta os dados dos DataFrames de clientes, clientes_opt e endereços.
    Realiza as agregações necessárias e prepara para a camada gold.
    """

    # Juntar os DataFrames (clientes, clientes_opt e endereços)
    df_joined = df_clientes \
        .join(df_clientes_opt, on=CODIGO_CLIENTE, how="left") \
        .join(df_enderecos, on=CODIGO_CLIENTE, how="left")

    # Selecionar as colunas necessárias para a camada gold
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


# Diretório base do projeto (raiz do container /app)
BASE_DIR = Path("/app")

# Recupera os caminhos do arquivo de configuração
processed_tables = config["input_paths"]["processed_tables"]

# Ajusta os caminhos dos arquivos para o caminho absoluto
processed_tables["CLIENTES_PATH"] = str(BASE_DIR / processed_tables["CLIENTES_PATH"])
processed_tables["CLIENTES_OPT_PATH"] = str(BASE_DIR / processed_tables["CLIENTES_OPT_PATH"])
processed_tables["ENDERECOS_CLIENTES_PATH"] = str(BASE_DIR / processed_tables["ENDERECOS_CLIENTES_PATH"])

# Inicializa o DataLoader
data_loader = DataLoader(spark)

# Carrega os DataFrames da camada prata (processed)
df_processed_clientes, df_processed_clientes_opt, df_processed_enderecos_clientes = data_loader.load_processed_data(processed_tables)

# Realiza a agregação e transformação
df_gold_clientes = aggregate_and_join(df_processed_clientes, df_processed_clientes_opt, df_processed_enderecos_clientes)

# Aplica a substituição de nulos antes de salvar
df_gold_clientes_transformed = replace_nulls(df_gold_clientes)

# Caminho de saída para a camada gold
output_path_clientes_gold = str(BASE_DIR / config["output_paths"]["gold_tables"]["CLIENTES_PATH"])


# Salva o arquivo parquet na camada gold
save_parquet(df_gold_clientes_transformed, output_path_clientes_gold)

# Finaliza a sessão Spark
data_loader.stop()
