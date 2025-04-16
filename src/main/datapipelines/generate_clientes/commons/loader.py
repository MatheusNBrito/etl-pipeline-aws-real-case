from pathlib import Path
from datapipelines.generate_clientes.commons.functions import DataLoader
from datapipelines.generate_clientes.config_loader import config
from datapipelines.generate_clientes.commons.functions import save_parquet
from datapipelines.generate_clientes.transformations.transform_clientes_opt import transform_clientes_opt
from datapipelines.generate_clientes.transformations.transform_enderecos_clientes import transform_enderecos_clientes
from datapipelines.generate_clientes.transformations.transform_clientes import transform_clientes
from datapipelines.generate_clientes.commons.spark_session import SparkSessionWrapper

# Inicializa a sessão Spark
spark_wrapper = SparkSessionWrapper(app_name="GenerateClientesSparkSession")
spark = spark_wrapper.get_session()

# Diretório base do projeto (raiz do container /app)
BASE_DIR = Path("/app")


# Recupera os caminhos do arquivo de configuração para a camada raw
raw_tables = config["input_paths"]["raw_tables"]

# Ajusta os caminhos dos arquivos para o caminho absoluto
raw_tables["CLIENTES_PATH"] = str(BASE_DIR / raw_tables["CLIENTES_PATH"])
raw_tables["CLIENTES_OPT_PATH"] = str(BASE_DIR / raw_tables["CLIENTES_OPT_PATH"])
raw_tables["ENDERECOS_CLIENTES_PATH"] = str(BASE_DIR / raw_tables["ENDERECOS_CLIENTES_PATH"])

# Caminhos de saída (para a camada processed)
output_path_clientes_opt = str(BASE_DIR / config["output_paths"]["processed_tables"]["CLIENTES_OPT_PATH"])
output_path_enderecos_clientes = str(BASE_DIR / config["output_paths"]["processed_tables"]["ENDERECOS_CLIENTES_PATH"])
output_path_clientes = str(BASE_DIR / config["output_paths"]["processed_tables"]["CLIENTES_PATH"])


# Inicializa o DataLoader
data_loader = DataLoader(spark)  

# Carrega os DataFrames da camada raw (dados brutos)
df_clientes_raw, df_clientes_opt_raw, df_enderecos_clientes_raw = data_loader.load_raw_data(raw_tables)

# Realiza as transformações
df_transformed_clientes_opt = transform_clientes_opt(df_clientes_opt_raw)
df_transformed_enderecos_clientes = transform_enderecos_clientes(df_enderecos_clientes_raw)
df_transformed_clientes = transform_clientes(df_clientes_raw)

# Salva o arquivo parquet tratado na camada processed
save_parquet(df_transformed_clientes_opt, output_path_clientes_opt)
save_parquet(df_transformed_enderecos_clientes, output_path_enderecos_clientes)
save_parquet(df_transformed_clientes, output_path_clientes)

# Finaliza a sessão Spark
data_loader.stop()
