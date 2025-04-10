from pathlib import Path
from datapipelines.generate_clientes.commons.functions import DataLoader
from datapipelines.generate_clientes.config_loader import config
from datapipelines.generate_clientes.commons.functions import save_parquet
from datapipelines.generate_clientes.transformations.transform_clientes_opt import transform_clientes_opt
from datapipelines.generate_clientes.transformations.transform_enderecos_clientes import transform_enderecos_clientes
from datapipelines.generate_clientes.transformations.transform_clientes import transform_clientes



# Diretório base do projeto (raiz do container /app)
BASE_DIR = Path("/app")

output_path_clientes_opt = str(BASE_DIR / config["output_paths"]["CLIENTES_OPT_PATH"])
output_path_enderecos_clientes = str(BASE_DIR / config["output_paths"]["ENDERECOS_CLIENTES_PATH"])
output_path_clientes = str(BASE_DIR / config["output_paths"]["CLIENTES_PATH"])

# Recupera os caminhos do arquivo de configuração
raw_tables = config["input_paths"]["raw_tables"]

# Ajusta os caminhos dos arquivos para o caminho absoluto
raw_tables["CLIENTES_PATH"] = str(BASE_DIR / raw_tables["CLIENTES_PATH"])
raw_tables["CLIENTES_OPT_PATH"] = str(BASE_DIR / raw_tables["CLIENTES_OPT_PATH"])
raw_tables["ENDERECOS_CLIENTES_PATH"] = str(BASE_DIR / raw_tables["ENDERECOS_CLIENTES_PATH"])

# Inicializa o DataLoader
data_loader = DataLoader()

# Carrega os DataFrames
clientes_raw_df, clientes_opt_raw_df, enderecos_clientes_raw_df = data_loader.load_data(raw_tables)

df_transformed_clientes_opt = transform_clientes_opt(clientes_opt_raw_df)
df_transformed_enderecos_clientes = transform_enderecos_clientes(enderecos_clientes_raw_df)
df_transformed_clientes = transform_clientes(clientes_raw_df)

# Exibe os dados lidos (top 5 registros de cada dataframe)
print("======= CLIENTES =======")
clientes_raw_df.show(5)

print("======= CLIENTES OPT =======")
clientes_opt_raw_df.show(5)

print("======= ENDERECOS CLIENTES =======")
enderecos_clientes_raw_df.show(5)

# Salva o arquivo parquet tratado
save_parquet(df_transformed_clientes_opt, output_path_clientes_opt)
save_parquet(df_transformed_enderecos_clientes, output_path_enderecos_clientes)
save_parquet(df_transformed_clientes, output_path_clientes)


# Finaliza a sessão Spark
data_loader.stop()
