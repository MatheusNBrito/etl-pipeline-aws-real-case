from pathlib import Path
from datapipelines.generate_vendas.commons.functions import DataLoader
from datapipelines.generate_vendas.config_loader import config
from datapipelines.generate_vendas.commons.functions import save_parquet
from datapipelines.generate_vendas.transformations.transform_vendas import transform_vendas
from datapipelines.generate_vendas.transformations.transform_pedidos import transform_pedidos
from datapipelines.generate_vendas.transformations.transform_itens_vendas import transform_itens_vendas
from datapipelines.generate_vendas.transformations.transform_pedido_venda import transform_pedido_venda
from datapipelines.generate_vendas.commons.spark_session import SparkSessionWrapper

# Inicializa a sessão Spark
spark_wrapper = SparkSessionWrapper(app_name="GenerateVendasSparkSession")
spark = spark_wrapper.get_session()

# Diretório base do projeto (raiz do container /app)
BASE_DIR = Path("/app")


# Recupera os caminhos do arquivo de configuração para a camada raw
raw_tables = config["input_paths"]["raw_tables"]

# Ajusta os caminhos dos arquivos para o caminho absoluto
raw_tables["VENDAS_PATH"] = str(BASE_DIR / raw_tables["VENDAS_PATH"])
raw_tables["PEDIDOS_PATH"] = str(BASE_DIR / raw_tables["PEDIDOS_PATH"])
raw_tables["ITENS_VENDA_PATH"] = str(BASE_DIR / raw_tables["ITENS_VENDA_PATH"])
raw_tables["PEDIDO_VENDA_PATH"] = str(BASE_DIR / raw_tables["PEDIDO_VENDA_PATH"])

# Caminhos de saída (para a camada processed)
output_path_vendas = str(BASE_DIR / config["output_paths"]["processed_tables"]["VENDAS_PATH"])
output_path_pedidos = str(BASE_DIR / config["output_paths"]["processed_tables"]["PEDIDOS_PATH"])
output_path_itens_venda = str(BASE_DIR / config["output_paths"]["processed_tables"]["ITENS_VENDA_PATH"])
output_path_pedido_venda = str(BASE_DIR / config["output_paths"]["processed_tables"]["PEDIDO_VENDA_PATH"])

# Inicializa o DataLoader
data_loader = DataLoader(spark)  

# Carrega os DataFrames da camada raw (dados brutos)
df_vendas_raw, df_pedidos_raw, df_itens_vendas_raw, df_pedido_venda_raw = data_loader.load_raw_data(raw_tables)

# Realiza as transformações
df_transformed_vendas = transform_vendas(df_vendas_raw)
df_transformed_pedidos = transform_pedidos(df_pedidos_raw)
df_transformed_itens_vendas = transform_itens_vendas(df_itens_vendas_raw)
df_transformed_pedido_venda = transform_pedido_venda(df_pedido_venda_raw)


# Salva o arquivo parquet tratado na camada processed
save_parquet(df_transformed_vendas, output_path_vendas)
save_parquet(df_transformed_pedidos, output_path_pedidos)
save_parquet(df_transformed_itens_vendas, output_path_itens_venda)
save_parquet(df_transformed_pedido_venda, output_path_pedido_venda)

# Finaliza a sessão Spark
data_loader.stop()
