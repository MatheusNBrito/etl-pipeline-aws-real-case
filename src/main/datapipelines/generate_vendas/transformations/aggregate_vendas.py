from os import replace
from pyspark.sql import DataFrame
from pyspark.sql.functions import sum
from pathlib import Path
from datapipelines.generate_vendas.commons.constants import *
from datapipelines.generate_vendas.commons.functions import DataLoader, save_parquet, replace_nulls
from datapipelines.generate_vendas.config_loader import config
from datapipelines.generate_vendas.commons.spark_session import SparkSessionWrapper

# Inicializa a sessão Spark
spark_wrapper = SparkSessionWrapper(app_name="GenerateVendasSparkSession")
spark = spark_wrapper.get_session()


def aggregate_and_join(df_vendas: DataFrame, df_pedidos: DataFrame, df_itens_vendas: DataFrame, df_pedido_venda: DataFrame) -> DataFrame:
    """
    Junta os dados dos DataFrames de vendas, pedidos e itens_vendas.
    Realiza as agregações necessárias e prepara para a camada gold.
    """
    # Join para pegar o canal_venda corretamente
    df_vendas_com_canal = join_vendas_com_canal(
        df_vendas=df_vendas,
        df_pedido_venda=df_pedido_venda,
        df_pedidos=df_pedidos
    )

       # Join com itens_vendas (lembrando que CODIGO_CUPOM_VENDA = n_id_vd_fil)
    df_joined = df_vendas_com_canal.join(
        df_itens_vendas,
        on=CODIGO_CUPOM_VENDA,
        how="left"
    )

    # Selecionar as colunas necessárias para a camada gold
    df_vendas_gold = df_joined.select(
        CODIGO_FILIAL,
        CODIGO_CUPOM_VENDA,
        DATA_EMISSAO,
        CODIGO_ITEM,
        VALOR_UNITARIO,
        QUANTIDADE,
        CODIGO_CLIENTE,
        TIPO_DESCONTO,
        CANAL_VENDA,
    )

    return df_vendas_gold

def join_vendas_com_canal(
    df_vendas: DataFrame,
    df_pedido_venda: DataFrame,
    df_pedidos: DataFrame
) -> DataFrame:
    """
    Realiza o join entre as tabelas vendas, pedido_venda e pedidos
    para adicionar a coluna 'canal_venda' na tabela final de vendas.
    """

    # Join vendas com pedido_venda para obter o n_id_pdd
    df_vendas_pedido = df_vendas.join(
        df_pedido_venda,
        on=["CODIGO_FILIAL", "CODIGO_CUPOM_VENDA"],
        how="left"
    )

    # Join com pedidos (que já deve ter sido transformado com canal_venda)
    df_vendas_com_canal = df_vendas_pedido.join(
        df_pedidos,
        on="CODIGO_PEDIDO",
        how="left"
    )

    return df_vendas_com_canal

# Diretório base do projeto (raiz do container /app)
BASE_DIR = Path("/app")

# Recupera os caminhos do arquivo de configuração
processed_tables = config["input_paths"]["processed_tables"]

# Ajusta os caminhos dos arquivos para o caminho absoluto
processed_tables["VENDAS_PATH"] = str(BASE_DIR / processed_tables["VENDAS_PATH"])
processed_tables["PEDIDOS_PATH"] = str(BASE_DIR / processed_tables["PEDIDOS_PATH"])
processed_tables["ITENS_VENDA_PATH"] = str(BASE_DIR / processed_tables["ITENS_VENDA_PATH"])
processed_tables["PEDIDO_VENDA_PATH"] = str(BASE_DIR / processed_tables["PEDIDO_VENDA_PATH"])

# Inicializa o DataLoader
data_loader = DataLoader(spark)

# Carrega os DataFrames da camada prata (processed)
df_processed_vendas, df_processed_pedidos, df_processed_itens_vendas, df_processed_pedido_venda = data_loader.load_processed_data(processed_tables)

# Realiza a agregação e transformação
df_gold_vendas = aggregate_and_join(df_processed_vendas, df_processed_pedidos, df_processed_itens_vendas, df_processed_pedido_venda)

# Aplica a substituição de nulos antes de salvar
# df_gold_clientes_transformed = replace_nulls(df_gold_clientes)

# Caminho de saída para a camada gold
output_path_vendas_gold = str(BASE_DIR / config["output_paths"]["gold_tables"]["VENDAS_PATH"])


# Salva o arquivo parquet na camada gold
save_parquet(df_gold_vendas, output_path_vendas_gold)

# Finaliza a sessão Spark
data_loader.stop()
