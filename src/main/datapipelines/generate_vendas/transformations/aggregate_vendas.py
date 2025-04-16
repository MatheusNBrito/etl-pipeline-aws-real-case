from pyspark.sql import DataFrame
from pyspark.sql.functions import sum
from pathlib import Path
import logging
from pyspark import StorageLevel
from datapipelines.generate_vendas.commons.constants import *
from datapipelines.generate_vendas.commons.functions import DataLoader, save_parquet, replace_nulls
from datapipelines.generate_vendas.config_loader import config
from datapipelines.generate_vendas.commons.spark_session import SparkSessionWrapper
import gc

# Configura log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializa a sessão Spark
spark_wrapper = SparkSessionWrapper(app_name="GenerateVendasSparkSession")
spark = spark_wrapper.get_session()


def aggregate_and_join(df_vendas: DataFrame, df_pedidos: DataFrame, df_itens_vendas: DataFrame, df_pedido_venda: DataFrame) -> DataFrame:
    """
    Junta os dados dos DataFrames de vendas, pedidos e itens_vendas.
    Realiza as agregações necessárias e prepara para a camada gold.
    """

    logger.info("Iniciando join de vendas com pedido_venda e pedidos...")

    df_vendas_com_canal = join_vendas_com_canal(
        df_vendas=df_vendas,
        df_pedido_venda=df_pedido_venda,
        df_pedidos=df_pedidos
    )

    logger.info("Join de vendas com canal_venda finalizado. Quantidade de linhas: %s")

    # Otimização para grande volume de dados
    logger.info("⏳ Schema de df_vendas_com_canal:")
    df_vendas_com_canal.printSchema()
    logger.info("⏳ Schema de df_itens_vendas:")
    df_itens_vendas.printSchema()
    common_columns = set(df_vendas_com_canal.columns) & set(df_itens_vendas.columns)
    logger.info("🔁 Colunas em comum entre os DataFrames: %s", common_columns)

    df_vendas_com_canal = df_vendas_com_canal.repartition(50, CODIGO_CUPOM_VENDA).cache()
    df_itens_vendas = df_itens_vendas.repartition(50, CODIGO_CUPOM_VENDA).persist(StorageLevel.DISK_ONLY)


    logger.info("Join de vendas com itens_vendas em andamento...")

    df_joined = df_vendas_com_canal.join(
        df_itens_vendas,
        on=CODIGO_CUPOM_VENDA,
        how="left"
    )

    # logger.info("Join finalizado. Quantidade de linhas totais: %s")

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

    logger.info("Dataframe gold final gerado com sucesso!")

    return df_vendas_gold


def join_vendas_com_canal(df_vendas: DataFrame, df_pedido_venda: DataFrame, df_pedidos: DataFrame) -> DataFrame:
    """
    Realiza o join entre as tabelas vendas, pedido_venda e pedidos
    para adicionar a coluna 'canal_venda' na tabela final de vendas.
    """

    df_vendas_pedido = df_vendas.join(
        df_pedido_venda,
        on=[CODIGO_FILIAL, CODIGO_CUPOM_VENDA],
        how="left"
    )

    df_vendas_com_canal = df_vendas_pedido.join(
        df_pedidos,
        on=CODIGO_PEDIDO,
        how="left"
    )

    return df_vendas_com_canal


# Diretório base do projeto (raiz do container /app)
BASE_DIR = Path("/app")

# Recupera os caminhos do arquivo de configuração
processed_tables = config["input_paths"]["processed_tables"]

# Ajusta os caminhos dos arquivos para o caminho absoluto
for key, path in processed_tables.items():
    processed_tables[key] = str(BASE_DIR / path)

# Inicializa o DataLoader
data_loader = DataLoader(spark)

# Carrega os DataFrames da camada prata (processed)
df_processed_vendas, df_processed_pedidos, df_processed_itens_vendas, df_processed_pedido_venda = data_loader.load_processed_data(processed_tables)

# Realiza a agregação e transformação
try:
    df_gold_vendas = aggregate_and_join(
        df_processed_vendas,
        df_processed_pedidos,
        df_processed_itens_vendas,
        df_processed_pedido_venda
    )
except Exception as e:
    logger.exception("❌ Erro ao gerar df_gold_vendas:")
    raise

# Caminho de saída para a camada gold
output_path_vendas_gold = str(BASE_DIR / config["output_paths"]["gold_tables"]["VENDAS_PATH"])

# Reparticiona e limpa memória antes do save devido ao tamanho do dataset
df_gold_vendas = df_gold_vendas.coalesce(1)

logger.info("⏳ Liberando memória antes de salvar o parquet...")
gc.collect()

# Salva o arquivo parquet na camada gold
save_parquet(df_gold_vendas, output_path_vendas_gold)

logger.info("Arquivo gold de vendas salvo com sucesso em: %s", output_path_vendas_gold)


# Finaliza a sessão Spark
data_loader.stop()
