from pyspark import StorageLevel
from pyspark.sql import DataFrame
from datapipelines.generate_vendas.commons.constants import *
from datapipelines.generate_vendas.commons.functions import DataLoader, save_parquet, replace_null_canal_venda
from datapipelines.generate_vendas.config_loader import config
from datapipelines.generate_vendas.commons.spark_session import SparkSessionWrapper
import logging
import gc

# Configuração do logger
logger = logging.getLogger(__name__)

def load_processed_data(spark):
    """Carrega os dados da camada processed."""
    processed_paths = config["input_paths"]["processed_tables"]
    loader = DataLoader(spark)
    return loader.load_processed_data(processed_paths)

def join_vendas_com_canal(df_vendas: DataFrame, df_pedido_venda: DataFrame, df_pedidos: DataFrame) -> DataFrame:
    """Realiza o join entre vendas, pedido_venda e pedidos."""
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

def aggregate_and_join(df_vendas, df_pedidos, df_itens_vendas, df_pedido_venda):
    """Executa os joins e gera o DataFrame gold."""
    logger.info("Iniciando join de vendas com pedido_venda e pedidos...")
    df_vendas_com_canal = join_vendas_com_canal(df_vendas, df_pedido_venda, df_pedidos)

    # Otimiza para grandes volumes de dados
    df_vendas_com_canal = df_vendas_com_canal.repartition(50, CODIGO_CUPOM_VENDA).cache()
    df_itens_vendas = df_itens_vendas.repartition(50, CODIGO_CUPOM_VENDA).persist(StorageLevel.DISK_ONLY)

    logger.info("Join de vendas com itens_vendas em andamento...")
    df_joined = df_vendas_com_canal.join(df_itens_vendas, on=CODIGO_CUPOM_VENDA, how="left")

    df_gold = df_joined.select(
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

    logger.info("DataFrame gold final gerado com sucesso.")
    return df_gold

def save_gold_data(df_gold: DataFrame):
    """Trata nulos e salva a camada gold no S3."""
    path = config["output_paths"]["gold_tables"]["VENDAS_PATH"]

    df_gold = replace_null_canal_venda(df_gold)
    df_gold = df_gold.coalesce(1)

    logger.info("Liberando memória antes de salvar o Parquet...")
    gc.collect()

    save_parquet(df_gold, path)
    logger.info("Arquivo gold salvo com sucesso em: %s", path)

if __name__ == "__main__":
    spark_wrapper = SparkSessionWrapper(app_name="ETLVendasGOLD")
    spark = spark_wrapper.get_session()

    df_vendas, df_pedidos, df_itens_vendas, df_pedido_venda = load_processed_data(spark)
    df_gold = aggregate_and_join(df_vendas, df_pedidos, df_itens_vendas, df_pedido_venda)
    save_gold_data(df_gold)

    spark_wrapper.stop()
