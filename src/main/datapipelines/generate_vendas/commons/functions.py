from datapipelines.generate_vendas.commons.variables import *
from pyspark.sql import DataFrame
import logging
from pyspark.sql.functions import col, when
from typing import Tuple


# Configura o logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, spark):
        self.spark = spark

    def load_raw_data(self, tables: dict) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        """
        Carrega os dados da camada raw, utilizando os nomes de colunas originais.
        """
        vendas_df = self.spark.read.parquet(tables["VENDAS_PATH"]).select(*vendas_col_seq_raw)
        pedidos_df = self.spark.read.parquet(tables["PEDIDOS_PATH"]).select(*pedidos_col_seq_raw)
        itens_vendas_df = self.spark.read.parquet(tables["ITENS_VENDA_PATH"]).select(*itens_vendas_col_seq_raw)
        pedido_venda_df = self.spark.read.parquet(tables["PEDIDO_VENDA_PATH"])

        return vendas_df, pedidos_df, itens_vendas_df, pedido_venda_df

    def load_processed_data(self, tables: dict) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Carrega os dados da camada processed, utilizando os nomes padronizados.
        """
        vendas_df = self.spark.read.parquet(tables["VENDAS_PATH"]).select(*vendas_processed_col_seq)
        pedidos_df = self.spark.read.parquet(tables["PEDIDOS_PATH"]).select(*pedidos_processed_col_seq)
        itens_vendas_df = self.spark.read.parquet(tables["ITENS_VENDA_PATH"]).select(*itens_vendas_processed_col_seq)
        pedido_venda_df = self.spark.read.parquet(tables["PEDIDO_VENDA_PATH"])

        return vendas_df, pedidos_df, itens_vendas_df, pedido_venda_df

# Salvar os arquivos
def save_parquet(df: DataFrame, output_path: str, mode: str = "overwrite") -> None:
    try:
        df.write.mode(mode).parquet(output_path)
        logger.info(f"✅ Arquivo salvo com sucesso em: {output_path}")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar arquivo em: {output_path} - Erro: {str(e)}")
        raise e

# Transforma os dados nulos para N/I
def replace_nulls(df: DataFrame) -> DataFrame:
    for column in df.columns:
        # Verifica se a coluna é booleana
        if dict(df.dtypes)[column] == 'boolean':
            df = df.withColumn(column, when(col(column).isNull(), False).otherwise(col(column)))
        else:
            # Para as outras colunas, substitui os nulos por "N/I"
            df = df.withColumn(column, when(col(column).isNull(), "N/I").otherwise(col(column)))
    return df


def replace_null_canal_venda(df: DataFrame) -> DataFrame:
    """
    Substitui valores nulos da coluna 'canal_venda' por 'N/I' de forma eficiente.
    """
    return df.withColumn(CANAL_VENDA, when(col(CANAL_VENDA).isNull(), "N/I").otherwise(col(CANAL_VENDA)))
