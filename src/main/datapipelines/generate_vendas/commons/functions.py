from datapipelines.generate_clientes.config_loader import config
from datapipelines.generate_clientes.commons.variables import *
from pyspark.sql import DataFrame
import logging
from pyspark.sql.functions import col, lit, when, isnull, lit, length, explode, count, upper, lower, regexp_replace, regexp_extract
from typing import Tuple


# Configura o logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, spark):
        self.spark = spark

        """
        Função para carregar os dados dos arquivos
        :Layer: Indica a camada de dados a ser carregada ("raw" ou "processed").
        """

    def load_raw_data(self, tables: dict) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Carrega os dados da camada raw, utilizando os nomes de colunas originais.
        """
        clientes_df = self.spark.read.parquet(tables["CLIENTES_PATH"]).select(*clientes_col_seq_raw)
        clientes_opt_df = self.spark.read.json(tables["CLIENTES_OPT_PATH"]).select(*clientes_opt_col_seq__raw)
        enderecos_df = self.spark.read.parquet(tables["ENDERECOS_CLIENTES_PATH"]).select(*enderecos_clientes_col_seq_raw)

        return clientes_df, clientes_opt_df, enderecos_df

    def load_processed_data(self, tables: dict) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Carrega os dados da camada processed, utilizando os nomes padronizados.
        """
        clientes_df = self.spark.read.parquet(tables["CLIENTES_PATH"]).select(*clientes_processed_col_seq)
        clientes_opt_df = self.spark.read.parquet(tables["CLIENTES_OPT_PATH"]).select(*clientes_opt_processed_col_seq)
        enderecos_df = self.spark.read.parquet(tables["ENDERECOS_CLIENTES_PATH"]).select(*enderecos_processed_col_seq)

        return clientes_df, clientes_opt_df, enderecos_df

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
