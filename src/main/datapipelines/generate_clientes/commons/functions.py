from datapipelines.generate_clientes.commons.variables import *
from pyspark.sql import DataFrame
import logging
from pyspark.sql.functions import col, when
from typing import Tuple

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    """
    Classe responsável pelo carregamento dos dados nas diferentes camadas (raw e processed).
    """

    def __init__(self, spark):
        self.spark = spark

    def load_raw_data(self, tables: dict) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Carrega os dados da camada raw utilizando os esquemas de colunas brutos.

        Args:
            tables (dict): Dicionário com os caminhos dos arquivos.

        Returns:
            Tuple[DataFrame, DataFrame, DataFrame]: DataFrames de clientes, clientes_opt e endereços.
        """
        clientes_df = self.spark.read.parquet(tables["CLIENTES_PATH"]).select(*clientes_col_seq_raw)
        clientes_opt_df = self.spark.read.json(tables["CLIENTES_OPT_PATH"]).select(*clientes_opt_col_seq__raw)
        enderecos_df = self.spark.read.parquet(tables["ENDERECOS_CLIENTES_PATH"]).select(*enderecos_clientes_col_seq_raw)

        return clientes_df, clientes_opt_df, enderecos_df

    def load_processed_data(self, tables: dict) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Carrega os dados da camada processed utilizando os esquemas de colunas padronizados.

        Args:
            tables (dict): Dicionário com os caminhos dos arquivos.

        Returns:
            Tuple[DataFrame, DataFrame, DataFrame]: DataFrames de clientes, clientes_opt e endereços.
        """
        clientes_df = self.spark.read.parquet(tables["CLIENTES_PATH"]).select(*clientes_processed_col_seq)
        clientes_opt_df = self.spark.read.parquet(tables["CLIENTES_OPT_PATH"]).select(*clientes_opt_processed_col_seq)
        enderecos_df = self.spark.read.parquet(tables["ENDERECOS_CLIENTES_PATH"]).select(*enderecos_processed_col_seq)

        return clientes_df, clientes_opt_df, enderecos_df

def save_parquet(df: DataFrame, output_path: str, mode: str = "overwrite") -> None:
    """
    Salva o DataFrame no formato Parquet no caminho especificado.

    Args:
        df (DataFrame): DataFrame a ser salvo.
        output_path (str): Caminho de destino no S3.
        mode (str, optional): Modo de gravação (default: "overwrite").
    """
    try:
        df.write.mode(mode).parquet(output_path)
        logger.info(f"Arquivo salvo com sucesso em: {output_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo em: {output_path} - Erro: {str(e)}")
        raise e

def replace_nulls(df: DataFrame) -> DataFrame:
    """
    Substitui valores nulos nas colunas do DataFrame.
    - Para colunas booleanas, substitui nulos por False.
    - Para outras colunas, substitui nulos por "N/I".

    Args:
        df (DataFrame): DataFrame a ser tratado.

    Returns:
        DataFrame: DataFrame com nulos substituídos.
    """
    for column in df.columns:
        if dict(df.dtypes)[column] == 'boolean':
            df = df.withColumn(column, when(col(column).isNull(), False).otherwise(col(column)))
        else:
            df = df.withColumn(column, when(col(column).isNull(), "N/I").otherwise(col(column)))
    return df
