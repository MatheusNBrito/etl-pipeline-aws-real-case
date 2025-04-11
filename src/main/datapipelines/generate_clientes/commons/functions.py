from datapipelines.generate_clientes.config_loader import config
from datapipelines.generate_clientes.commons.variables import clientes_col_seq, clientes_opt_col_seq, enderecos_clientes_col_seq
from pyspark.sql import DataFrame
import logging

# Configura o logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, spark):
        self.spark = spark

    def load_data(self, layer: str, tables: dict):
        """
        Função para carregar os dados dos arquivos
        :Layer: Indica a camada de dados a ser carregada ("raw" ou "processed").
        """
        if layer == "raw":
            # Carregar os dados da camada raw (bruto)
            clientes_df = self.spark.read.parquet(tables["CLIENTES_PATH"]).select(*clientes_col_seq)
            clientes_opt_df = self.spark.read.json(tables["CLIENTES_OPT_PATH"]).select(*clientes_opt_col_seq)
            enderecos_clientes_df = self.spark.read.parquet(tables["ENDERECOS_CLIENTES_PATH"]).select(*enderecos_clientes_col_seq)
        elif layer == "processed":
            # Carregar os dados da camada processed (transformados)
            clientes_df = self.spark.read.parquet(tables["CLIENTES_PATH"]).select(*clientes_col_seq)
            clientes_opt_df = self.spark.read.parquet(tables["CLIENTES_OPT_PATH"]).select(*clientes_opt_col_seq)
            enderecos_clientes_df = self.spark.read.parquet(tables["ENDERECOS_CLIENTES_PATH"]).select(*enderecos_clientes_col_seq)
        else:
            raise ValueError("A camada especificada deve ser 'raw' ou 'processed'.")

        # Imprimir o esquema para verificação
        clientes_df.printSchema()
        clientes_opt_df.printSchema()
        enderecos_clientes_df.printSchema()

        # Retornar os DataFrames carregados
        return clientes_df, clientes_opt_df, enderecos_clientes_df


def save_parquet(df: DataFrame, output_path: str, mode: str = "overwrite") -> None:
    """
    :param df: DataFrame a ser salvo
    :param output_path: Caminho de saída
    :param mode: Modo de escrita (overwrite, append, etc)
    """
    try:
        df.write.mode(mode).parquet(output_path)
        logger.info(f"✅ Arquivo salvo com sucesso em: {output_path}")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar arquivo em: {output_path} - Erro: {str(e)}")
        raise e
