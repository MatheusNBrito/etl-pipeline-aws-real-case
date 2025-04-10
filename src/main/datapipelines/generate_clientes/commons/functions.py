from datapipelines.generate_clientes.config_loader import config
from datapipelines.generate_clientes.commons.spark_session import SparkSessionWrapper
from datapipelines.generate_clientes.commons.variables import clientes_col_seq, clientes_opt_col_seq, enderecos_clientes_col_seq
from pyspark.sql import DataFrame
import logging

# Configura o logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializa a sessão Spark
spark_wrapper = SparkSessionWrapper(app_name="GenerateClientesSparkSession")
spark = spark_wrapper.get_session()


class DataLoader:
    def __init__(self):
        # Inicializa a sessão Spark
        self.spark_wrapper = SparkSessionWrapper(app_name="GenerateClientesSparkSession")
        self.spark = self.spark_wrapper.get_session()

    def load_data(self, raw_tables):
        """
        Função para carregar os dados dos arquivos definidos no arquivo de configuração (application.conf).
        """
        # Carregar os dados dos arquivos conforme o tipo (Parquet, JSON)
        clientes_raw_df = self.spark.read.parquet(raw_tables["CLIENTES_PATH"]).select(*clientes_col_seq)
        clientes_opt_raw_df = self.spark.read.json(raw_tables["CLIENTES_OPT_PATH"]).select(*clientes_opt_col_seq)
        enderecos_clientes_raw_df = self.spark.read.parquet(raw_tables["ENDERECOS_CLIENTES_PATH"]).select(*enderecos_clientes_col_seq)

        
        # Imprimir o esquema para verificação
        clientes_raw_df.printSchema()
        clientes_opt_raw_df.printSchema()
        enderecos_clientes_raw_df.printSchema()

        # Retornar os DataFrames carregados
        return clientes_raw_df, clientes_opt_raw_df, enderecos_clientes_raw_df

    def stop(self):
        """Para a sessão Spark quando não for mais necessária"""
        self.spark_wrapper.stop()

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