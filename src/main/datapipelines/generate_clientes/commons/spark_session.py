from pyspark.sql import SparkSession
import logging

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkSessionWrapper:
    """
    Wrapper para criação e gerenciamento da Spark Session com configurações específicas para integração com S3.
    """

    def __init__(self, app_name="GenerateClientesSparkSession", master="local[*]"):
        """
        Inicializa a sessão Spark com configurações padrões e específicas para o acesso ao S3.

        Args:
            app_name (str, optional): Nome do aplicativo Spark. Default é "GenerateClientesSparkSession".
            master (str, optional): URL do cluster master. Default é "local[*]".
        """
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
            .config("spark.hadoop.io.nativeio", "false") \
            .master(master) \
            .getOrCreate()

        # Configurações para leitura e escrita com s3a://
        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.multiobjectdelete.enable", "false")

        logger.info(f"Spark session started with app: {app_name}")

    def get_session(self):
        """
        Retorna a instância da Spark Session.
        """
        return self.spark

    def stop(self):
        """
        Finaliza a Spark Session.
        """
        logger.info("Stopping Spark session")
        self.spark.stop()

# Teste da criação da sessão Spark
if __name__ == "__main__":
    spark_wrapper = SparkSessionWrapper(app_name="TestSession")
    spark = spark_wrapper.get_session()

    print("Sessão Spark criada com sucesso.")
    print(f"Versão do Spark: {spark.version}")

    spark_wrapper.stop()
