from pyspark.sql import SparkSession
import logging

# Configuração do logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkSessionWrapper:
    """
    Wrapper para criação e gerenciamento da SparkSession para o pipeline de vendas.
    """

    def __init__(self, app_name="GenerateVendasSparkSession", master="local[*]"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .config("spark.hadoop.io.nativeio", "false") \
            .master(master) \
            .config("spark.driver.memory", "10g") \
            .config("spark.executor.memory", "10g") \
            .config("spark.sql.shuffle.partitions", "50") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()

        # Configurações necessárias para leitura e escrita com S3
        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

        logger.info(f"Spark session started with app: {app_name}")

    def get_session(self):
        """Retorna a sessão Spark criada."""
        return self.spark

    def stop(self):
        """Encerra a sessão Spark."""
        logger.info("Stopping Spark session")
        self.spark.stop()

# Teste da criação da sessão Spark
if __name__ == "__main__":
    spark_wrapper = SparkSessionWrapper(app_name="TestSession")
    spark = spark_wrapper.get_session()

    print("Sessão Spark criada com sucesso.")
    print(f"Versão do Spark: {spark.version}")

    spark_wrapper.stop()
