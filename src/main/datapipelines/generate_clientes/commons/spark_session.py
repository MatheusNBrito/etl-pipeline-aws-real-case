from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkSessionWrapper:
    def __init__(self, app_name="GenerateClientesSparkSession", master="local[*]"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
            .config("spark.hadoop.io.nativeio", "false") \
            .master(master) \
            .getOrCreate()
        
        #  Configurações necessárias para leitura e escrita com s3a://
        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

        logger.info(f"✅ Spark session started with app: {app_name}")

    def get_session(self):
        return self.spark

    def stop(self):
        logger.info("🛑 Stopping Spark session")
        self.spark.stop()

#Bloco para teste de conexão com o spark

if __name__ == "__main__":
    spark_wrapper = SparkSessionWrapper(app_name="TestSession")
    spark = spark_wrapper.get_session()

    print("Sessão Spark criada com sucesso!")
    print(f"Versão do Spark: {spark.version}")

    spark_wrapper.stop()
