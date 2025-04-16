from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkSessionWrapper:
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
