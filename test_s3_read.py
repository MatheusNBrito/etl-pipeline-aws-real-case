from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestS3Read").getOrCreate()

df = spark.read.parquet("s3a://etl-pipeline-aws-dev-bucket/raw/vendas.parquet")
df.show(5)
