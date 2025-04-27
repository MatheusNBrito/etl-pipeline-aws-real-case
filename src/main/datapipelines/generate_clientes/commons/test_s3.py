from pyspark.sql import SparkSession

# SparkSession configurada para acessar o S3
spark = SparkSession.builder \
    .appName("S3WriteTest") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-2.amazonaws.com") \
    .getOrCreate()

# Criar DataFrame simples
data = [(1, "teste")]
columns = ["id", "nome"]
df = spark.createDataFrame(data, columns)

# Caminho S3
path = "s3a://etl-pipeline-aws-dev-bucket/teste_write_s3"

# Escreve no S3
df.write.mode("overwrite").parquet(path)

print("✅ Arquivo enviado para o S3 com sucesso!")

spark.stop()
