# Imagem base (MANTIDO python:3.10)
FROM python:3.10

LABEL maintainer="Matheus Brito"

ENV DEBIAN_FRONTEND=noninteractive

# Versões do Spark e Hadoop 
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Instalação de dependências 
RUN apt-get update && \
    apt-get install -y curl wget openjdk-17-jdk git netcat-openbsd unzip postgresql-client && \
    apt-get clean


# Instalação do Apache Spark 
RUN mkdir -p /opt/spark && \
    cd /opt/spark && \
    ( \
      wget -t 3 --waitretry=30 --no-check-certificate https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz || \
      wget -t 3 --waitretry=30 --no-check-certificate https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz || \
      wget -t 3 --waitretry=30 --no-check-certificate https://ftp.unicamp.br/pub/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    ) && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz --strip-components=1 && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Instalação do Jupyter (MANTIDO)
RUN pip install notebook ipykernel && \
    python -m ipykernel install --user --name etl-pipeline-aws --display-name "Python (etl-pipeline-aws)"

# Instalação do Airflow (MANTIDO, só adicionei validação)
ENV AIRFLOW_VERSION=2.7.3
ENV CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt

RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" && \
    pip install psycopg2-binary boto3 pyspark==${SPARK_VERSION} && \
    airflow version  # Verificação
    
# Criação da pasta de trabalho (MANTIDO)
WORKDIR /app
COPY . /app

ENV PYTHONPATH="/app/src/main"

# Instala requirements.txt (MANTIDO)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

EXPOSE 8080 8888

# CMD ["airflow", "standalone"]