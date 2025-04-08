# Imagem base com Python
FROM python:3.10

LABEL maintainer="Matheus Brito"

# Evita interações durante instalação
ENV DEBIAN_FRONTEND=noninteractive

# Define variáveis de ambiente para Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Instalar dependências do sistema
RUN apt-get update && \
    apt-get install -y curl wget openjdk-17-jdk git netcat-openbsd unzip && \
    apt-get clean

# Instalar Apache Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Variáveis do Airflow
ENV AIRFLOW_VERSION=2.7.3
ENV CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt

# Instalar Airflow e libs Python
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" && \
    pip install pandas boto3 pyarrow pyspark==${SPARK_VERSION}

# Criar diretórios de trabalho
WORKDIR /app
COPY . /app

# Inicializa o Airflow (pré-criação do banco e pastas)
RUN airflow db init

# Porta padrão do webserver
EXPOSE 8080

# Comando de entrada (modo desenvolvimento)
CMD ["airflow", "standalone"]
