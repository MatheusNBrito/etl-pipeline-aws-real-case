# Imagem base
FROM python:3.10

LABEL maintainer="Matheus Brito"

ENV DEBIAN_FRONTEND=noninteractive

# Versões do Spark e Hadoop
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Instalação de dependências
RUN apt-get update && \
    apt-get install -y curl wget openjdk-17-jdk git netcat-openbsd unzip && \
    apt-get clean

# Instalação do Apache Spark
RUN mkdir -p /opt/spark && \
    cd /opt/spark && \
    wget -q https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --strip-components=1 && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Instalação do Jupyter e registro do kernel
RUN pip install notebook ipykernel && \
    python -m ipykernel install --user --name etl-pipeline-aws --display-name "Python (etl-pipeline-aws)"

# Instalação do Airflow + libs extras
ENV AIRFLOW_VERSION=2.7.3
ENV CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt

RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" && \
    pip install psycopg2-binary boto3 pyspark==${SPARK_VERSION}
    
# Instalação do Jupyter e suas dependências
RUN pip install notebook ipykernel jupyterlab jupyter_server

# Criação da pasta de trabalho
WORKDIR /app
COPY . /app

ENV PYTHONPATH="/app/src/main"

# Copia o requirements.txt e instala as dependências
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Expõe Airflow (8080) e Jupyter (8888)
EXPOSE 8080 8888

# Comando padrão: inicia Airflow (o Jupyter será manual)
CMD ["airflow", "standalone"]

