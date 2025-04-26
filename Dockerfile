# Imagem base
FROM python:3.10

LABEL maintainer="Matheus Brito"

ENV DEBIAN_FRONTEND=noninteractive

# Versões do Spark e Hadoop 
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Instalação de dependências básicas + Docker + AWS CLI
RUN apt-get update && \
    apt-get install -y \
        curl \
        wget \
        openjdk-17-jdk \
        git \
        netcat-openbsd \
        unzip \
        postgresql-client \
        apt-transport-https \
        ca-certificates \
        gnupg \
        lsb-release \
        groff \
        less \
        && \
    # Instalação do Docker Engine
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y docker-ce docker-ce-cli containerd.io && \
    # Instalação do AWS CLI v2
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" && \
    unzip /tmp/awscliv2.zip -d /tmp && \
    /tmp/aws/install && \
    rm -rf /tmp/aws /tmp/awscliv2.zip && \
    # Limpeza
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

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

# Instalação do Jupyter
RUN pip install notebook ipykernel && \
    python -m ipykernel install --user --name etl-pipeline-aws --display-name "Python (etl-pipeline-aws)"

# Instalação do Airflow
ENV AIRFLOW_VERSION=2.7.3
ENV CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt

RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" && \
    pip install psycopg2-binary boto3 pyspark==${SPARK_VERSION} && \
    airflow version  # Verificação

# Pasta de trabalho
WORKDIR /app
COPY . /app

# Configura o PYTHONPATH
ENV PYTHONPATH="/app/src/main"

# Instala requirements.txt
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Exposição de portas
EXPOSE 8080 8888

# Comando default comentado para flexibilidade
# CMD ["airflow", "standalone"]
