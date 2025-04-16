#!/bin/bash

echo "🔧 Inicializando Airflow..."

# Sobe containers necessários
docker-compose up airflow-webserver airflow-scheduler -d

# Espera um pouco pro container subir
sleep 5

# Executa comandos dentro do container
docker exec -it airflow-webserver airflow db init

# Cria usuário admin (padrão)
docker exec -it airflow-webserver airflow users create \
  --username airflow \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email airflow@local.com \
  --password airflow

echo "✅ Airflow inicializado. Acesse: http://localhost:8081"
