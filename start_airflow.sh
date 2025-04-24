#!/bin/bash

echo "⏳ Verificando se o banco do Airflow já está inicializado..."

# Verifica se a tabela core_dag existe no banco
docker-compose run --rm airflow-webserver bash -c "airflow db check" > /dev/null 2>&1

if [ $? -eq 0 ]; then
  echo "✅ Banco de dados já está inicializado. Pulando 'airflow db init'."
else
  echo "⚙️ Banco não encontrado. Executando 'airflow db init'..."
  docker-compose run --rm airflow-webserver airflow db init
  echo "✅ Banco de dados inicializado com sucesso."
fi

echo "🚀 Subindo os containers do Airflow..."
docker-compose up
