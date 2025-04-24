#!/bin/bash

echo "⏳ Verificando se o PostgreSQL está pronto..."

# Espera o PostgreSQL ficar saudável
while ! docker-compose exec postgres pg_isready -U airflow > /dev/null 2>&1; do
  sleep 5
  echo "Aguardando PostgreSQL..."
done

echo "✅ PostgreSQL está pronto!"

echo "⚙️ Verificando se o banco do Airflow já está inicializado..."

# Verifica se a tabela core_dag existe no banco
if docker-compose exec airflow airflow db check > /dev/null 2>&1; then
  echo "✅ Banco de dados já está inicializado."
else
  echo "⚙️ Inicializando banco de dados..."
  docker-compose exec airflow airflow db init
  docker-compose exec airflow airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
  echo "✅ Banco inicializado com usuário admin (senha: admin)."
fi

echo "🚀 Iniciando Airflow Standalone..."
docker-compose up -d airflow

echo "🔍 Acesse o Airflow em: http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4):8081"
echo "👤 Usuário: admin | Senha: admin"