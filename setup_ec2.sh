#!/bin/bash

echo "🚀 Iniciando configuração da EC2 para o ambiente Airflow..."

# Atualiza pacotes
sudo yum update -y || sudo apt-get update -y

# Instala Docker
echo "🐳 Instalando Docker..."
sudo yum install -y docker || sudo apt-get install -y docker.io
sudo systemctl start docker
sudo systemctl enable docker

# Instala Docker Compose
echo "📦 Instalando Docker Compose..."
DOCKER_COMPOSE_VERSION="v2.23.3"  # Versão mais recente
sudo curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Adiciona usuário ao grupo docker
sudo usermod -aG docker $USER
echo "🔑 Reinicie a sessão (logout/login) para aplicar grupo docker sem sudo."

# Instala Git
echo "📚 Instalando Git..."
sudo yum install -y git || sudo apt-get install -y git

# Clona o repositório
REPO_URL="https://github.com/MatheusNBrito/etl-pipeline-aws-real-case.git"
CLONE_DIR="etl-pipeline-aws-real-case"

echo "📥 Clonando repositório..."
git clone $REPO_URL $CLONE_DIR
cd $CLONE_DIR || exit 1

# Permissão para scripts
chmod +x start_airflow.sh

# Build da imagem Docker
echo "🛠️ Buildando containers..."
docker-compose build --no-cache

echo "✅ Setup concluído! Agora:"
echo "1. Faça logout e login novamente"
echo "2. Execute: ./start_airflow.sh"