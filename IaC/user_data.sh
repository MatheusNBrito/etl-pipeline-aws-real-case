#!/bin/bash

set -e

# Atualiza os pacotes do sistema
apt-get update -y

# Instala dependências para o Docker
apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    apt-transport-https

# Adiciona chave GPG oficial do Docker
mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Adiciona repositório Docker
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

# Atualiza de novo e instala Docker Engine
apt-get update -y
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Starta o serviço do Docker
systemctl enable docker
systemctl start docker

# Adiciona o usuário ubuntu no grupo docker (importante para usar sem sudo)
usermod -aG docker ubuntu

# Instala docker-compose (caso precise ainda)
apt-get install -y docker-compose

# Agendamento para desligar todos os dias às 20h UTC (17h Brasília)
echo "0 20 * * * root /sbin/shutdown -h now" >> /etc/crontab
