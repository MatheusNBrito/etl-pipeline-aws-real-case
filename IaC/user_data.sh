#!/bin/bash

# Atualiza os pacotes do sistema
yum update -y

# Instala o Docker
yum install -y docker
systemctl start docker
systemctl enable docker  # inicia o Docker automaticamente ao ligar
usermod -aG docker ec2-user

# Instala o Docker Compose (última versão disponível)
curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Agendamento para desligar todos os dias às 20h UTC (17hrs Brasília)
echo "0 20 * * * root /sbin/shutdown -h now" >> /etc/crontab
