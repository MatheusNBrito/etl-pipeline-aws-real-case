# Nome da chave SSH para acesso à EC2
key_name = "etl-key-aws"

# Projeto e ambiente
project     = "etl-pipeline-aws"
environment = "dev"

# Tags padrão para os recursos
tags = {
  Project     = "ETL Clientes/Vendas"
  Environment = "dev"
  Owner       = "matheusnbrito"
  ManagedBy   = "Terraform"
}

# IP que terá acesso ao SSH (formato CIDR)
my_ip = "191.217.211.182" 

# Email que receberá os alertas da instância
alert_email = "matheusnunesdebrito@gmail.com"
