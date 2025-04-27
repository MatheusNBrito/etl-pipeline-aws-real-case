# Região AWS
variable "region" {
  description = "Região onde os recursos serão provisionados"
  default     = "us-east-2"
}

# Nome do projeto
variable "project" {
  description = "Nome do projeto"
  type        = string
}

# Ambiente (ex: dev, prod)
variable "environment" {
  description = "Ambiente de implantação (ex: dev, prod)"
  type        = string
}

# Tags padrão para todos os recursos
variable "tags" {
  description = "Mapa de tags aplicadas a todos os recursos"
  type        = map(string)
  default = {
    Project     = "DataEngineeringPortfolio"
    ManagedBy   = "Terraform"
    Environment = "dev"
  }
}

# Nome da chave SSH
variable "key_name" {
  description = "Nome da chave SSH para acesso à instância EC2"
  type        = string
}

# E-mail para recebimento de alertas via SNS
variable "alert_email" {
  description = "Endereço de e-mail para receber alertas de monitoramento"
  type        = string
}

# IP autorizado para acesso SSH (no formato CIDR /32)
variable "my_ip" {
  description = "Endereço IP autorizado para acesso SSH (formato: xxx.xxx.xxx.xxx/32)"
  type        = string
}
