variable "region" {
  description = "Região AWS"
  default     = "us-east-2"
}

variable "project" {
  description = "Nome do projeto"
  type        = string
}

variable "environment" {
  description = "Ambiente (ex: dev, prod)"
  type        = string
}

variable "tags" {
  description = "Tags padrão para todos os recursos"
  type        = map(string)
  default     = {
    Project     = "DataEngineeringPortfolio"
    ManagedBy   = "Terraform"
    Environment = "dev"
  }
}

variable "key_name" {
  description = "Nome da chave SSH"
  type        = string
}

variable "alert_email" {
  description = "Email para receber alertas"
  type        = string
}

variable "my_ip" {
  description = "Seu IP para acesso SSH (formato: xxx.xxx.xxx.xxx/32)"
  type        = string
}
