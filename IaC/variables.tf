variable "region" {
  default = "us-east-2"
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
}

variable "s3_bucket_name" {
  default = "nao-utilizado-mais"
}

variable "ami_id" {
  default = "ami-0945157a116cd5d12"
}

variable "key_name" {
  description = "Nome da chave SSH"
  type        = string
}
