# ETL Pipeline AWS Real Case

## 📌 Visão Geral

Este projeto implementa uma pipeline ETL utilizando Apache Airflow e PySpark, orquestrando a transformação de dados em um Datalake estruturado com as camadas Bronze, Silver e Gold, armazenado no Amazon S3. Toda a infraestrutura é gerenciada via Terraform e monitorada com AWS CloudWatch.

## 🛠️ Tecnologias

- **Apache Airflow**
- **PySpark**
- **Terraform**
- **AWS (S3, EC2, CloudWatch)**
- **Docker**

## 🚧 Estrutura do Projeto

```
.
├── dags/                   # DAGs do Airflow
├── src/
│   └── main/
│       └── datapipelines/  # Código dos pipelines (Bronze, Silver, Gold)
├── IaC/                    # Código para provisionar recursos AWS (Terraform)
└── dockerfile              # Ambiente local com Docker
```

## 🚀 Próximos passos

- [ ] Implementar o Airflow
- [ ] Subir para a AWS
- [ ] Integrar CI/CD.



