from datapipelines.generate_vendas.commons.spark_session import SparkSessionWrapper
from datapipelines.generate_vendas.commons.etl_steps import (
    load_raw_data,
    apply_transformations,
    save_processed_data
)
from datapipelines.logger_config import get_logger

# Inicializa o logger
logger = get_logger("raw_to_processed_vendas")

def main():
    """
    Executa o pipeline de carga da camada raw para a camada processed de vendas.
    """
    logger.info("Iniciando sessão Spark para carga da camada processed.")
    spark_wrapper = SparkSessionWrapper(app_name="GenerateVendasSparkSession")
    spark = spark_wrapper.get_session()

    try:
        logger.info("Lendo dados brutos da camada raw...")
        df_raws = load_raw_data(spark)

        logger.info("Aplicando transformações nos dados...")
        df_processed = apply_transformations(df_raws)

        logger.info("Salvando arquivos na camada processed...")
        save_processed_data(df_processed)

        logger.info("Pipeline raw_to_processed finalizada com sucesso.")
    except Exception as e:
        logger.exception("Erro durante a execução da pipeline raw_to_processed:")
        raise
    finally:
        spark_wrapper.stop()
        logger.info("Sessão Spark finalizada.")

if __name__ == "__main__":
    main()
