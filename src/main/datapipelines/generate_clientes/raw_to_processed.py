from datapipelines.generate_clientes.commons.etl_steps import (
    load_raw_data,
    apply_transformations,
    save_processed_data
)
from datapipelines.generate_clientes.commons.spark_session import SparkSessionWrapper


def main():
    spark_wrapper = SparkSessionWrapper(app_name="GenerateClientesSparkSession")
    spark = spark_wrapper.get_session()

    df_raws = load_raw_data(spark)
    df_processed = apply_transformations(df_raws)
    save_processed_data(df_processed)

    spark_wrapper.stop()


if __name__ == "__main__":
    main()
