from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, current_date, when
from datapipelines.generate_clientes.commons.constants import *

def transform_clientes(df: DataFrame) -> DataFrame:
    """
    Transforma o DataFrame de clientes, renomeando colunas, tratando valores nulos e calculando idade.

    Args:
        df (DataFrame): DataFrame original da camada raw.

    Returns:
        DataFrame: DataFrame transformado para a camada processed.
    """
    df_transformed_clientes = (
        df.select(
            col(V_ID_CLI).alias(CODIGO_CLIENTE),
            col(D_DT_NASC).alias(DATA_NASCIMENTO),
            col(V_SX_CLI).alias(SEXO),
            col(N_EST_CVL).alias(ESTADO_CIVIL),
        )
        .filter(col(V_ID_CLI).isNotNull())
        .fillna({
            SEXO: "N/I",
            ESTADO_CIVIL: "N/I"
        })
        .withColumn(
            IDADE,
            when(
                col(DATA_NASCIMENTO).isNotNull(),
                year(current_date()) - year(col(DATA_NASCIMENTO))
            ).otherwise(None)
        )
    )

    return df_transformed_clientes
