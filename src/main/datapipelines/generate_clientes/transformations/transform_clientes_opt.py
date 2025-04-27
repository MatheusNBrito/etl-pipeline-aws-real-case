from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from datapipelines.generate_clientes.commons.constants import (
    V_ID_CLI,
    B_PUSH,
    B_SMS,
    B_EMAIL,
    B_CALL,
    CODIGO_CLIENTE,
    FLAG_LGPD_PUSH,
    FLAG_LGPD_SMS,
    FLAG_LGPD_EMAIL,
    FLAG_LGPD_CALL,
)

def transform_clientes_opt(df: DataFrame) -> DataFrame:
    """
    Transforma o DataFrame de clientes_opt, renomeando as colunas e preenchendo nulos com False.

    Args:
        df (DataFrame): DataFrame original da camada raw.

    Returns:
        DataFrame: DataFrame transformado para a camada processed.
    """
    df_transformed_clientes_opt = (
        df.select(
            col(V_ID_CLI).alias(CODIGO_CLIENTE),
            col(B_PUSH).alias(FLAG_LGPD_PUSH),
            col(B_SMS).alias(FLAG_LGPD_SMS),
            col(B_EMAIL).alias(FLAG_LGPD_EMAIL),
            col(B_CALL).alias(FLAG_LGPD_CALL),
        )
        .filter(col(V_ID_CLI).isNotNull())
        .fillna({
            FLAG_LGPD_PUSH: False,
            FLAG_LGPD_SMS: False,
            FLAG_LGPD_EMAIL: False,
            FLAG_LGPD_CALL: False
        })
    )

    return df_transformed_clientes_opt
