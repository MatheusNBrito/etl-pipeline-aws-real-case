from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from datapipelines.generate_clientes.commons.constants import *

def transform_enderecos_clientes(df: DataFrame) -> DataFrame:
    """
    Transforma o DataFrame de endereços de clientes, renomeando as colunas conforme o padrão da camada processed.

    Args:
        df (DataFrame): DataFrame original da camada raw.

    Returns:
        DataFrame: DataFrame transformado para a camada processed.
    """
    df_transformed_enderecos_clientes = (
        df.select(
            col(V_ID_CLI).alias(CODIGO_CLIENTE),
            col(V_UF).alias(UF),
            col(V_LCL).alias(CIDADE),
        )
    )

    return df_transformed_enderecos_clientes
