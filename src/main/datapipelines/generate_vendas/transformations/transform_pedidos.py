from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from datapipelines.generate_vendas.commons.constants import *

def transform_pedidos(df: DataFrame) -> DataFrame:
    """
    Transforma o DataFrame de pedidos, renomeando as colunas conforme o modelo esperado.
    """
    df_transformed_pedidos = (
        df.select(
            col(V_CNL_ORIG_PDD).alias(CANAL_VENDA),
            col(N_ID_PDD).alias(CODIGO_PEDIDO)
        )
    )
    return df_transformed_pedidos
