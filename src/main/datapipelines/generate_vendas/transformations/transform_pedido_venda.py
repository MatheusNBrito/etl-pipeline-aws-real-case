from ast import alias
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from datapipelines.generate_vendas.commons.constants import *

def transform_pedido_venda(df: DataFrame) -> DataFrame:
    """
    Aplica as transformações necessárias no DataFrame de pedidos.
    """
    # Altero os nomes das colunas para os nomes finais e marco como False os valores nulos (clientes n tem acesso ao recurso)
    df_transformed_pedido_venda = (
        df.select(
            col(N_ID_VD_FIL).alias(CODIGO_CUPOM_VENDA),
            col(N_ID_PDD).alias(CODIGO_PEDIDO),
            col(N_ID_FIL).alias(CODIGO_FILIAL),
        )
    )
    
    return df_transformed_pedido_venda