from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from datapipelines.generate_vendas.commons.constants import *

def transform_pedidos(df: DataFrame) -> DataFrame:
    """
    Aplica as transformações necessárias no DataFrame de clientes_opt.
    """
    # Altero os nomes das colunas para os nomes finais e marco como False os valores nulos (clientes n tem acesso ao recurso)
    df_transformed_pedidos = (
        df.select(
            col(V_CNL_ORIG_PDD).alias(CANAL_VENDA),
        )
    )
    
    return df_transformed_pedidos