from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from datapipelines.generate_vendas.commons.constants import *

def transform_itens_vendas(df: DataFrame) -> DataFrame:
    """
    Aplica as transformações necessárias no DataFrame de itens_vendas
    """
    # Altero os nomes das colunas para os nomes que o modelo espera
    df_transformed_itens_vendas = (
        df.select(
            col(N_ID_IT).alias(CODIGO_ITEM),
            col(V_IT_VD_CONV).alias(TIPO_DESCONTO),
            col(N_VLR_VD).alias(VALOR_UNITARIO),
            col(N_QTD).alias(QUANTIDADE),
        )
    )
    
    return df_transformed_itens_vendas


       
        