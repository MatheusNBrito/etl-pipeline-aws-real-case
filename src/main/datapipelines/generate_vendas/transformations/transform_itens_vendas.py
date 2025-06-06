from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from datapipelines.generate_vendas.commons.constants import *

def transform_itens_vendas(df: DataFrame) -> DataFrame:
    """
    Transforma o DataFrame de itens_vendas, renomeando as colunas conforme o modelo esperado.
    """
    df_transformed_itens_vendas = (
        df.select(
            col(N_ID_IT).alias(CODIGO_ITEM),
            col(V_IT_VD_CONV).alias(TIPO_DESCONTO),
            col(N_VLR_VD).alias(VALOR_UNITARIO),
            col(N_QTD).alias(QUANTIDADE),
            col(N_ID_VD_FIL).alias(CODIGO_CUPOM_VENDA)
        )
    )
    return df_transformed_itens_vendas
