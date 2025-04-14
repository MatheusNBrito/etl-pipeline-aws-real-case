from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from datapipelines.generate_vendas.commons.constants import *

def transform_vendas(df: DataFrame) -> DataFrame:
    """
    Aplica as transformações necessárias no DataFrame de vendas
    """
    # Altero os nomes das colunas para os nomes que o modelo espera
    df_transformed_vendas = (
        df.select(
            col(D_DT_VD).alias(DATA_EMISSAO),
            col(N_ID_FIL).alias(CODIGO_FILIAL),
            col(N_ID_VD_FIL).alias(CODIGO_CUPOM_VENDA),
            col(V_CLI_COD).alias(CODIGO_CLIENTE),
        )
       .filter(col(N_ID_FIL).isNotNull()) 
        .fillna(
            {
                CODIGO_CLIENTE: "N/I"  
            }
        )
    )
    
    return df_transformed_vendas


       
        