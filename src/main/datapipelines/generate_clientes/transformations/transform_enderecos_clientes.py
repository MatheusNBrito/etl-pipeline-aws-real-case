from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from datapipelines.generate_clientes.commons.constants import *

from datapipelines.generate_clientes.commons.functions import save_parquet

def transform_enderecos_clientes(df: DataFrame) -> DataFrame:
    """
    Aplica as transformações necessárias no DataFrame de clientes_opt.
    """
    # Altero os nomes das colunas para os nomes finais 
    df_transformed_enderecos_clientes = (
        df.select(
            col(V_ID_CLI).alias(CODIGO_CLIENTE),
            col(V_UF).alias(UF),
            col(V_LCL).alias(CIDADE),
        )
    )
    
    return df_transformed_enderecos_clientes

