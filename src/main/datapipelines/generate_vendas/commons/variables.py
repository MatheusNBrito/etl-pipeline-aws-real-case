from datapipelines.generate_vendas.commons.constants import *  

# Sequência de colunas para os dados de Vendas
vendas_col_seq_raw = [
    D_DT_VD, N_ID_FIL, N_ID_VD_FIL, V_CLI_COD
]

# Sequência de colunas para os dados de pedidos
pedidos_col_seq_raw = [
    V_CNL_ORIG_PDD, N_ID_PDD
]

# Sequência de colunas para os dados itens_venas
itens_vendas_col_seq_raw = [
    N_ID_IT, V_IT_VD_CONV, N_VLR_VD, N_QTD, N_ID_VD_FIL
]

#############################################################################

vendas_processed_col_seq = [
    DATA_EMISSAO,
    CODIGO_FILIAL,
    CODIGO_CUPOM_VENDA,
    CODIGO_CLIENTE
]

pedidos_processed_col_seq = [
    CANAL_VENDA,
    CODIGO_PEDIDO
   ]

itens_vendas_processed_col_seq = [
    CODIGO_ITEM,
    TIPO_DESCONTO,
    VALOR_UNITARIO,
    QUANTIDADE,
    CODIGO_CUPOM_VENDA
]
