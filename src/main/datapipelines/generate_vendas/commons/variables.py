from datapipelines.generate_vendas.commons.constants import *

# Sequência de colunas para os dados da camada RAW

# Colunas da tabela VENDAS (raw)
vendas_col_seq_raw = [
    D_DT_VD,
    N_ID_FIL,
    N_ID_VD_FIL,
    V_CLI_COD
]

# Colunas da tabela PEDIDOS (raw)
pedidos_col_seq_raw = [
    V_CNL_ORIG_PDD,
    N_ID_PDD
]

# Colunas da tabela ITENS_VENDAS (raw)
itens_vendas_col_seq_raw = [
    N_ID_IT,
    V_IT_VD_CONV,
    N_VLR_VD,
    N_QTD,
    N_ID_VD_FIL
]

# Sequência de colunas para os dados da camada PROCESSED

# Colunas da tabela VENDAS (processed)
vendas_processed_col_seq = [
    DATA_EMISSAO,
    CODIGO_FILIAL,
    CODIGO_CUPOM_VENDA,
    CODIGO_CLIENTE
]

# Colunas da tabela PEDIDOS (processed)
pedidos_processed_col_seq = [
    CANAL_VENDA,
    CODIGO_PEDIDO
]

# Colunas da tabela ITENS_VENDAS (processed)
itens_vendas_processed_col_seq = [
    CODIGO_ITEM,
    TIPO_DESCONTO,
    VALOR_UNITARIO,
    QUANTIDADE,
    CODIGO_CUPOM_VENDA
]
