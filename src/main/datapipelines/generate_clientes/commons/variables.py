from datapipelines.generate_clientes.commons.constants import *

# Sequência de colunas para os dados da camada RAW

# Colunas para a tabela CLIENTES (raw)
clientes_col_seq_raw = [
    V_ID_CLI,
    D_DT_NASC,
    V_SX_CLI,
    N_EST_CVL
]

# Colunas para a tabela CLIENTES OPT (raw)
clientes_opt_col_seq__raw = [
    V_ID_CLI,
    B_PUSH,
    B_SMS,
    B_EMAIL,
    B_CALL
]

# Colunas para a tabela ENDEREÇOS CLIENTES (raw)
enderecos_clientes_col_seq_raw = [
    V_ID_CLI,
    V_LCL,
    V_UF
]

# Sequência de colunas para os dados da camada PROCESSED (já padronizados)

# Colunas para a tabela CLIENTES (processed)
clientes_processed_col_seq = [
    CODIGO_CLIENTE,
    DATA_NASCIMENTO,
    SEXO,
    ESTADO_CIVIL,
    IDADE
]

# Colunas para a tabela CLIENTES OPT (processed)
clientes_opt_processed_col_seq = [
    CODIGO_CLIENTE,
    FLAG_LGPD_CALL,
    FLAG_LGPD_SMS,
    FLAG_LGPD_EMAIL,
    FLAG_LGPD_PUSH
]

# Colunas para a tabela ENDEREÇOS CLIENTES (processed)
enderecos_processed_col_seq = [
    CODIGO_CLIENTE,
    UF,
    CIDADE
]
