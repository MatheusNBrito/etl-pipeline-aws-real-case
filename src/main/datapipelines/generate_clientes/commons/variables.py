from datapipelines.generate_clientes.commons.constants import *  

# Sequência de colunas para os dados de CLIENTES
clientes_col_seq = [
    V_ID_CLI, D_DT_NASC, V_SX_CLI, N_EST_CVL
]

# Sequência de colunas para os dados de CLIENTES OPT
clientes_opt_col_seq = [
    V_ID_CLI, B_PUSH, B_SMS, B_EMAIL, B_CALL
]

# Sequência de colunas para os dados de ENDEREÇOS
enderecos_clientes_col_seq = [
    V_ID_CLI, V_LCL, V_UF
]
