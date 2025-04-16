from datapipelines.generate_clientes.commons.constants import *  

# Sequência de colunas para os dados de CLIENTES
clientes_col_seq_raw = [
    V_ID_CLI, D_DT_NASC, V_SX_CLI, N_EST_CVL
]

# Sequência de colunas para os dados de CLIENTES OPT
clientes_opt_col_seq__raw = [
    V_ID_CLI, B_PUSH, B_SMS, B_EMAIL, B_CALL
]

# Sequência de colunas para os dados de ENDEREÇOS
enderecos_clientes_col_seq_raw = [
    V_ID_CLI, V_LCL, V_UF
]


####-CONTINUAR DAQUI, MAPEANDO AS NOVAS VARIAVEIS DA CAMADA PROCESSADO, 
# TENDO EM VISTA QUE O NOMES DAS COLUNAS MUDARAM
# Colunas da camada PROCESSED (padronizadas e com valores tratados)
clientes_processed_col_seq = [
    CODIGO_CLIENTE,
    DATA_NASCIMENTO,
    SEXO,
    ESTADO_CIVIL,
    IDADE
]

clientes_opt_processed_col_seq = [
    CODIGO_CLIENTE,
    FLAG_LGPD_CALL,
    FLAG_LGPD_SMS,
    FLAG_LGPD_EMAIL,
    FLAG_LGPD_PUSH
]

enderecos_processed_col_seq = [
    CODIGO_CLIENTE,
    UF,
    CIDADE
]

