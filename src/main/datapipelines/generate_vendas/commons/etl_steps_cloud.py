from datapipelines.generate_vendas.commons.functions import DataLoader, save_parquet
from datapipelines.generate_vendas.config_loader import config
from datapipelines.generate_vendas.transformations.transform_vendas import transform_vendas
from datapipelines.generate_vendas.transformations.transform_pedidos import transform_pedidos
from datapipelines.generate_vendas.transformations.transform_itens_vendas import transform_itens_vendas
from datapipelines.generate_vendas.transformations.transform_pedido_venda import transform_pedido_venda


def load_raw_data(spark):
    """Carrega os dados da camada raw (S3)"""
    raw_paths = config["input_paths"]["raw_tables"]

    loader = DataLoader(spark)
    # Carrega os dados das tabelas raw
    df_vendas_raw, df_pedidos_raw, df_itens_vendas_raw, df_pedido_venda_raw = loader.load_raw_data(raw_paths)

    return df_vendas_raw, df_pedidos_raw, df_itens_vendas_raw, df_pedido_venda_raw


def apply_transformations(df_tuple):
    """Aplica as transformações nos DataFrames carregados"""
    df_vendas, df_pedidos, df_itens_vendas, df_pedido_venda = df_tuple

    # Realiza as transformações nos DataFrames de vendas, pedidos, itens de vendas e pedido de venda
    df_transformed_vendas = transform_vendas(df_vendas)
    df_transformed_pedidos = transform_pedidos(df_pedidos)
    df_transformed_itens_vendas = transform_itens_vendas(df_itens_vendas)
    df_transformed_pedido_venda = transform_pedido_venda(df_pedido_venda)

    return df_transformed_vendas, df_transformed_pedidos, df_transformed_itens_vendas, df_transformed_pedido_venda


def save_processed_data(df_tuple):
    """Salva os dados processados na camada processed (S3)"""
    df_vendas, df_pedidos, df_itens_vendas, df_pedido_venda = df_tuple
    paths = config["output_paths"]["processed_tables"]

    # Salva os dados processados nas tabelas correspondentes
    save_parquet(df_vendas, paths["VENDAS_PATH"])
    save_parquet(df_pedidos, paths["PEDIDOS_PATH"])
    save_parquet(df_itens_vendas, paths["ITENS_VENDA_PATH"])
    save_parquet(df_pedido_venda, paths["PEDIDO_VENDA_PATH"])
