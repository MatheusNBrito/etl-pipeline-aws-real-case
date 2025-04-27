from datapipelines.generate_vendas.commons.functions import DataLoader, save_parquet
from datapipelines.generate_vendas.config_loader import config
from datapipelines.generate_vendas.transformations.transform_vendas import transform_vendas
from datapipelines.generate_vendas.transformations.transform_pedidos import transform_pedidos
from datapipelines.generate_vendas.transformations.transform_itens_vendas import transform_itens_vendas
from datapipelines.generate_vendas.transformations.transform_pedido_venda import transform_pedido_venda
from datapipelines.generate_vendas.commons.spark_session import SparkSessionWrapper

def load_raw_data(spark):
    """
    Carrega os dados brutos dos arquivos Parquet e JSON.
    """
    raw_paths = config["input_paths"]["raw_tables"]
    loader = DataLoader(spark)
    df_vendas_raw, df_pedidos_raw, df_itens_vendas_raw, df_pedido_venda_raw = loader.load_raw_data(raw_paths)
    return df_vendas_raw, df_pedidos_raw, df_itens_vendas_raw, df_pedido_venda_raw

def apply_transformations(df_tuple):
    """
    Aplica as transformações nos dados brutos.
    """
    df_vendas, df_pedidos, df_itens_vendas, df_pedido_venda = df_tuple
    df_transformed_vendas = transform_vendas(df_vendas)
    df_transformed_pedidos = transform_pedidos(df_pedidos)
    df_transformed_itens_vendas = transform_itens_vendas(df_itens_vendas)
    df_transformed_pedido_venda = transform_pedido_venda(df_pedido_venda)
    return df_transformed_vendas, df_transformed_pedidos, df_transformed_itens_vendas, df_transformed_pedido_venda

def save_processed_data(df_tuple):
    """
    Salva os dados transformados no S3 em formato Parquet.
    """
    df_vendas, df_pedidos, df_itens_vendas, df_pedido_venda = df_tuple
    paths = config["output_paths"]["processed_tables"]
    save_parquet(df_vendas, paths["VENDAS_PATH"])
    save_parquet(df_pedidos, paths["PEDIDOS_PATH"])
    save_parquet(df_itens_vendas, paths["ITENS_VENDA_PATH"])
    save_parquet(df_pedido_venda, paths["PEDIDO_VENDA_PATH"])
    print("Arquivos processados enviados para o S3 com sucesso.")

# Execução principal
if __name__ == "__main__":
    spark_wrapper = SparkSessionWrapper(app_name="ETLVendasProcessed")
    spark = spark_wrapper.get_session()

    raw_dfs = load_raw_data(spark)
    transformed_dfs = apply_transformations(raw_dfs)
    save_processed_data(transformed_dfs)

    spark_wrapper.stop()
