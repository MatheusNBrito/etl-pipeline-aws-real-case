from pathlib import Path
from datapipelines.generate_vendas.commons.functions import DataLoader, save_parquet
from datapipelines.generate_vendas.config_loader import config
from datapipelines.generate_vendas.transformations.transform_vendas import transform_vendas
from datapipelines.generate_vendas.transformations.transform_pedidos import transform_pedidos
from datapipelines.generate_vendas.transformations.transform_itens_vendas import transform_itens_vendas
from datapipelines.generate_vendas.transformations.transform_pedido_venda import transform_pedido_venda
from datapipelines.generate_vendas.commons.spark_session import SparkSessionWrapper

BASE_DIR = Path("/app")


def load_raw_data(spark):
    """Carrega os dados da camada raw"""
    raw_paths = config["input_paths"]["raw_tables"]
    paths = {
        "VENDAS_PATH": str(BASE_DIR / raw_paths["VENDAS_PATH"]),
        "PEDIDOS_PATH": str(BASE_DIR / raw_paths["PEDIDOS_PATH"]),
        "ITENS_VENDA_PATH": str(BASE_DIR / raw_paths["ITENS_VENDA_PATH"]),
        "PEDIDO_VENDA_PATH": str(BASE_DIR / raw_paths["PEDIDO_VENDA_PATH"]),
    }

    loader = DataLoader(spark)
    return loader.load_raw_data(paths)


def apply_transformations(df_tuple):
    """Aplica as transformações nos DataFrames carregados"""
    df_vendas, df_pedidos, df_itens_vendas, df_pedido_venda = df_tuple

    return (
        transform_vendas(df_vendas),
        transform_pedidos(df_pedidos),
        transform_itens_vendas(df_itens_vendas),
        transform_pedido_venda(df_pedido_venda)
    )


def save_processed_data(df_tuple):
    """Salva os dados processados na camada processed"""
    df_vendas, df_pedidos, df_itens_vendas, df_pedido_venda = df_tuple
    paths = config["output_paths"]["processed_tables"]

    save_parquet(df_vendas, str(BASE_DIR / paths["VENDAS_PATH"]))
    save_parquet(df_pedidos, str(BASE_DIR / paths["PEDIDOS_PATH"]))
    if not df_itens_vendas.rdd.isEmpty():
        save_parquet(df_itens_vendas, str(BASE_DIR / paths["ITENS_VENDA_PATH"]))
    else:
        print("⚠️ DataFrame itens_vendas vazio. Não será salvo.")

    save_parquet(df_pedido_venda, str(BASE_DIR / paths["PEDIDO_VENDA_PATH"]))

if __name__ == "__main__":
    from datapipelines.generate_vendas.commons.spark_session import SparkSessionWrapper

    spark_wrapper = SparkSessionWrapper(app_name="ETLVendas")
    spark = spark_wrapper.get_session()

    dfs = load_raw_data(spark)
    dfs_transformed = apply_transformations(dfs)
    save_processed_data(dfs_transformed)

    spark_wrapper.stop()