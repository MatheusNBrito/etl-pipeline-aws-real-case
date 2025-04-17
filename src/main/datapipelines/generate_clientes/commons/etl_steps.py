from datapipelines.generate_clientes.commons.functions import DataLoader, save_parquet
from datapipelines.generate_clientes.config_loader import config
from datapipelines.generate_clientes.transformations.transform_clientes_opt import transform_clientes_opt
from datapipelines.generate_clientes.transformations.transform_enderecos_clientes import transform_enderecos_clientes
from datapipelines.generate_clientes.transformations.transform_clientes import transform_clientes
from pathlib import Path

BASE_DIR = Path("/app")


def load_raw_data(spark):
    """Carrega os dados da camada raw com base no application.conf"""
    raw_paths = config["input_paths"]["raw_tables"]
    raw_paths = {
        "CLIENTES_PATH": str(BASE_DIR / raw_paths["CLIENTES_PATH"]),
        "CLIENTES_OPT_PATH": str(BASE_DIR / raw_paths["CLIENTES_OPT_PATH"]),
        "ENDERECOS_CLIENTES_PATH": str(BASE_DIR / raw_paths["ENDERECOS_CLIENTES_PATH"]),
    }

    loader = DataLoader(spark)
    df_clientes_raw, df_clientes_opt_raw, df_enderecos_clientes_raw = loader.load_raw_data(raw_paths)

    return df_clientes_raw, df_clientes_opt_raw, df_enderecos_clientes_raw


def apply_transformations(df_tuple):
    """Aplica as transformações em cada DataFrame raw"""
    df_clientes, df_clientes_opt, df_enderecos = df_tuple

    df_transformed_clientes = transform_clientes(df_clientes)
    df_transformed_clientes_opt = transform_clientes_opt(df_clientes_opt)
    df_transformed_enderecos = transform_enderecos_clientes(df_enderecos)

    return df_transformed_clientes, df_transformed_clientes_opt, df_transformed_enderecos


def save_processed_data(df_tuple):
    """Salva os dados transformados na camada processed"""
    df_clientes, df_clientes_opt, df_enderecos = df_tuple
    paths = config["output_paths"]["processed_tables"]

    save_parquet(df_clientes, str(BASE_DIR / paths["CLIENTES_PATH"]))
    save_parquet(df_clientes_opt, str(BASE_DIR / paths["CLIENTES_OPT_PATH"]))
    save_parquet(df_enderecos, str(BASE_DIR / paths["ENDERECOS_CLIENTES_PATH"]))
