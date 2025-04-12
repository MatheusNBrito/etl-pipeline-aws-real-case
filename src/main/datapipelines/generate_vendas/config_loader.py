from pyhocon import ConfigFactory
from pathlib import Path

# Caminho até o arquivo application.conf
conf_path = Path(__file__).resolve().parent / "resources" / "application.conf"

# Carrega e expõe a configuração como um dicionário-like
config = ConfigFactory.parse_file(str(conf_path))

