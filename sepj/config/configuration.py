import boto3
import os
import threading


class ConfigHandler:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(ConfigHandler, cls).__new__(cls)
                cls._initialized = False
        return cls._instance

    def __init__(self, s3_bucket=None, s3_key_prefix=None, squad=None, produto=None):
        if self._initialized:
            return
        if s3_bucket is None or s3_key_prefix is None or squad is None or produto is None:
            raise ValueError("Todos os parâmetros de configuração devem ser fornecidos na primeira inicialização.")
        
        self.s3_bucket = s3_bucket
        self.s3_key_prefix = s3_key_prefix
        self.squad = squad
        self.produto = produto
        self._validate_config()
        self._initialized = True

    def _validate_config(self):
        """Valida as configurações fornecidas."""
        if not self.s3_bucket:
            raise ValueError("O bucket S3 não pode ser vazio.")
        if not self.s3_key_prefix:
            raise ValueError("O prefixo da chave S3 não pode ser vazio.")
        if not self.squad:
            raise ValueError("A squad não pode ser vazia.")
        if not self.produto:
            raise ValueError("O produto não pode ser vazio.")

    def save_to_s3(self, file_path):
        """
        Salva um arquivo no local especificado no S3.

        :param file_path: Caminho local do arquivo a ser salvo no S3.
        """
        s3_client = boto3.client('s3')
        s3_key = os.path.join(self.s3_key_prefix, os.path.basename(file_path))

        try:
            s3_client.upload_file(file_path, self.s3_bucket, s3_key)
            print(f"Arquivo {file_path} salvo em s3://{self.s3_bucket}/{s3_key}")
        except Exception as e:
            print(f"Erro ao salvar o arquivo no S3: {e}")

    def save_config_to_file(self, config_file_path):
        """
        Salva a configuração atual em um arquivo.

        :param config_file_path: Caminho do arquivo de configuração.
        """
        config = {
            's3_bucket': self.s3_bucket,
            's3_key_prefix': self.s3_key_prefix,
            'squad': self.squad,
            'produto': self.produto
        }

        try:
            with open(config_file_path, 'w') as file:
                for key, value in config.items():
                    file.write(f"{key}={value}\n")
            print(f"Configuração salva em {config_file_path}")
        except Exception as e:
            print(f"Erro ao salvar a configuração: {e}")

    @classmethod
    def load_config_from_file(cls, config_file_path):
        """
        Carrega a configuração de um arquivo e retorna uma instância de ConfigHandler.

        :param config_file_path: Caminho do arquivo de configuração.
        :return: Instância de ConfigHandler.
        """
        config = {}
        try:
            with open(config_file_path, 'r') as file:
                for line in file:
                    key, value = line.strip().split('=')
                    config[key] = value
            print(f"Configuração carregada de {config_file_path}")
            return cls(**config)
        except Exception as e:
            print(f"Erro ao carregar a configuração: {e}")
            return None
