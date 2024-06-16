import yaml

class YamlReader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None

    def _load_yaml(self):
        try:
            with open(self.file_path, 'r') as file:
                self.data = yaml.safe_load(file)
        except FileNotFoundError:
            print(f"O arquivo {self.file_path} não foi encontrado.")
        except yaml.YAMLError as exc:
            print(f"Erro ao ler o arquivo YAML: {exc}")

    def get_data(self):
        return self.data

    def reload(self):
        self._load_yaml()

    def __enter__(self):
        self._load_yaml()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Aqui você pode adicionar qualquer limpeza que precise ser feita
        pass
