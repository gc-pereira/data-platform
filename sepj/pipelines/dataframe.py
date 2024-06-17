import pandas as pd

class DataFrameHandler:
    def __init__(self, data=None):
        """
        Inicializa o DataFrameHandler com um DataFrame vazio ou com dados fornecidos.

        :param data: Opcional, pode ser um caminho para um arquivo CSV/Excel ou um DataFrame.
        """
        if isinstance(data, str):
            if data.endswith('.csv'):
                self.df = pd.read_csv(data)
            elif data.endswith('.xlsx'):
                self.df = pd.read_excel(data)
            elif data.endswith('.parquet'):
                self.df = pd.read_parquet(data)
            else:
                raise ValueError("Formato de arquivo não suportado.")
        elif isinstance(data, pd.DataFrame):
            self.df = data
        else:
            self.df = pd.DataFrame()
            
    def write(self, file_type):
        ...

    def load_csv(self, file_path):
        """Carrega dados de um arquivo CSV."""
        self.df = pd.read_csv(file_path)

    def load_excel(self, file_path):
        """Carrega dados de um arquivo Excel."""
        self.df = pd.read_excel(file_path)
        
    def load_excel(self, file_path):
        """Carrega dados de um arquivo Parquet."""
        self.df = pd.read_excel(file_path)

    def filter_data(self, condition):
        """
        Filtra o DataFrame com base em uma condição.

        :param condition: Uma expressão booleana para filtrar os dados.
        :return: Um novo DataFrameHandler com os dados filtrados.
        """
        filtered_df = self.df.query(condition)
        return DataFrameHandler(filtered_df)

    def add_column(self, column_name, data):
        """
        Adiciona uma nova coluna ao DataFrame.

        :param column_name: Nome da nova coluna.
        :param data: Dados para a nova coluna (deve ter o mesmo comprimento que o DataFrame).
        """
        self.df[column_name] = data

    def remove_column(self, column_name):
        """Remove uma coluna do DataFrame."""
        self.df.drop(columns=[column_name], inplace=True)

    def aggregate_data(self, group_by_columns, agg_dict):
        """
        Agrega dados do DataFrame.

        :param group_by_columns: Colunas pelas quais os dados serão agrupados.
        :param agg_dict: Dicionário definindo as agregações.
        :return: Um novo DataFrameHandler com os dados agregados.
        """
        aggregated_df = self.df.groupby(group_by_columns).agg(agg_dict).reset_index()
        return DataFrameHandler(aggregated_df)

    def get_data(self):
        """Retorna o DataFrame."""
        return self.df
