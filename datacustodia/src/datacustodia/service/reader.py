from datetime import datetime
from dateutil.relativedelta import relativedelta
from datacustodia.models.enum import DE_PARA_BUCKET_NAME

from datacustodia.readers.catalog_reader import CatalogReader
from datacustodia.readers.csv_reader import CSVReader
from datacustodia.readers.parquet_reader import ParquetReader


class Reader:
    
    def __init__(self, spark, glue_context, config, **kwargs):
        self.data_layer = config['DataLayer']
        self.spark = spark
        self.glue_context = glue_context
        self.config = config
        self.path = kwargs.get('path')   

    
    def __construct_predicate(self, predicate_template: list):
        """
        Substitui os placeholders no template do predicate pelos valores correspondentes do config.
        """
        predicates = []
        for predicate in predicate_template:
            if predicate['Type'] == 'Date':
                date_predicate = datetime.today() + relativedelta(days=predicate['Filter'])
                date_predicate = date_predicate.strftime(predicate['Pattern'])
                push_down = predicate['PushDown'].format(date_predicate)
                predicates.append(push_down)
            elif predicate['Type'] == 'Integer':
                ...
            else:
                raise Exception('O tipo de dado passado para predicate não está mapeado')
        return ' AND '.join(predicates)

    def __get_predicate(self, config: dict, table_name: str):
        """
        Retorna o Predicate da dependência cujo TableName corresponde ao solicitado.
        Se não encontrar, retorna None.
        """
        dependencies = config.get("Dependencies", [])

        for dep in dependencies:
            if dep.get("TableName") in table_name:
                return self.__construct_predicate(dep.get("Predicate"))

        return None
    
    def read(self):
        if self.data_layer == 'CSV':
            print('lendo CSV')
            path = f"s3://{DE_PARA_BUCKET_NAME[self.config['DataLayer']]}/{self.config['TableName']}/"
            return CSVReader(context=self.spark).read(path=path)
        elif self.data_layer in ['SOR', 'SOT', 'SPEC']:
            print('lendo tabelas')
            push_down_predicate = self.__get_predicate(self.config, self.path)
            return CatalogReader(push_down_predicate=push_down_predicate, glue_context=self.glue_context).read(self.path)
        else:
            raise Exception()
