import json
import boto3

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from datetime import datetime

from datacustodia.models.enum import TABELA_IDEMPOTENCIA


class Idempotence:
    def __init__(self, df: DataFrame, table_name: str, partition_columns: str):
        self.table_name = table_name
        self.df = df
        self.partition_columns = partition_columns
       
        
    def extract_partitions(self, df: DataFrame, partition_columns: list):
        """
        Retorna uma lista de dicionários com os valores únicos das partições.
        Exemplo: [{"year": "2024", "month": "01"}, ...]
        """

        # Seleciona apenas colunas de partição e faz distinct
        partitions_df = df.select([col(c) for c in partition_columns]).distinct()

        # Converte para lista de dicts
        return [row.asDict() for row in partitions_df.collect()]


    def create_partition_key(self, partition_values: dict) -> str:
        """
        Cria uma chave idempotente baseada nos valores da partição.
        Ex: {"year":"2024","month":"01"} --> "year=2024#month=01"
        """
        return "#".join([f"{k}={v}" for k, v in partition_values.items()])


    def save_partitions_to_dynamodb(
            self,
            table_name: str,
            partitions: list,
            idempotence_table_name: str,
            partition_key_name: str = "partition_id"
        ):
        """
        Salva partições no DynamoDB com idempotência.
        Usa ConditionExpression para evitar inserir duplicados.
        """
        
        idempotence_dict = {
            "prtc_key ": "",
            "data_prtc": "",
            "last_update_date": datetime.today(),
            "table_name": table_name
        }

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(idempotence_table_name)

        for p in partitions:
            item = p.copy()
            idempotence_dict[partition_key_name] = self.create_partition_key(p)
            idempotence_dict["data_prtc"] = json.dumps(item)
            
            for key in idempotence_dict.keys():
                idempotence_dict[key] = str(idempotence_dict[key])
            
            print('Item a ser inserido na tabela de idempotencia')
            print(idempotence_dict)
            try:
                table.put_item(
                    Item=idempotence_dict,
                    ConditionExpression=f"attribute_not_exists({partition_key_name})"
                )
                print(f"✔ Partição registrada: {idempotence_dict[partition_key_name]}")

            except Exception as e:
                # Se já existe, ignora (comportamento idempotente)
                if "ConditionalCheckFailedException" in str(e):
                    print(f"⚠ Partição já existente (ignorado): {item[partition_key_name]}")
                else:
                    raise e
         
                
    def get(self):
        # 1. Extrair partições únicas
        partitions_list = self.extract_partitions(self.df, self.partition_columns)

        # 2. Salvar no DynamoDB (tabela deve existir)
        self.save_partitions_to_dynamodb(
            idempotence_table_name=TABELA_IDEMPOTENCIA,
            partitions=partitions_list,
            table_name=self.table_name,
            partition_key_name="prtc_key"
        )

