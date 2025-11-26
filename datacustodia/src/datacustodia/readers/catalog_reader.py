from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from .base_reader import BaseReader


class CatalogReader(BaseReader):

    def __init__(self, push_down_predicate, glue_context: GlueContext):
        self.glue_context = glue_context
        self.push_down_predicate = push_down_predicate

    def read(self, table_identifier: str) -> DataFrame:
        """
        table_identifier format:
            "database.table_name"
        """
        print(f"Lendo tabela do catálogo: {table_identifier} com predicate: {self.push_down_predicate}")
        database, table_name = table_identifier.split(".")[0], table_identifier.split(".")[1]
        dynamic_frame = self.glue_context.create_dynamic_frame.from_catalog(
            database=database,
            table_name=table_name,
            push_down_predicate=self.push_down_predicate,
            transformation_ctx=f"dynamic_frame_from_catalog_{table_name}"
        )
        
        data_frame = dynamic_frame.toDF()
        print(f"Tabela lida com {data_frame.count()} registros.")

        return data_frame
