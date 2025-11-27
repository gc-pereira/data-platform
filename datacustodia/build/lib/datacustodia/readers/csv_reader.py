from pyspark.sql import DataFrame
from .base_reader import BaseReader
from awsglue.context import GlueContext


class CSVReader(BaseReader):
    
    def __init__(self, table_name, database, glue_context: GlueContext):
        self.glue_context = glue_context
        self.table_name=table_name 
        self.database=database
        
    def read(self, path: str) -> DataFrame:
        
        schema = self.glue_context.get_catalog_schema(
            database=self.database,
            table_name=self.table_name
        ).as_spark_schema()
        
        return (
            self.spark.read
            .option("header", "false")
            .option("inferSchema", "false")
            .schema(schema)
            .csv(path)
        )
