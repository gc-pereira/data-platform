from pyspark.sql import DataFrame
from .base_reader import BaseReader
from awsglue.context import GlueContext


class CSVReader(BaseReader):
    
    def __init__(self, spark, schema):
        self.schema = schema
        self.spark = spark
        
    def read(self, path: str) -> DataFrame:    
        return (
            self.spark.read
            .option("header", "false")
            .option("inferSchema", "false")
            .schema(self.schema)
            .csv(path)
        )
