from pyspark.sql import DataFrame
from .base_reader import BaseReader


class CSVReader(BaseReader):
    def read(self, path: str) -> DataFrame:
        return (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(path)
        )
