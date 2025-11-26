from pyspark.sql import SparkSession, DataFrame
from .base_reader import BaseReader


class ParquetReader(BaseReader):
    def read(self, path: str) -> DataFrame:
        return self.spark.read.parquet(path)
