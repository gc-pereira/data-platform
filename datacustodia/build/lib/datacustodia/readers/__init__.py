from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession


class BaseReader(ABC):

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def read(self, path_or_table: str) -> DataFrame:
        pass
