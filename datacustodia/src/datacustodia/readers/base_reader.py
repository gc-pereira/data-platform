from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class BaseReader(ABC):

    def __init__(self, context):
        self.spark = context

    @abstractmethod
    def read(self, path_or_table: str) -> DataFrame:
        pass
