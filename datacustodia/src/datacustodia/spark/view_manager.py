from pyspark.sql import DataFrame


class ViewManager:

    def __init__(self, spark):
        self.spark = spark

    def create_temp_view(self, df: DataFrame, name: str):
        df.createOrReplaceTempView(name)
