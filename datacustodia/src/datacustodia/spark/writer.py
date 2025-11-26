from pyspark.sql import DataFrame


class SparkWriter:

    def write_partitioned(self, df: DataFrame, path: str, partition_cols: list):
        (
            df.write
            .mode("overwrite")
            .partitionBy(partition_cols)
            .parquet(path)
        )
