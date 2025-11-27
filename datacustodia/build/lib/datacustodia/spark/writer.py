from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

class SparkWriter:

    def write_partitioned(self, df: DataFrame, path: str, partition_cols: list):
        df = df.withColumn('last_update_date', lit(datetime.today()))
        (
            df.write
            .mode("overwrite")
            .partitionBy(partition_cols)
            .parquet(path)
        )
