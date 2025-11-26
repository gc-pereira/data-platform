import sys

from pyspark.sql import DataFrame
from pyspark.context import SparkContext

from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions


class SparkManager:

    def __init__(self, args: list):
        self.args = getResolvedOptions(sys.argv, args)
        sc = SparkContext()
        self.glueContext = GlueContext(sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        
    def get_spark(self) -> tuple:
        return self.spark, self.glueContext, self.job, self.args

    def sql(self, query: str) -> DataFrame:
        return self.spark.sql(query)

    def stop(self):
        self.spark.stop()
