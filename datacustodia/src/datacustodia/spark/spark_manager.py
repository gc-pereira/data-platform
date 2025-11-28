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
        # Usa o codec ZSTD no Parquet (melhor compressão que Snappy, reduz tamanho e I/O mantendo boa velocidade)
        self.spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
        # Aumenta o paralelismo de shuffle (mais tasks → melhor distribuição de carga para grandes volumes)
        self.spark.conf.set("spark.sql.shuffle.partitions", 3000)
        # Ativa o Adaptive Query Execution (AQE) — Spark ajusta dinamicamente plano e número de partições
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        # Permite o AQE reduzir automaticamente partições pequenas após o shuffle (evita milhões de arquivos pequenos)
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Faz o Spark tratar arquivos pequenos como extremamente “caros” → estimula fusão em menos arquivos (menos small files)
        self.spark.conf.set("spark.sql.files.openCostInBytes", 1099511627776)  # 1 TB
        # Habilita o modo de upload rápido do S3A (multi-part upload paralelo → escrita muito mais rápida no S3)
        self.spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
        # Usa o committer "directory" — evita renomes no S3, melhora performance e reduz inconsistências na escrita
        self.spark.conf.set("spark.hadoop.fs.s3a.committer.name", "directory")
        # Aumenta o tamanho dos blocos de multi-part upload (128MB) → menos partes → upload mais eficiente
        self.spark.conf.set("spark.hadoop.fs.s3a.multipart.size", 128 * 1024 * 1024)
        # Habilita dictionary encoding no Parquet (melhora compressão e desempenho de leitura)
        self.spark.conf.set("parquet.enable.dictionary", "true")
        # Limita quantidade de linhas por arquivo para evitar arquivos gigantes ou muito fragmentados
        self.spark.conf.set("spark.sql.files.maxRecordsPerFile", 5000000)
        return self.spark, self.glueContext, self.job, self.args

    def sql(self, query: str) -> DataFrame:
        return self.spark.sql(query)

    def stop(self):
        self.spark.stop()
