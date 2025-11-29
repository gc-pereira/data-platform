from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.sot.tb_aux_primeira_insercao (
    contract_id STRING,
    data_primeira_insercao TIMESTAMP
)
USING iceberg
PARTITIONED BY (contract_id)
LOCATION 's3://sor-data-custodia/tb_aux_primeira_insercao/'
TBLPROPERTIES (
    'format'='parquet',
    'write_compression'='snappy'
)        
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.sot.tb_aux_ultima_atualizacao (
    contract_id STRING,
    data_ultima_atualizacao TIMESTAMP,
    hash_atual STRING
)
USING iceberg
PARTITIONED BY (contract_id)
LOCATION 's3://sor-data-custodia/tb_aux_ultima_atualizacao/'
TBLPROPERTIES (
    'format'='parquet',
    'write_compression'='snappy'
)
""")

df = spark.table("glue_catalog.sot.tb_unindo_diferentes_fontes_iceberg")

df_hash = df.withColumn(
    "hash_value",
    F.sha2(F.concat_ws("||", *df.columns), 256)
)

df_hash.cache()

df_primeira = spark.table("glue_catalog.sot.tb_aux_primeira_insercao")

novos_contratos = (
    df_hash
    .join(df_primeira, "contract_id", "left_anti")
    .select(
        "contract_id",
        F.current_timestamp().alias("data_primeira_insercao")
    )
)

if novos_contratos.count() > 0:
    novos_contratos.writeTo("glue_catalog.sot.tb_aux_primeira_insercao").append()


df_ultima = spark.table("glue_catalog.sot.tb_aux_ultima_atualizacao")

joined = (
    df_hash.alias("new")
    .join(df_ultima.alias("old"), "contract_id", "left")
    .select(
        "new.contract_id",
        F.col("new.hash_value").alias("hash_atual"),
        F.col("old.hash_atual").alias("hash_antigo")
    )
)

atualizados = (
    joined
    .filter((F.col("hash_antigo").isNull()) | (F.col("hash_antigo") != F.col("hash_atual")))
    .select(
        "contract_id",
        F.current_timestamp().alias("data_ultima_atualizacao"),
        "hash_atual"
    )
)

if atualizados.count() > 0:
    atualizados.writeTo("glue_catalog.sot.tb_aux_ultima_atualizacao").overwritePartitions()

spark.stop()
