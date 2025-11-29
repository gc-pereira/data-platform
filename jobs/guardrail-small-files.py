import boto3

from pyspark.sql import functions as F
from pyspark.context import SparkContext

from awsglue.context import GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

### CONFIGURAÇÕES ####
S3_BUCKET = "sor-data-custodia"
TABLE_PATH = "s3://sor-data-custodia/tb_contatos_mainframe/"
THRESHOLD_SMALL_MB = 32     # small files = menores que isso
TARGET_MIN_MB = 256         # arquivo final mínimo
TARGET_MAX_MB = 1024        # arquivo final máximo
PARTITION_COL = "dt"

s3 = boto3.client("s3")


def list_s3_files(prefix):
    bucket = S3_BUCKET
    paginator = s3.get_paginator("list_objects_v2")
    paths = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]
            paths.append((key, size))

    return paths


def compact_partition(partition_value):
    print(f"\n=== Compactando partição: {partition_value} ===")

    prefix = f"minha-tabela/{PARTITION_COL}={partition_value}/"
    files = list_s3_files(prefix)

    small_files = [(k, size) for k, size in files if size < THRESHOLD_SMALL_MB * 1024 * 1024]

    if not small_files:
        print("Nenhum small file encontrado.")
        return

    print(f"{len(small_files)} small files encontrados.")

    # Arquivos em full path
    paths = [f"s3://{S3_BUCKET}/{k}" for k, _ in small_files]

    total_bytes = sum(size for _, size in small_files)
    total_mb = total_bytes / (1024 * 1024)

    print(f"Tamanho total dos small files: {round(total_mb, 2)} MB")

    # Cálculo da quantidade ideal de arquivos finais
    # Queremos arquivos entre 256MB e 1GB
    ideal_num_files = max(1, int(total_mb / TARGET_MAX_MB))
    max_num_files = max(1, int(total_mb / TARGET_MIN_MB))

    # Garantir que não gere arquivos muito pequenos
    num_final_files = max(ideal_num_files, 1)
    num_final_files = min(num_final_files, max_num_files)

    print(f"Gerando {num_final_files} arquivos finais...")

    df = spark.read.parquet(*paths)

    # Cria os arquivos no tamanho desejado
    (
        df.repartition(num_final_files)
          .write
          .mode("overwrite")
          .parquet(f"{TABLE_PATH}{PARTITION_COL}={partition_value}/")
    )

    print("Compactado com sucesso.")


if __name__ == "__main__":
    df = spark.read.parquet(TABLE_PATH)
    particoes = [row[PARTITION_COL] for row in df.select(PARTITION_COL).distinct().collect()]

    print("Partições encontradas:", particoes)


    for p in particoes:
        compact_partition(p)

    spark.stop()
