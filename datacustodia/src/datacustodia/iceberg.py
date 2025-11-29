from datetime import datetime, timedelta       
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

class IcebergQuery():

    def __init__(
        self,
        catalog_name,
        catalog_database,
        catalog_table,
        spark,
        older_than=None,
    ):
        self.catalog_name = catalog_name
        self.catalog_database = catalog_database
        self.catalog_table = catalog_table

        self.target_size = str(256 * 1024 * 1024)  # 256 MB
        self.max_file_group_size = str(10 * 1024 * 1024 * 1024)  # 10 GB
        self.older_than = older_than
        
        self.spark = spark

        """
        O parâmetro older_than = 'TIMESTAMP '2025-10-27 00:00:00.000''
        No comando expire_snapshots irá excluir snapshots criados antes
        do início do dia 27, ou seja, até 26/10/2025 23:59:59.999...
        """
        self.spark.sql("""
            MERGE INTO glue_catalog.sot.tb_unindo_diferentes_fontes_iceberg AS tgt
            USING novos AS src
            ON tgt.contract_id = src.contract_id
            WHEN NOT MATCHED THEN
            INSERT *
        """)

        # Stored procedure do Iceberg para expiração de Snapshots
        # Remove snapshots antigos e libera arquivos não referenciados
        # retain_last => 10 — Garante manter pelo menos os dez snapshots mais recentes
        self.EXPIRE_SNAPSHOTS = f"""
            CALL {self.catalog_name}.system.expire_snapshots(
                table => '{self.catalog_database}.{self.catalog_table}',
                older_than => TIMESTAMP '{self.older_than}',
                retain_last => 10,
                max_concurrent_deletes => 16,
                stream_results => true
            )
        """

        # Stored procedure do Iceberg para exclusão de arquivos órfãos
        # Exclui arquivos que não estão mais referenciados por nenhum snapshot
        self.REMOVE_ORPHAN = f"""
            CALL {self.catalog_name}.system.remove_orphan_files(
                table => '{self.catalog_database}.{self.catalog_table}',
                older_than => TIMESTAMP '{self.older_than}',
                max_concurrent_deletes => 8,
                dry_run => false
            )
        """

        # Stored procedure do Iceberg para reescrita de arquivos de manifesto
        # Reorganiza os arquivos de manifesto para melhorar a performance de leitura e escrita
        self.REWRITE_MANIFESTS = f"""
            CALL {self.catalog_name}.system.rewrite_manifests(
                table => '{self.catalog_database}.{self.catalog_table}'
            )
        """

        # Stored procedure do Iceberg para compactação de arquivos
        self.REWRITE_DATA = f"""
            CALL {self.catalog_name}.system.rewrite_data_files(
                table => '{self.catalog_database}.{self.catalog_table}',
                options => map(
                    'target-file-size-bytes', {self.target_size},
                    'max-file-group-size-bytes', {self.max_file_group_size}
                )
            )
        """

    

## @params: [JOB_NAME]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

tb_contratos_mobile = glueContext.create_dynamic_frame.from_catalog(
    database='sor',
    table_name='tb_contratos_mobile',
    transformation_ctx=f"tb_contratos_mobile"
).toDF()
tb_contratos_mainframe = glueContext.create_dynamic_frame.from_catalog(
    database='sor',
    table_name='tb_contratos_mainframe',
    transformation_ctx=f"tb_contratos_mainframe"
).toDF()
tb_dados_externos = glueContext.create_dynamic_frame.from_catalog(
    database='sor',
    table_name='tb_dados_externos',
    transformation_ctx=f"tb_dados_externos"
).toDF()

tb_contratos_mobile.createOrReplaceTempView('tb_contratos_mobile')
tb_contratos_mainframe.createOrReplaceTempView('tb_contratos_mainframe')
tb_dados_externos.createOrReplaceTempView('tb_dados_externos')


df = spark.sql('''
SELECT 
    CAST(CONTRACT_ID AS STRING) AS CONTRACT_ID,
    CAST(ID_CLIENTE AS STRING) AS CUSTOMER_ID,
    SUM(VALOR_TOTAL) AS VALOR,
    'MAINFRAME' AS FONTE,
    DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS DATA_PROCESSAMENTO
FROM TB_CONTRATOS_MAINFRAME
GROUP BY 
    CONTRACT_ID, ID_CLIENTE

UNION

SELECT 
    CAST(CONTRACT_ID AS STRING) AS CONTRACT_ID,
    CAST(CUSTOMER_ID AS STRING) AS CUSTOMER_ID,
    SUM(CAST(VALOR_TOTAL AS DECIMAL(38,10))) AS VALOR,
    'MOBILE' AS FONTE,
    DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS DATA_PROCESSAMENTO
FROM TB_CONTRATOS_MOBILE
GROUP BY 
    CONTRACT_ID, CUSTOMER_ID

UNION

SELECT 
    CAST(CONTRACT_ID AS STRING) AS CONTRACT_ID,
    CAST(CUSTOMER_ID AS STRING) AS CUSTOMER_ID,
    SUM(VALOR_TOTAL) AS VALOR,
    'EXTERNOS' AS FONTE,
    DATE_FORMAT(LAST_UPDATE, 'yyyyMMdd') AS DATA_PROCESSAMENTO
FROM TB_DADOS_EXTERNOS
GROUP BY 
    CONTRACT_ID, CUSTOMER_ID, DATE_FORMAT(LAST_UPDATE, 'yyyyMMdd') 
''')

df.createOrReplaceTempView('novos')

iceberg = IcebergQuery(
    catalog_database='sot',
    catalog_name='glue_catalog',
    spark=spark,
    catalog_table='tb_unindo_diferentes_fontes_iceberg'
)