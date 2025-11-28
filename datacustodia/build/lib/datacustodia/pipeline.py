import awswrangler as wr
from datetime import datetime
from pyspark.sql import functions as F

from datacustodia.s3.s3_loader import S3Loader
from datacustodia.service.reader import Reader
from datacustodia.service.idempotence import Idempotence
from datacustodia.spark.writer import SparkWriter
from datacustodia.spark.schema_cast import SchemaCaster
from datacustodia.spark.view_manager import ViewManager
from datacustodia.quality.evaluate import QualityChecks
from datacustodia.spark.spark_manager import SparkManager
from datacustodia.models.orchestration import ConfigModel
from datacustodia.models.enum import BUCKET_NAME, PREFIX_ATIFACTS, DE_PARA_BUCKET_NAME, CSV_LANDING_BUCKET


class JobPipeline:

    def __init__(self, args: list):
        spark_manager = SparkManager(args=args)
        self.spark, self.glue_context, self.job, self.args = spark_manager.get_spark()
        self.s3_loader = S3Loader()
        self.schema_caster = SchemaCaster()
        self.view_manager = ViewManager(self.spark)
        self.writer = SparkWriter()
        self.table_name = self.args['table_name']


    def load_config(self, bucket: str, key: str) -> ConfigModel:
        cfg_dict = self.s3_loader.load_json(bucket, key)
        try:
            ConfigModel(**cfg_dict)
        except Exception as ex:
            raise ex
        else:
            return cfg_dict
        
        
    def extract(self):
        key = f"{PREFIX_ATIFACTS}{self.table_name}/orchestration.json"
        self.config = self.load_config(bucket=BUCKET_NAME, key=key)
        for dependencie in self.config.get('Dependencies', []):
            if self.config['DataLayer'] in ['SOR', 'SOT', 'SPEC']:
                path = '{}.{}'.format(dependencie['DatabaseName'], dependencie['TableName'])
                self.dependencie_df = Reader(spark=self.spark, glue_context=self.glue_context, config=self.config, path=path).read()
                self.view_manager.create_temp_view(self.dependencie_df, dependencie['TableName'])
                self.predq(self.dependencie_df, dependencie['TableName'], dependencie['Quality']['Rules'])
                print('Calculando regras de qualidade para tabelas origens')
            elif self.config['DataLayer'] in ['CSV']:
                filename = dependencie['TableName'].replace('%Y%m%d', datetime.today().strftime('%Y%m%d'))
                path = f's3://{CSV_LANDING_BUCKET}/{self.config["TableName"]}/{filename}'
                self.dependencie_df = Reader(spark=self.spark, glue_context=self.glue_context, config=self.config, path=path).read()
                self.predq(self.dependencie_df, dependencie['TableName'], dependencie['Quality']['Rules'])
            else:
                raise Exception('DataLayer não mapeado na extração de dependências')
            
            if self.read_if_quality_passed == True or (self.read_if_quality_passed == True and dependencie['Quality']['ReadIfFail'] == False):
                print(f'Tabela de origem {dependencie["TableName"]} passou nas regras de qualidade.')
            elif self.read_if_quality_passed == False:
                print(f'Tabela de origem {dependencie["TableName"]} não passou nas regras de qualidade.')
                raise Exception('Tabela de origem não passou nas regras de qualidade.')
            else:
                raise Exception('Regra de qualidade não definida para leitura.')
            
            
    def transform(self):
        query_key = f"{PREFIX_ATIFACTS}{self.table_name}/sparksql.sql"
        if self.config['DataLayer'] in ['SOR', 'SOT', 'SPEC']:
            query = self.s3_loader.load_text(BUCKET_NAME, query_key)
            result = self.spark.sql(query)
            result.printSchema()
            self.result = self.schema_caster.cast(df=result, db=self.config['DatabaseName'], table=self.config['TableName'])
        else:
            self.result = self.dependencie_df
    
    
    def write(self):
        if (self.write_if_quality_passed) or (self.write_if_quality_passed and self.config['Quality']['WriteIfFail'] == False):
            output_path = f"s3://{DE_PARA_BUCKET_NAME[self.config['DataLayer']]}/{self.config['TableName']}/"
            self.partition_cols = self.schema_caster.get_partition_keys(database=self.config['DatabaseName'], table=self.config['TableName'])
            self.writer.write_partitioned(df=self.result, 
                                        path=output_path, 
                                        partition_cols=self.partition_cols)
            self.write_success = True
        elif (not self.write_if_quality_passed and self.config['Quality']['WriteIfFail'] == False):
            self.write_success = False
            print(f"Data quality checks failed. Aborting write for table {self.config['TableName']}.")
        else:
            self.write_success = False
            print('Regra de qualidade não definida para escrita.')
            
    
    def idempotence(self):
        if self.write_success:
            idempotence = Idempotence(
                df=self.result,
                table_name=self.table_name,
                partition_columns=self.partition_cols
            )
            idempotence.get()
                
                
    def __dq(self, ruleset, dqdf, dq_phase, table_name):
        quality, success = QualityChecks(df=dqdf,
                rules=ruleset,
                glue_context=self.glue_context,
                spark_session=self.spark).evaluate()
        
        quality = quality.withColumn('table_name', F.lit(table_name))
        quality = quality.withColumn('anomesdia', F.lit(datetime.today().strftime('%Y%m%d')))
        
        self.writer.write_partitioned(df=quality,
                                      path=f"s3://{BUCKET_NAME}/{dq_phase}/",
                                      partition_cols=['table_name', 'anomesdia'])
        
        return success
            
            
    def posdq(self):
        rules = ",\n    ".join(self.config['Quality']['Rules'])
        ruleset = "Rules = [\n    " + rules + "\n]"
        print('Regras de qualidade de dados que serão aplicadas: \n' + ruleset)
        self.write_if_quality_passed = self.__dq(ruleset, self.result, 'quality', self.config['TableName'])
        
        
    def predq(self, pre_df, table_name, rules_list):
        rules = ",\n    ".join(rules_list)
        ruleset = "Rules = [\n    " + rules + "\n]"
        print('Regras de qualidade de dados que serão aplicadas: \n' + ruleset)
        self.read_if_quality_passed = self.__dq(ruleset, pre_df, 'predq', table_name)


    def update_partitions(self):
        wr.athena.repair_table(table=self.config['TableName'], database=self.config['DatabaseName'])
        wr.athena.repair_table(table='predq', database='sor')
        wr.athena.repair_table(table='quality', database='sor')