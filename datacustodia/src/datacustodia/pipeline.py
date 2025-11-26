from datacustodia.spark.spark_manager import SparkManager
from datacustodia.spark.schema_cast import SchemaCaster
from datacustodia.spark.view_manager import ViewManager
from datacustodia.spark.writer import SparkWriter
from datacustodia.s3.s3_loader import S3Loader
from datacustodia.service.reader import Reader
from datacustodia.models.orchestration import ConfigModel
from datacustodia.models.enum import BUCKET_NAME, PREFIX_ATIFACTS, DE_PARA_BUCKET_NAME


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
            path = '{}.{}'.format(dependencie['DatabaseName'], dependencie['TableName'])
            self.dependencie_df = Reader(spark=self.spark, glue_context=self.glue_context, config=self.config, path=path).read()
            if self.config['DataLayer'] in ['SOR', 'SOT', 'SPEC']:
                self.view_manager.create_temp_view(self.dependencie_df, dependencie['TableName'])
           
            
    def transform(self):
        query_key = f"{PREFIX_ATIFACTS}{self.table_name}/sparksql.sql"
        query = self.s3_loader.load_text(BUCKET_NAME, query_key)
        if self.config['DataLayer'] in ['SOR', 'SOT', 'SPEC']:
            result = self.spark.sql(query)
            result.printSchema()
            self.result = self.schema_caster.cast(df=result, db=self.config['DatabaseName'], table=self.config['TableName'])
            return self.result
        else:
            return self.dependencie_df
    
    
    def write(self):
        if self.config['DataLayer'] in ['SOR', 'SOT', 'SPEC']:
            output_path = f"s3://{DE_PARA_BUCKET_NAME[self.config['DataLayer']]}/{self.config['TableName']}/"
            partition_cols = self.schema_caster.get_partition_keys(database=self.config['DatabaseName'], table=self.config['TableName'])
            result = self.transform()
            self.writer.write_partitioned(df=result, 
                                          path=output_path, 
                                          partition_cols=partition_cols)
