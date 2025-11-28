import re
import boto3
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    BooleanType,
    FloatType,
    DoubleType,
    TimestampType,
    DateType,
    DecimalType,
    StructType,
    StructField
)

class SchemaCaster:
    """
    Lê schema do Glue Catalog e aplica CAST no DataFrame
    respeitando todos os tipos do Glue, incluindo DECIMAL(precision, scale).
    """

    # Tipos simples: sem parâmetros
    SIMPLE_GLUE_TO_SPARK = {
        "string": StringType(),
        "varchar": StringType(),
        "char": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "bigint": LongType(),
        "boolean": BooleanType(),
        "float": FloatType(),
        "double": DoubleType(),
        "timestamp": TimestampType(),
        "date": DateType()
    }

    DECIMAL_REGEX = r"decimal\((\d+),\s?(\d+)\)"

    def __init__(self, glue_client=None):
        self.glue = glue_client or boto3.client("glue")

    def get_glue_schema(self, database: str, table: str):
        """Obtém o schema do Glue Catalog."""
        response = self.glue.get_table(DatabaseName=database, Name=table)
        return response["Table"]["StorageDescriptor"]["Columns"] + response["Table"].get("PartitionKeys", [])
    
    def get_partition_keys(self, database: str, table: str):
        """Obtém as chaves de partição do Glue Catalog."""
        response = self.glue.get_table(DatabaseName=database, Name=table)
        return [col["Name"] for col in response["Table"].get("PartitionKeys", [])]

    def map_glue_type_to_spark(self, glue_type: str):
        """
        Converte um tipo Glue para Spark.
        Suporta decimal(precision, scale).
        """
        glue_type = glue_type.lower()

        # --- Caso 1: DECIMAL(X,Y) ---
        decimal_match = re.match(self.DECIMAL_REGEX, glue_type)
        if decimal_match:
            precision = int(decimal_match.group(1))
            scale = int(decimal_match.group(2))
            return DecimalType(precision, scale)

        # --- Caso 2: Tipos simples ---
        base_type = glue_type.split("(")[0]
        return self.SIMPLE_GLUE_TO_SPARK.get(base_type, StringType())


    def cast_dataframe(self, df: DataFrame, glue_schema: list) -> DataFrame:
        """Aplica CAST no DataFrame baseado no schema do Glue."""
        for col in glue_schema:
            col_name = col["Name"]
            glue_type = col["Type"]
            target_type = self.map_glue_type_to_spark(glue_type)

            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(target_type))

        return df
    
    
    def cast(self, df, db, table):
        glue_schema = self.get_glue_schema(db, table)
        return self.cast_dataframe(df, glue_schema)
    
    
    def get_spark_schema(self, database, table):
        glue_schema = self.get_glue_schema(database, table)
        struct_fields = []
        for col in glue_schema:
            col_name = col["Name"]
            col_type = col["Type"]
            spark_type = self.map_glue_type_to_spark(col_type)
            struct_fields.append(StructField(col_name, spark_type, True))
        return StructType(struct_fields)
        
