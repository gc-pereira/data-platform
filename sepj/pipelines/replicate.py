import io
import boto3
import pandas as pd

class Replication:
    
    _ingestion_type = 'replication'
    
    def __init__(self) -> None:
        pass

    def run_athena_query_and_save_to_s3(self, query, s3_output, format='csv'):
        """
        Executa uma consulta no Amazon Athena e salva o resultado em um bucket S3 em CSV ou Parquet.

        :param query: A consulta SQL a ser executada no Athena.
        :param s3_output: O caminho S3 onde o resultado será salvo.
        :param format: O formato de saída, pode ser 'csv' ou 'parquet'.
        """
        client = boto3.client('athena')
        
        # Executar a consulta no Athena
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': 'default'},  # Altere para o seu banco de dados
            ResultConfiguration={'OutputLocation': s3_output}
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # Esperar pela conclusão da consulta
        while True:
            response = client.get_query_execution(QueryExecutionId=query_execution_id)
            state = response['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
        
        if state != 'SUCCEEDED':
            raise Exception(f"Query failed or was cancelled: {response['QueryExecution']['Status']['StateChangeReason']}")
        
        # Obter o resultado da consulta
        result_output = response['QueryExecution']['ResultConfiguration']['OutputLocation']
        
        # Carregar o resultado da consulta
        s3 = boto3.client('s3')
        bucket_name, key = result_output.replace("s3://", "").split("/", 1)
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        
        # Salvar o resultado no S3 no formato especificado
        bucket_name, key_prefix = s3_output.replace("s3://", "").split("/", 1)
        if format == 'csv':
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            s3.put_object(Bucket=bucket_name, Key=f'{key_prefix}/result.csv', Body=csv_buffer.getvalue())
            print(f"Resultado salvo em s3://{bucket_name}/{key_prefix}/result.csv")
        elif format == 'parquet':
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            s3.put_object(Bucket=bucket_name, Key=f'{key_prefix}/result.parquet', Body=parquet_buffer.getvalue())
            print(f"Resultado salvo em s3://{bucket_name}/{key_prefix}/result.parquet")
        else:
            raise ValueError("Formato não suportado. Use 'csv' ou 'parquet'.")
