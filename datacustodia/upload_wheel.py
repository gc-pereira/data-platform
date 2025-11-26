import boto3

s3 = boto3.client('s3')

s3.upload_file('dist/datacustodia-0.0.2-py3-none-any.whl', 'data-custodia', 'configuration/datacustodia-0.0.2-py3-none-any.whl')