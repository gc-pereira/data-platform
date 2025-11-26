import boto3
import json


class S3Loader:

    def __init__(self):
        self.s3 = boto3.client("s3")

    def load_text(self, bucket: str, key: str) -> str:
        obj = self.s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")

    def load_json(self, bucket: str, key: str) -> dict:
        return json.loads(self.load_text(bucket, key))

