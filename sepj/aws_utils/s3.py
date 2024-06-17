import boto3


class S3Manager:
    def __init__(self, pipe_stage) -> None:
        self.pipe_stage = pipe_stage
    
    def get_s3_file(self, pipe_stage):
        ...
        
    def save_s3_file(self, pipe_stage):
        ...