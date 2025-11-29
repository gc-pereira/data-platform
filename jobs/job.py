from datacustodia.pipeline import JobPipeline

if __name__ == "__main__":
    job_pipeline = JobPipeline(
        args=["table_name"]
    )
    job_pipeline.extract()
    job_pipeline.transform()
    job_pipeline.posdq()
    job_pipeline.write()
    job_pipeline.update_partitions()
    job_pipeline.idempotence()