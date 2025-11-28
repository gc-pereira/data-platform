from datacustodia.src.datacustodia.pipeline import JobPipeline

if __name__ == "__main__":
    table_name = "table_orch"
    job_pipeline = JobPipeline(
        table_name=table_name,
        args=["table_name"]
    )
    job_pipeline.extract()
    job_pipeline.write()
    job_pipeline.stop()