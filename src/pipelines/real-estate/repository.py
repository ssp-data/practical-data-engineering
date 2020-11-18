from dagster import repository

# from .pipelines import define_realestate_ingest_pipeline, define_realestate_warehouse_pipeline
from realestate.pipelines import define_parallel_pipeline


@repository
def realestate_repo():
    return {
        "pipelines": {
            # "realestate_ingest_pipeline": define_realestate_ingest_pipeline,
            # "realestate_warehouse_pipeline": define_realestate_warehouse_pipeline,
            "parallel_pipeline": define_parallel_pipeline,
        }
    }
