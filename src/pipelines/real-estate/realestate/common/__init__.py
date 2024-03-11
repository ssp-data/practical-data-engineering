import os
from dagster_aws.s3 import S3Resource

from .resources import boto3_connection
from dagster import fs_io_manager

from dagster_deltalake_pandas import (
    DeltaLakePandasIOManager,
    DeltaLakePandasTypeHandler,
)
from dagster_deltalake import S3Config

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "miniostorage")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")  # TODO: needed? correct?

resource_def = {
    "local": {
        "s3": S3Resource(
            endpoint_url=MINIO_ENDPOINT,  # might be needed to be 127.0.0.1:60599 or similar...
        ),
        # 'druid': druid_db_info_resource,
        "boto3": boto3_connection,
        # "boto3": boto3_connection(
        #     config_schema={
        #         "aws_access_key_id":MINIO_ACCESS_KEY,
        #         "aws_secret_access_key":MINIO_SECRET_KEY,
        #         "endpoint_url":"http://127.0.0.1:9000",
        #     }
        # ),
        "fs_io_manager": fs_io_manager,
        "io_manager": DeltaLakePandasIOManager(
            root_uri="lake/bronze/",  # required
            storage_options=S3Config(
                bucket="real-estate", # Usually this is defined in the table not as general
                access_key_id=MINIO_ACCESS_KEY,
                secret_access_key=MINIO_SECRET_KEY,
                endpoint=MINIO_ENDPOINT, 
                #region=AWS_REGION,#not needed for minio
            ),  # required
            # schema="real-estate",  # optional, defaults to "public"
        ),
    },
}
