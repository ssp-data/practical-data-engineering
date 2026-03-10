import os

import dagster as dg
from dagster._core.storage.file_manager import LocalFileManager
from dagster_aws.s3 import S3Resource

from dagster_deltalake_pandas import (
    DeltaLakePandasIOManager,
)
from dagster_deltalake import S3Config

from .resources import Boto3Resource, DruidResource

S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "admin")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://127.0.0.1:8333")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

resource_def = {
    "local": {
        "s3": S3Resource(
            endpoint_url=S3_ENDPOINT,
        ),
        "boto3": Boto3Resource(
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            endpoint_url=S3_ENDPOINT,
        ),
        # "druid": DruidResource(druid_router="http://localhost:8888"),
        "file_manager": LocalFileManager(base_dir="/tmp/dagster/file_cache"),
        "fs_io_manager": dg.FilesystemIOManager(),
        "io_manager": DeltaLakePandasIOManager(
            root_uri="lake/bronze/",
            storage_options=S3Config(
                bucket="real-estate",
                access_key_id=S3_ACCESS_KEY,
                secret_access_key=S3_SECRET_KEY,
                endpoint=S3_ENDPOINT,
            ),
        ),
    },
}
