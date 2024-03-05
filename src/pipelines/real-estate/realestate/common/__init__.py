from dagster_pyspark import pyspark_resource
from dagster_aws.s3 import S3Resource
from .resources import boto3_connection
from dagster import fs_io_manager

resource_def = {
    "local": {
        "pyspark": pyspark_resource,
        "s3": S3Resource,
        # 'druid': druid_db_info_resource,
        "boto3": boto3_connection,
        # "file_cache": fs_file_cache,
        # "db_info": postgres_db_info_resource,
        # "io_manager": default
        "io_manager": fs_io_manager,
    },
}

