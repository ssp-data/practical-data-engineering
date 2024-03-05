import os
from dagster_pyspark import pyspark_resource
from dagster_aws.s3 import S3Resource
from .resources import boto3_connection
from dagster import fs_io_manager


# TODO: change to what we have in yaml
configured_pyspark = pyspark_resource.configured(
    {
        "spark_conf": {
            "spark.jars.packages": ",".join(
                [
                    "net.snowflake:snowflake-jdbc:3.8.0",
                    "net.snowflake:spark-snowflake_2.12:2.8.2-spark_3.0",
                    "com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7",
                ]
            ),
            "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3native.NativeS3FileSystem",
            "spark.hadoop.fs.s3.awsAccessKeyId": os.getenv("AWS_ACCESS_KEY_ID", ""),
            "spark.hadoop.fs.s3.awsSecretAccessKey": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            "spark.hadoop.fs.s3.buffer.dir": "/tmp",
        }
    }
)




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

