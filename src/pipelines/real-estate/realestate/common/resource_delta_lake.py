import dagster as dg
import pandas as pd
from deltalake import DeltaTable, write_deltalake

from .types import S3Coordinate


# TODO: checkout Delta Dagster IO manager: https://github.com/delta-io/delta-rs/commit/fe36b136161fdf019b821e154d83f457795e8579
# https://docs.dagster.io/integrations/deltalake/using-deltalake-with-dagster


class DeltaLakeResource(dg.ConfigurableResource):
    minio_access_key: str
    minio_secret_key: str
    minio_endpoint: str
    aws_region: str = "us-east-1"

    @property
    def storage_options(self):
        return {
            "AWS_ACCESS_KEY_ID": self.minio_access_key,
            "AWS_SECRET_ACCESS_KEY": self.minio_secret_key,
            "AWS_ENDPOINT_URL": self.minio_endpoint,
            "AWS_ALLOW_HTTP": "true",
            "AWS_REGION": self.aws_region,
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

    def create_table(self, bucket_name: str, folder_path: str) -> DeltaTable:
        table_url = f"s3a://{bucket_name}/{folder_path}"
        dt = DeltaTable(table_url, storage_options=self.storage_options)
        return dt

    def merge_table(self, target_dt: DeltaTable, source: pd.DataFrame, join_condition: str):
        """delta-rs alternative to merge delta table"""
        (
            target_dt.merge(
                source=source,
                predicate=join_condition,
                source_alias='source',
                target_alias='target')
            .execute()
        )
