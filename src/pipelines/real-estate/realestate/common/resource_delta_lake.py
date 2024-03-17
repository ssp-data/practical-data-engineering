import pandas as pd
from deltalake import DeltaTable, write_deltalake
from delta_spark import merge, TableMerger

from .types import S3Coordinate


# TODO: checkout Delta Dagster IO manager: https://github.com/delta-io/delta-rs/commit/fe36b136161fdf019b821e154d83f457795e8579
# https://docs.dagster.io/integrations/deltalake/using-deltalake-with-dagster



class delta_lake_resource(ConfigurableResource):
    minio_access_key: str
    minio_secret_key: str
    minio_endpoint: str
    aws_region: str

    def __init__(self):
        self.storage_options = {
            "AWS_ACCESS_KEY_ID": self.minio_access_key,
            "AWS_SECRET_ACCESS_KEY": self.minio_secret_key,
            "AWS_ENDPOINT_URL": self.minio_endpoint,
            "AWS_ALLOW_HTTP": "true",
            "AWS_REGION": self.aws_region,  #'us-east-1',
            "AWS_S3_ALLOW_UNSAFE_RENAME" : "true",
        }

    def create_table(self, bucket_name: str, folder_path: str) -> DeltaTable:
        table_url = f"s3a://{self.minio_access_key}:{self.minio_secret_key}@{self.minio_endpoint}/{bucket_name}/{folder_path}"
        dt = DeltaTable(table_url, storage_options=self.storage_options) #.to_pandas()
        return dt


    #def merge_table(self, target_dt: DeltaTable, source : pd.DataFrame, join_condition : str) -> TableMerger:
    def merge_table(self, target_dt : S3Coodinate, source : pd.DataFrame, join_condition : str) -> TableMerger:
        "delta-rs alternative to merge delta table"
        # data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
        # write_deltalake("tmp", data)
        # dt = DeltaTable("tmp")
        # new_data = pa.table({"x": [2, 3], "deleted": [False, True]})

        (
            target_dt.merge(
                source=source, # pyarrow.Table, pyarrow.RecordBatch, pyarrow.RecordBatchReader, ds.Dataset, pd.DataFrame]
                predicate=join_condition, #'target.x = source.x', 
                source_alias='source',
                target_alias='target')
            # .when_matched_delete(
            #     predicate="source.deleted = true")
            .execute()
        )


