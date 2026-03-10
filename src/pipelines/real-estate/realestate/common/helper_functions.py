'''Compress and decompress a JSON object'''
import os
import zlib
import gzip
import json
from typing import Tuple

from deltalake import DeltaTable, _internal


import pandas as pd
import pyarrow as pa


def json_zip_writer(j, file_obj):
    gzipped_data = gzip.compress(json.dumps(j).encode('utf-8'))
    file_obj.write(gzipped_data)


def json_zip(j):
    return zlib.compress(json.dumps(j).encode('utf-8'))


def json_unzip(j, insist=True):
    try:
        j = zlib.decompress(j)
    except:
        raise RuntimeError("Could not decode/unzip the contents")

    try:
        j = json.loads(j)
    except:
        raise RuntimeError("Could interpret the unzipped contents")

    return j


def rename_pandas_dataframe_columns(data_frame, fn):
    new_columns = [fn(c) for c in data_frame.columns]
    data_frame.columns = new_columns
    return data_frame


def read_gzipped_json(path):
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        return json.load(f)


def reading_delta_table(context, s3_path_property) -> Tuple[pd.DataFrame, DeltaTable]:
    MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
    MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY", "admin")
    MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://127.0.0.1:8333")

    storage_options = {
        "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
        "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    try:
        dt = DeltaTable(s3_path_property, storage_options=storage_options)

        df = dt.to_pyarrow_dataset().to_table().to_pandas()
        remove_columns = [
            "propertyDetails_images",
            "propertyDetails_pdfs",
            "propertyDetails_commuteTimes_defaultPois_transportations",
            "viewData_viewDataWeb_webView_structuredData",
        ]
        context.log.debug(f"Removing columns: {remove_columns}")
        df = df.drop(columns=remove_columns, errors="ignore")
    except _internal.TableNotFoundError:
        df = pd.DataFrame(columns=['propertyDetails_propertyId', 'propertyDetails_normalizedPrice'])
        dt = DeltaTable.create(
            table_uri=s3_path_property,
            schema=pa.schema(
                [pa.field("propertyDetails_propertyId", pa.string()), pa.field("propertyDetails_normalizedPrice", pa.int64())]
            ),
            mode="error",
            storage_options=storage_options,
        )

    context.log.info(f"df type: {type(df)}")
    context.log.info(f"df columns: {df.columns}")
    return df, dt
