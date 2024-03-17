# gernal op pyspark execution

from .types import S3Coordinate

from pandas import DataFrame


from realestate.common.types_realestate import PropertyDataFrame
from realestate.common.helper_functions import reading_delta_table

import re
import os

import pandas as pd
import pandasql as ps
import pyarrow as pa

from dagster import (
    LocalFileHandle,
    op,
    Field,
    String,
    Output,
    Out,
    check,
)

from realestate.common.types import DeltaCoordinate
from realestate.common.helper_functions import rename_pandas_dataframe_columns, read_gzipped_json

from dagster import Field, String


PARQUET_SPECIAL_CHARACTERS = r"[ ,;{}()\n\t=]"


def _get_s3a_path(bucket, path):
    # TODO: remove unnessesary slashs if there
    return "s3a://" + bucket + "/" + path


@op(
    required_resource_keys={"pyspark", "s3"},
    description="""Ingest s3 path with zipped jsons
and load it into a Spark Dataframe.
It infers header names but and infer schema.

It also ensures that the column names are valid parquet column names by
filtering out any of the following characters from column names:

Characters (within quotations): "`{chars}`"

""".format(
        chars=PARQUET_SPECIAL_CHARACTERS
    ),
)
def s3_to_df(context, s3_coordinate: S3Coordinate) -> DataFrame:
    context.log.debug(
        "AWS_KEY: {access} - Secret: {secret})".format(
            access=os.environ["MINIO_ROOT_USER"], secret=os.environ["MINIO_ROOT_PASSWORD"]
        )
    )
    # findspark.init(spark_home='/path/to/spark/lib/')
    s3_path = _get_s3a_path(s3_coordinate["bucket"], s3_coordinate["key"])

    context.log.info(
        "Reading dataframe from s3 path: {path} (Bucket: {bucket} and Key: {key})".format(
            path=s3_path, bucket=s3_coordinate["bucket"], key=s3_coordinate["key"]
        )
    )

    # reading from a folder handles zipped and unzipped jsons automatically
    data_frame = context.resources.pyspark.spark_session.read.json(s3_path)

    # df.columns #print columns

    context.log.info("Column FactId removed from df")

    # parquet compat
    return rename_spark_dataframe_columns(
        data_frame, lambda x: re.sub(PARQUET_SPECIAL_CHARACTERS, "", x)
    )


@op(
    description="""This function is to flatten the nested json properties to a table with flat columns. Renames columns to avoid parquet special characters.""",
    config_schema={
        "remove_columns": Field(
            [String],
            default_value=[
                "propertyDetails_images",
                "propertyDetails_pdfs",
                "propertyDetails_commuteTimes_defaultPois_transportations",
                "viewData_viewDataWeb_webView_structuredData",
            ],
            is_required=False,
            description=("unessesary columns to be removed in from the json"),
        ),
    },
    out=Out(io_manager_key="fs_io_manager"),
)
def flatten_json(context, local_file: LocalFileHandle) -> pd.DataFrame:

    # reading from a folder with zipped JSONs 
    context.log.info(f"Reading from local file: {local_file.path} ...")
    json_data = read_gzipped_json(local_file.path)

    # Flatten: Normalize the JSON data
    df = pd.json_normalize(json_data)

    #TODO: Still need to remove FactId?
    if 'FactId' in df.columns:
        df.drop('FactId', axis=1, inplace=True)
        context.log.info("Column FactId removed from df")


    # rename for avoid parquet special characters
    df = rename_pandas_dataframe_columns(
        df, lambda x: re.sub(PARQUET_SPECIAL_CHARACTERS, "", x)
    )

    df.drop(columns=context.op_config["remove_columns"], errors='ignore', inplace=True)
    # convert . column names to underlines
    df.columns = df.columns.str.replace('.', '_', regex=False)

    context.log.info(f"faltten df length length: {len(df)} and schema: {df.columns}")
    return df



@op(out=Out(io_manager_key="fs_io_manager"))
def merge_property_delta(context, input_dataframe: DataFrame) -> DeltaCoordinate:
    
    target_delta_table = "s3a://real-estate/lake/bronze/property"
    target_delta_coordinate = { "s3_coordinate_bucket": "real-estate", "s3_coordinate_key": "lake/bronze/property", "table_name": "property", "database": "core"}

    df, dt = reading_delta_table(context, target_delta_table)

    input_table_pa = pa.Table.from_pandas(input_dataframe)
    context.log.debug(f"Target Delta table schema: {dt.to_pyarrow_dataset().schema}")
    context.log.debug(f"input_dataframe: {type(input_dataframe)} and lenght {len(input_dataframe)}")
    context.log.debug(f"input_dataframe schema: {input_table_pa.schema}")

    (
        dt.merge(
            source=input_dataframe,
            # predicate='target.propertyDetails_id = source."propertyDetails_propertyId"',
            predicate='target.propertyDetails_propertyId = source."propertyDetails_propertyId"',
            source_alias='source',
            target_alias='target')
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    context.log.info("Merged data into Delta table `property` successfully")

    #return delta coordinates for notebooks to read from
    return target_delta_coordinate

@op(
    required_resource_keys={"s3"},
    description="""This will check if property is already downloaded. If so, check if price or other
    columns have changed in the meantime, or if date is very old, download again""",
    out={"properties": Out(dagster_type=PropertyDataFrame, is_required=False, io_manager_key="fs_io_manager")},

)
def get_changed_or_new_properties(context, properties: PropertyDataFrame, property_table: pd.DataFrame) -> PropertyDataFrame:
    # prepare ids and fingerprints from fetched properties
    ids_tmp: list = [p["id"] for p in properties]
    ids: str = ", ".join(ids_tmp)

    context.log.info("Fetched propertyDetails_id's: [{}]".format(ids))
    # context.log.debug(f"type: property_table: {type(property_table)} and lenght {len(property_table)}")

    cols_props = ["propertyDetails_propertyId", "fingerprint"]
    cols_PropertyDataFrame = [
        "id",
        "fingerprint",
        "is_prefix",
        "rentOrBuy",
        "city",
        "propertyType",
        "radius",
        "last_normalized_price",
    ]

    query = f"""SELECT propertyDetails_propertyId
                , CAST(propertyDetails_propertyId AS STRING)
                    || '-'
                    || propertyDetails_normalizedPrice AS fingerprint
            FROM property_table
            WHERE propertyDetails_propertyId IN ( {ids} )
            """
    result_df = ps.sqldf(query, locals())
    context.log.info(f"Lenght: property_table: {len(result_df)}")

    # get a list selected colum: `property_ids` and its fingerprint
    existing_props = result_df[["propertyDetails_propertyId", "fingerprint"]].values.tolist()

    # Convert dict into pandas dataframe
    pd_existing_props = pd.DataFrame(existing_props, columns=cols_props)
    pd_properties = pd.DataFrame(properties, columns=cols_PropertyDataFrame)

    # debugging
    # context.log.debug(f"pd_existing_props: {pd_existing_props}, type: {type(pd_existing_props)}")
    # context.log.debug(f"pd_properties: {pd_properties}")

    # select new or changed once
    df_changed = ps.sqldf(
        """
        SELECT p.id, p.fingerprint, p.is_prefix, p.rentOrBuy, p.city, p.propertyType, p.radius, p.last_normalized_price
        FROM pd_properties p LEFT OUTER JOIN pd_existing_props e
            ON p.id = e.propertyDetails_propertyId
            WHERE p.fingerprint != e.fingerprint
                OR e.fingerprint IS NULL
        """, locals()
    )
    context.log.info(f"lenght: df_changed: {len(df_changed)}")
    if df_changed.empty:
        context.log.info("No property of [{}] changed".format(ids))
    else:
        changed_properties = []
        for index, row in df_changed.iterrows():
            changed_properties.append(row.to_dict())

        ids_changed = ", ".join(str(e) for e in df_changed["id"].tolist())

        context.log.info("changed properties: {}".format(ids_changed))
        yield Output(changed_properties, "properties")




#
# GENERAL MINOR SPARK FUNCTIONS
def do_prefix_column_names(df, prefix):
    check.inst_param(df, "df", DataFrame)
    check.str_param(prefix, "prefix")
    return rename_spark_dataframe_columns(
        df, lambda c: "{prefix}{c}".format(prefix=prefix, c=c)
    )


@op
def canonicalize_column_names(_context, data_frame: DataFrame) -> DataFrame:
    return rename_spark_dataframe_columns(data_frame, lambda c: c.lower())


def replace_values_spark(data_frame, old, new):
    return data_frame.na.replace(old, new)
