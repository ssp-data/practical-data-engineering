# general op pyspark execution

from pandas import DataFrame

from realestate.common.types_realestate import PropertyDataFrame
from realestate.common.helper_functions import reading_delta_table

import re

import pandas as pd
import pandasql as ps
import pyarrow as pa

import dagster as dg

from realestate.common.types import DeltaCoordinate
from realestate.common.helper_functions import rename_pandas_dataframe_columns, read_gzipped_json



PARQUET_SPECIAL_CHARACTERS = r"[ ,;{}()\n\t=]"


def _get_s3a_path(bucket, path):
    return "s3a://" + bucket + "/" + path


# @dg.op(
#     required_resource_keys={"pyspark", "s3"},
#     description="""Ingest s3 path with zipped jsons
# and load it into a Spark Dataframe.
# It infers header names but and infer schema.
#
# It also ensures that the column names are valid parquet column names by
# filtering out any of the following characters from column names:
#
# Characters (within quotations): "`{chars}`"
#
# """.format(
#         chars=PARQUET_SPECIAL_CHARACTERS
#     ),
# )
# def s3_to_df(context, s3_coordinate: S3Coordinate) -> DataFrame:
#     context.log.debug(
#         "AWS_KEY: {access} - Secret: {secret})".format(
#             access=os.environ["MINIO_ROOT_USER"], secret=os.environ["MINIO_ROOT_PASSWORD"]
#         )
#     )
#     s3_path = _get_s3a_path(s3_coordinate["bucket"], s3_coordinate["key"])
#
#     context.log.info(
#         "Reading dataframe from s3 path: {path} (Bucket: {bucket} and Key: {key})".format(
#             path=s3_path, bucket=s3_coordinate["bucket"], key=s3_coordinate["key"]
#         )
#     )
#
#     # reading from a folder handles zipped and unzipped jsons automatically
#     data_frame = context.resources.pyspark.spark_session.read.json(s3_path)
#
#     context.log.info("Column FactId removed from df")
#
#     # parquet compat
#     return rename_spark_dataframe_columns(
#         data_frame, lambda x: re.sub(PARQUET_SPECIAL_CHARACTERS, "", x)
#     )


@dg.op(
    description="""This function is to flatten the nested json properties to a table with flat columns. Renames columns to avoid parquet special characters.""",
    config_schema={
        "remove_columns": dg.Field(
            [str],
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
    out=dg.Out(io_manager_key="fs_io_manager"),
)
def flatten_json(context, local_file: dg.LocalFileHandle) -> pd.DataFrame:

    # reading from a folder with zipped JSONs
    context.log.info(f"Reading from local file: {local_file.path} ...")
    json_data = read_gzipped_json(local_file.path)

    # Flatten: Normalize the JSON data
    df = pd.json_normalize(json_data)

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


# @dg.op(
#     description="""This function is to flatten the nested json properties to a table with flat columns""",
#     config_schema={
#         "remove_columns": dg.Field(
#             [str],
#             default_value=[
#                 "propertyDetails_images",
#                 "propertyDetails_pdfs",
#                 "propertyDetails_commuteTimes_defaultPois_transportations",
#                 "viewData_viewDataWeb_webView_structuredData",
#             ],
#             is_required=False,
#             description=("unessesary columns to be removed in from the json"),
#         ),
#     },
# )
# def flatten_json(context, df: DataFrame) -> DataFrame:
#     "Flatten array of structs and structs"
#
#     #    from pyspark.sql.types import *
#     #    from pyspark.sql.functions import *
#     # compute Complex Fields (Lists and Structs) in Schema
#     complex_fields = dict(
#         [
#             (field.name, field.dataType)
#             for field in df.schema.fields
#             if (type(field.dataType) == ArrayType or type(field.dataType) == StructType)
#             and field.name.startswith("propertyDetails")
#         ]
#     )
#
#     # print(complex_fields)
#     while len(complex_fields) != 0:
#         col_name = list(complex_fields.keys())[0]
#         context.log.debug(
#             "Processing :" + col_name + " Type : " + str(type(complex_fields[col_name]))
#         )
#
#         if col_name in context.op_config["remove_columns"]:
#             # remove and skip next part
#             df = df.drop(col_name)
#         else:
#             # if StructType then convert all sub element to columns.
#             # i.e. flatten structs
#             if type(complex_fields[col_name]) == StructType:
#                 expanded = [
#                     col(col_name + "." + k).alias(col_name + "_" + k)
#                     for k in [n.name for n in complex_fields[col_name]]
#                 ]
#                 df = df.select("*", *expanded).drop(col_name)
#
#             # if ArrayType then add the Array Elements as Rows using the explode function
#             # i.e. explode Arrays
#             elif type(complex_fields[col_name]) == ArrayType:
#                 df = df.withColumn(col_name, explode_outer(col_name))
#
#         # recompute remaining Complex Fields in Schema
#         complex_fields = dict(
#             [
#                 (field.name, field.dataType)
#                 for field in df.schema.fields
#                 if type(field.dataType) == ArrayType
#                 or type(field.dataType) == StructType
#             ]
#         )
#         context.log.debug(
#             "count of rows, in case of no errors, count should stay the same. Count: "
#             + str(df.count())
#         )
#
#     return df


# def sql_solid(
#     name,
#     sql_statement,
#     materialization_strategy,
#     table_name=None,
#     ins=None,
# ):
#     """Return a new solid that executes and materializes a SQL select statement."""
#     materialization_strategy_output_types = {
#         "table": SqlTableName,
#         "delta_table": DeltaCoordinate,
#     }
#
#     if materialization_strategy not in materialization_strategy_output_types:
#         raise Exception(
#             "Invalid materialization strategy {materialization_strategy}, must "
#             "be one of {materialization_strategies}".format(
#                 materialization_strategy=materialization_strategy,
#                 materialization_strategies=str(
#                     list(materialization_strategy_output_types.keys())
#                 ),
#             )
#         )
#
#     output_description = (
#         "The string name of the new table created by the solid"
#         if materialization_strategy == "table"
#         or materialization_strategy == "delta_table"
#         else "The materialized SQL statement."
#     )
#
#     description = """This solid executes the following SQL statement:
#     {sql_statement}""".format(
#         sql_statement=sql_statement
#     )
#
#     sql_statement = ("{sql_statement};").format(sql_statement=sql_statement)
#
#     @dg.op(
#         name=name,
#         ins=ins,
#         out=dg.Out(
#                 materialization_strategy_output_types[materialization_strategy],
#                 description=output_description,
#             ),
#         description=description,
#         required_resource_keys={"pyspark"},
#         tags={
#             "kind": "sql",
#             "sql": sql_statement,
#         },
#     )
#     def _sql_solid(context, **ins):
#         if ins["target_delta_table"] is None:
#             raise Exception("Input `target_delta_table` not provided.")
#         if ins["input_dataframe"] is None:
#             raise Exception("Input `input_dataframe` not provided.")
#
#         target_delta_path = _get_s3a_path(
#             ins["target_delta_table"]["s3_coordinate_bucket"],
#             ins["target_delta_table"]["s3_coordinate_key"],
#         )
#         context.log.info("Target Delta table path: {}".format(target_delta_path))
#
#         insert_columns = "\n, ".join(ins["input_dataframe"].columns)
#         update_columns = "\n, ".join(
#             ["trg." + c + " = src." + c for c in ins["input_dataframe"].columns]
#         )
#
#         sql_statement_template = Template(sql_statement)
#         repl_sql_statement = sql_statement_template.render(
#             target_delta_table="delta.`" + target_delta_path + "`",
#             update_columns=update_columns,
#             insert_columns=insert_columns,
#         )
#
#         context.log.info(
#             "Executing sql statement:\n{sql_statement}".format(
#                 sql_statement=repl_sql_statement
#             )
#         )
#
#         ins["input_dataframe"].createOrReplaceTempView("input_dataframe")
#
#         context.resources.pyspark.spark_session.sql(repl_sql_statement)
#         yield dg.AssetMaterialization(
#             asset_key=ins["target_delta_table"]["table_name"],
#             description="Target Delta table",
#             metadata={
#                 "delta_table_path": dg.MetadataValue.path(target_delta_path),
#             },
#         )
#
#         yield dg.Output(value=ins["target_delta_table"], output_name="result")
#
#     return _sql_solid

@dg.op(out=dg.Out(io_manager_key="fs_io_manager"))
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
            predicate='target.propertyDetails_propertyId = source."propertyDetails_propertyId"',
            source_alias='source',
            target_alias='target')
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    context.log.info("Merged data into Delta table `property` successfully")

    return target_delta_coordinate


# merge_property_delta = sql_solid(
#     name="merge_property_delta",
#     sql_statement="""
#     MERGE INTO {{ target_delta_table }} trg
#     USING input_dataframe AS src
#     ON trg.propertyDetails_id = src.propertyDetails_id
#     WHEN MATCHED THEN
#         UPDATE SET *
#     WHEN NOT MATCHED THEN
#         INSERT *
#     """
#     ,
#     materialization_strategy="delta_table",
#     ins = {"delta_coordinate": dg.In(dagster_type=DeltaCoordinate),
#     "df": dg.In(dagster_type=DataFrame)}
# )



@dg.op(
    required_resource_keys={"s3"},
    description="""This will check if property is already downloaded. If so, check if price or other
    columns have changed in the meantime, or if date is very old, download again""",
    out={"properties": dg.Out(dagster_type=PropertyDataFrame, is_required=False, io_manager_key="fs_io_manager")},

)
def get_changed_or_new_properties(context, properties: PropertyDataFrame, property_table: pd.DataFrame) -> PropertyDataFrame:
    # prepare ids and fingerprints from fetched properties
    ids_tmp: list = [p["id"] for p in properties]
    ids: str = ", ".join(ids_tmp)

    context.log.info("Fetched propertyDetails_id's: [{}]".format(ids))

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

    existing_props = result_df[["propertyDetails_propertyId", "fingerprint"]].values.tolist()

    pd_existing_props = pd.DataFrame(existing_props, columns=cols_props)
    pd_properties = pd.DataFrame(properties, columns=cols_PropertyDataFrame)

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
        yield dg.Output(changed_properties, "properties")



# @dg.op(
#     required_resource_keys={"pyspark", "s3"},
#     description="""Creates the delta table on S3 and returns the DeltaCoordinates""",
#     config_schema={
#         "mergeSchema": dg.Field(
#             bool,
#             default_value=True,
#             is_required=False,
#         ),
#         "mode": dg.Field(
#             str,
#             default_value="overwrite",
#             is_required=False,
#         ),
#         "partitionBy": dg.Field(
#             str,
#             default_value="DateTimeDate",
#             is_required=False,
#         ),
#     },
# )
# def create_delta_table(
#     context, data_frame: DataFrame, delta_coordinate: DeltaCoordinate
# ) -> DeltaCoordinate:
#     delta_path = _get_s3a_path(
#         delta_coordinate["s3_coordinate_bucket"], delta_coordinate["s3_coordinate_key"]
#     )
#     context.log.info(
#         "Writing dataframe to s3 delta table: "
#         + delta_coordinate["table_name"]
#         + " in path: {path} ...".format(path=delta_path)
#     )
#
#     context.resources.pyspark.spark_session.sql(
#         "CREATE DATABASE IF NOT EXISTS {}".format(delta_coordinate["database"])
#     )
#
#     context.resources.pyspark.spark_session.sql(
#         "DROP TABLE IF EXISTS {database}.{table_name}".format(
#             database=delta_coordinate["database"],
#             table_name=delta_coordinate["table_name"],
#         )
#     )
#
#     os.system("hdfs dfs -rm -r -skipTrash " + delta_path)
#
#     data_frame.write.format("delta").mode(context.op_config["mode"]).option(
#         "mergeSchema", context.op_config["mergeSchema"]
#     ).save(delta_path)
#
#     context.log.info("data_frame written to: " + delta_path)
#
#     context.resources.pyspark.spark_session.sql(
#         """
#         CREATE TABLE IF NOT EXISTS {}.{}
#         USING DELTA
#         LOCATION "{}"
#         """.format(
#             delta_coordinate["database"], delta_coordinate["table_name"], delta_path
#         )
#     )
#
#     context.log.info("delta table " + delta_coordinate["table_name"] + " created")
#
#     return delta_coordinate


# @dg.op(
#     required_resource_keys={"pyspark", "s3"},
#     description="""Loads given delta coordinates into a spark data frame""",
# )
# def load_delta_table_to_df(
#     context,
#     delta_coordinate: DeltaCoordinate,
#     where_conditions: str,
# ) -> DataFrame:
#     delta_path = _get_s3a_path(
#         delta_coordinate["s3_coordinate_bucket"], delta_coordinate["s3_coordinate_key"]
#     )
#     context.log.info("where condition: " + where_conditions)
#     if where_conditions != "None":
#         data_frame = (
#             context.resources.pyspark.spark_session.read.format("delta")
#             .load(delta_path)
#             .where(where_conditions)
#         )
#     else:
#         data_frame = context.resources.pyspark.spark_session.read.format("delta").load(
#             delta_path
#         )
#
#     return data_frame


#
# GENERAL MINOR SPARK FUNCTIONS (kept as reference)
#
# def do_prefix_column_names(df, prefix):
#     check.inst_param(df, "df", DataFrame)
#     check.str_param(prefix, "prefix")
#     return rename_spark_dataframe_columns(
#         df, lambda c: "{prefix}{c}".format(prefix=prefix, c=c)
#     )
#
#
# @dg.op
# def canonicalize_column_names(_context, data_frame: DataFrame) -> DataFrame:
#     return rename_spark_dataframe_columns(data_frame, lambda c: c.lower())
#
#
# def replace_values_spark(data_frame, old, new):
#     return data_frame.na.replace(old, new)
