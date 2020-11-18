from dagster import (
    pipeline,
    solid,
    repository,
    file_relative_path,
    ModeDefinition,
    PresetDefinition,
    composite_solid,
    local_file_manager,
    execute_pipeline,
    InputDefinition,
    OutputDefinition,
    Output,
    FileHandle,
    Bool,
    Optional,
)

from realestate.common.solids_druid import ingest_druid
from realestate.common.solids_scraping import list_props_immo24, cache_properies_from_rest_api
from realestate.common.resources import boto3_connection, druid_db_info_resource

from dagster_aws.s3.solids import S3Coordinate
from realestate.common.types import DeltaCoordinate
from realestate.common.types_realestate import PropertyDataFrame
from realestate.common.solids_filehandle import json_to_gzip
from realestate.common.solids_spark_delta import (
    upload_to_s3,
    get_changed_or_new_properties,
    merge_property_delta,
    flatten_json,
    s3_to_df,
)

from dagster_aws.s3.resources import s3_resource

# from dagster_aws.s3 import s3_plus_default_intermediate_storage_defs
from dagster_pyspark import pyspark_resource

from dagster.core.storage.file_cache import fs_file_cache

# from dagster.core.storage.temp_file_manager import tempfile_resource
from dagster_aws.s3.system_storage import s3_plus_default_intermediate_storage_defs

local_mode = ModeDefinition(
    name="local",
    resource_defs={
        'pyspark': pyspark_resource,
        's3': s3_resource,
        # 'druid': druid_db_info_resource,
        'boto3': boto3_connection,
        # 'tempfile': tempfile_resource,
        "file_cache": fs_file_cache,
        # "db_info": postgres_db_info_resource,
    },
    intermediate_storage_defs=s3_plus_default_intermediate_storage_defs,
)


# prod_mode = ModeDefinition(
#     name="prod",
#     resource_defs={
#         "pyspark_step_launcher": emr_pyspark_step_launcher,
#         "pyspark": pyspark_resource,
#         "s3": s3_resource,
#         "db_info": redshift_db_info_resource,
#         "tempfile": tempfile_resource,
#         "file_cache": s3_file_cache,
#         "file_manager": s3_file_manager,
#     },
#     intermediate_storage_defs=s3_plus_default_intermediate_storage_defs,
# )


@composite_solid(
    description='Downloads full dataset (JSON) from ImmoScout24, cache it, zip it and and upload it to S3',
    # input_defs=[InputDefinition(name='properties', dagster_type=PropertyDataFrame)]
    output_defs=[
        OutputDefinition(name="properties", dagster_type=PropertyDataFrame, is_required=False),
    ],
)
def list_changed_properties():

    return get_changed_or_new_properties(list_props_immo24())


@composite_solid(
    description="This will flatten the JSON file from S3 and Merge it to the Delta Table",
    input_defs=[InputDefinition(name="properties", dagster_type=PropertyDataFrame)],
    output_defs=[
        OutputDefinition(name="delta_coordinta", dagster_type=DeltaCoordinate, is_required=False)
    ],
)
def merge_staging_to_delta_table(properties):

    prop_s3_coordinate = upload_to_s3(cache_properies_from_rest_api(properties))
    # return assets for property
    return merge_property_delta(input_dataframe=flatten_json(s3_to_df(prop_s3_coordinate)))


@pipeline(
    mode_defs=[local_mode],
    preset_defs=[
        PresetDefinition.from_files(
            name='local',
            mode='local',
            config_files=[
                file_relative_path(__file__, 'config_environments/local_base.yaml'),
                file_relative_path(__file__, 'config_pipelines/scrape_realestate.yaml'),
            ],
        ),
    ],
)
def scrape_realestate():

    properties = list_changed_properties.alias('list_SO_buy_flat')()
    merge_staging_to_delta_table.alias('merge_SO_buy')(properties)

    properties = list_changed_properties.alias('list_BE_buy_flat')()
    merge_staging_to_delta_table.alias('merge_BE_buy')(properties)
