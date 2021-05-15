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
    List,
    Nothing,
)
from dagster.experimental import DynamicOutput, DynamicOutputDefinition

from realestate.common.solids_druid import ingest_druid
from realestate.common.solids_scraping import list_props_immo24, cache_properies_from_rest_api
from realestate.common.resources import boto3_connection, druid_db_info_resource

from dagster_aws.s3.solids import S3Coordinate
from realestate.common.types import DeltaCoordinate
from realestate.common.types_realestate import PropertyDataFrame, SearchCoordinate
from realestate.common.solids_filehandle import json_to_gzip
from realestate.common.solids_spark_delta import (
    upload_to_s3,
    get_changed_or_new_properties,
    merge_property_delta,
    flatten_json,
    s3_to_df,
)
from realestate.common.solids_jupyter import data_exploration
from itertools import chain

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
    input_defs=[InputDefinition(name="search_criteria", dagster_type=SearchCoordinate)],
    output_defs=[
        OutputDefinition(name="properties", dagster_type=PropertyDataFrame, is_required=False),
    ],
)
def list_changed_properties(search_criteria):

    return get_changed_or_new_properties(list_props_immo24(searchCriteria=search_criteria))


@composite_solid(
    description="""This will take the list of properties, downloads the full dataset (JSON) from ImmoScout24,
    cache it locally to avoid scraping again in case of error. The cache will be zipped and uploaded to S3.
    From there the JSON will be flatten and merged (with schemaEvloution=True) into the Delta Table""",
    input_defs=[InputDefinition(name="properties", dagster_type=PropertyDataFrame)],
    output_defs=[
        OutputDefinition(name="delta_coordinate", dagster_type=DeltaCoordinate, is_required=False)
    ],
)
def merge_staging_to_delta_table_composite(properties):

    prop_s3_coordinate = upload_to_s3(cache_properies_from_rest_api(properties))
    # return assets for property
    return merge_property_delta(input_dataframe=flatten_json(s3_to_df(prop_s3_coordinate)))


@solid(
    description="""Collect a List of `PropertyDataFrame` from different cities to a single `PropertyDataFrame` List""",
    input_defs=[InputDefinition(name="properties", dagster_type=List(PropertyDataFrame))],
    output_defs=[OutputDefinition(name="properties", dagster_type=PropertyDataFrame)],
)
def collect_properties(properties):

    return list(chain.from_iterable(properties))


@solid(
    description='Collects Search Coordinates and spawns dynamically Pipelines downstream.',
    input_defs=[InputDefinition("search_criterias", List[SearchCoordinate])],
    output_defs=[DynamicOutputDefinition(SearchCoordinate)],
)
def collect_search_criterias(context, search_criterias):
    for search in search_criterias:

        key = (
            '_'.join(
                [search['city'], search['rentOrBuy'], search['propertyType'], str(search['radius'])]
            )
            .replace("-", "_")
            .lower()
        )

        yield DynamicOutput(
            value=search, mapping_key=key,
        )


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

    search_criterias = collect_search_criterias().map(list_changed_properties)

    data_exploration(
        merge_staging_to_delta_table_composite.alias("merge_staging_to_delta_table")(
            collect_properties(search_criterias.collect())
        )
    )
