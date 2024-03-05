from dagster import (
    job,
    op,
    file_relative_path,
    config_from_files,
    graph,
    In,
    Out,
    List,
    DynamicOut,
    DynamicOutput,
    AssetOut,
    fs_io_manager,
)
from typing import List

from dagster._utils import dagster_type

from realestate.common import resource_def

# from realestate.common.solids_druid import ingest_druid
from realestate.common.solids_scraping import (
    list_props_immo24,
    cache_properies_from_rest_api,
)

# from realestate.common.resources import boto3_connection, druid_db_info_resource
from realestate.common.resources import boto3_connection, druid_db_info_resource

# from dagster_aws.s3.solids import S3Coordinate
from realestate.common.types import DeltaCoordinate
from realestate.common.types_realestate import PropertyDataFrame, SearchCoordinate

# from realestate.common.solids_filehandle import json_to_gzip
from realestate.common.solids_spark_delta import (
    # upload_to_s3,
    get_changed_or_new_properties,
    # merge_property_delta,
    # flatten_json,
    # s3_to_df,
)

# from realestate.common.solids_jupyter import data_exploration
from itertools import chain


# from dagster.core.storage.file_cache import fs_file_cache

# from dagster.core.storage.temp_file_manager import tempfile_resource


@graph(
    description="Downloads full dataset (JSON) from ImmoScout24, cache it, zip it and and upload it to S3",
    # ins={"search_criteria": In(SearchCoordinate)}, #define in function below
    # out={"properties": Out(dagster_type=PropertyDataFrame, is_required=False)},
)
def list_changed_properties(search_criteria: SearchCoordinate):
    # def list_changed_properties():
    return get_changed_or_new_properties(
        list_props_immo24(searchCriteria=search_criteria)
    )


# @graph(
#     description="""This will take the list of properties, downloads the full dataset (JSON) from ImmoScout24,
#     cache it locally to avoid scraping again in case of error. The cache will be zipped and uploaded to S3.
#     From there the JSON will be flatten and merged (with schemaEvloution=True) into the Delta Table""",
#     input_defs=[In(name="properties", dagster_type=PropertyDataFrame)],
#     output_defs=[
#         Out(name="delta_coordinate", dagster_type=DeltaCoordinate, is_required=False)
#     ],
# )
# def merge_staging_to_delta_table_composite(properties):
#     prop_s3_coordinate = upload_to_s3(cache_properies_from_rest_api(properties))
#     # return assets for property
#     return merge_property_delta(
#         input_dataframe=flatten_json(s3_to_df(prop_s3_coordinate))
#     )


# @op(
#     description="""Collect a List of `PropertyDataFrame` from different cities to a single `PropertyDataFrame` List""",
#     ins=List[In(name="properties", dagster_type=List(PropertyDataFrame))],
#     outs=[Out("properties", dagster_type=PropertyDataFrame)],
# )
# def collect_properties(properties):
#     return list(chain.from_iterable(properties))


@op(
    description="Collects Search Coordinates and spawns dynamically Pipelines downstream.",
    # ins={"search_criterias": In("search_criterias", List[SearchCoordinate])},
    # out={"sarch_coordinates": DynamicOutput(SearchCoordinate)},
    out=DynamicOut(),
)
def collect_search_criterias(context, search_criterias: List[SearchCoordinate]):
    for search in search_criterias:
        key = (
            "_".join(
                [
                    search["city"],
                    search["rentOrBuy"],
                    search["propertyType"],
                    str(search["radius"]),
                ]
            )
            .replace("-", "_")
            .lower()
        )

        yield DynamicOutput(
            search,
            mapping_key=key,
        )


@job(
    resource_defs=resource_def["local"],
    config=config_from_files(
        [
            file_relative_path(__file__, "config_environments/local_base.yaml"),
            file_relative_path(__file__, "config_pipelines/scrape_realestate.yaml"),
        ]
    ),
)
def scrape_realestate():
    search_criterias = collect_search_criterias().map(list_changed_properties)

    # data_exploration(
    #     merge_staging_to_delta_table_composite.alias("merge_staging_to_delta_table")(
    #         collect_properties(search_criterias.collect())
    #     )
    # )
