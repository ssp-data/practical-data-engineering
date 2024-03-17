from dagster import (
    job,
    op,
    file_relative_path,
    config_from_files,
    graph,
    Out,
    GraphOut,
    List,
    DynamicOut,
    DynamicOutput,
)
from typing import List

import pandas as pd

from realestate.common import resource_def
from realestate.common.helper_functions import reading_delta_table

# from realestate.common.solids_druid import ingest_druid
from realestate.common.solids_scraping import (
    list_props_immo24,
    cache_properies_from_rest_api,
)

from realestate.common.resources import boto3_connection, druid_db_info_resource

# from dagster_aws.s3.solids import S3Coordinate
from realestate.common.types import DeltaCoordinate
from realestate.common.types_realestate import PropertyDataFrame, SearchCoordinate

from realestate.common.solids_spark_delta import (
    get_changed_or_new_properties,
    merge_property_delta,
    flatten_json,
)

from realestate.common.solids_jupyter import data_exploration
from itertools import chain

@op(
    description="Reads the Delta Table from S3", out=Out(io_manager_key="fs_io_manager")
)
def property_table(context) -> pd.DataFrame:
    # should be with SourceAsset, but didnt' work: property_table = SourceAsset(key="s3a://real-estate/test/property")
    
    # s3_path_property = "s3a://real-estate/test/property"
    s3_path_property = "s3a://real-estate/lake/bronze/property"
    
    df, dt = reading_delta_table(context, s3_path_property)
    return df


# @asset
# def property() -> pd.DataFrame:
#     return pd.read_json(
#         "/Users/sspaeti/Documents/minio_bak/real-estate/src/azure-blob/scrapes/immo24ads__196b0de5-5511-4e0d-ae0f-be5d03bc943e",
#     )


@graph(
    description="Downloads full dataset (JSON) from ImmoScout24, cache it, zip it and and upload it to S3",
    # ins={"search_criteria": In(SearchCoordinate)}, #define in function below
    # out={"properties": Out(dagster_type=PropertyDataFrame, is_required=False)},
)
def list_changed_properties(search_criteria: SearchCoordinate):
    # def list_changed_properties():
    return get_changed_or_new_properties(
        properties=list_props_immo24(searchCriteria=search_criteria),
        property_table=property_table(),
    )


@graph(
    description="""This will take the list of properties, downloads the full dataset (JSON) from ImmoScout24,
    cache it locally to avoid scraping again in case of error. The cache will be zipped and uploaded to S3.
    From there the JSON will be flatten and merged (with schemaEvloution=True) into the Delta Table""",
    # input_defs=[In(name="properties", dagster_type=PropertyDataFrame)],
    out={"delta_coordinate": GraphOut()},
)
def merge_staging_to_delta_table_composite(properties: PropertyDataFrame) -> DeltaCoordinate:
    file_handle = cache_properies_from_rest_api(properties)

    return merge_property_delta(
        input_dataframe=flatten_json(file_handle)
    )


@op(
    description="""Collect a List of `PropertyDataFrame` from different cities to a single `PropertyDataFrame` List""",
    out={"properties": Out(dagster_type=PropertyDataFrame, io_manager_key="fs_io_manager")},
)
def collect_properties(properties: List[PropertyDataFrame]) -> List[PropertyDataFrame]:  # type: ignore
    return list(chain.from_iterable(properties))



@op(
    description="Collects Search Coordinates and spawns dynamically Pipelines downstream.",
    # ins={"search_criterias": In("search_criterias", List[SearchCoordinate])},
    # out={"sarch_coordinates": DynamicOutput(SearchCoordinate)},
    required_resource_keys={"fs_io_manager"},
    out=DynamicOut(io_manager_key="fs_io_manager"),
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

    data_exploration(
        merge_staging_to_delta_table_composite.alias("merge_staging_to_delta_table")(
            collect_properties(search_criterias.collect())
        )
    )
