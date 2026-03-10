import dagster as dg
from typing import List

import pandas as pd

from realestate.common.helper_functions import reading_delta_table

# from realestate.common.solids_druid import ingest_druid
from realestate.common.solids_scraping import (
    list_props_immo24,
    cache_properies_from_rest_api,
)

from realestate.common.types import DeltaCoordinate
from realestate.common.types_realestate import PropertyDataFrame, SearchCoordinate

from realestate.common.solids_spark_delta import (
    get_changed_or_new_properties,
    merge_property_delta,
    flatten_json,
)

from realestate.common.solids_jupyter import data_exploration
from itertools import chain


@dg.op(
    description="Reads the Delta Table from S3",
    out=dg.Out(io_manager_key="fs_io_manager"),
)
def property_table(context) -> pd.DataFrame:
    s3_path_property = "s3a://real-estate/lake/bronze/property"

    df, _dt = reading_delta_table(context, s3_path_property)
    return df


@dg.graph(
    description="Downloads full dataset (JSON) from ImmoScout24, cache it, zip it and and upload it to S3",
)
def list_changed_properties(search_criteria: SearchCoordinate):
    return get_changed_or_new_properties(
        properties=list_props_immo24(searchCriteria=search_criteria),
        property_table=property_table(),
    )


@dg.graph(
    description="""This will take the list of properties, downloads the full dataset (JSON) from ImmoScout24,
    cache it locally to avoid scraping again in case of error. The cache will be zipped and uploaded to S3.
    From there the JSON will be flatten and merged (with schemaEvloution=True) into the Delta Table""",
    out={"delta_coordinate": dg.GraphOut()},
)
def merge_staging_to_delta_table_composite(properties: PropertyDataFrame) -> DeltaCoordinate:
    file_handle = cache_properies_from_rest_api(properties)

    return merge_property_delta(
        input_dataframe=flatten_json(file_handle)
    )


@dg.op(
    description="""Collect a List of `PropertyDataFrame` from different cities to a single `PropertyDataFrame` List""",
    out={"properties": dg.Out(dagster_type=PropertyDataFrame, io_manager_key="fs_io_manager")},
)
def collect_properties(properties: List[PropertyDataFrame]) -> List[PropertyDataFrame]:  # type: ignore
    return list(chain.from_iterable(properties))



@dg.op(
    description="Collects Search Coordinates and spawns dynamically Pipelines downstream.",
    out=dg.DynamicOut(io_manager_key="fs_io_manager"),
)
def collect_search_criterias(context, search_criterias: list):  # noqa: ARG001
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

        yield dg.DynamicOutput(
            search,
            mapping_key=key,
        )


@dg.job(
    config=dg.config_from_files(
        [
            dg.file_relative_path(__file__, "config_pipelines/scrape_realestate.yaml"),
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
    # TODO: wire up ingest_druid after data_exploration when Druid Docker is ready
    # ingest_druid(delta_coordinate=..., druid_coordinate=...)
