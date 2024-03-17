"test json reading"

import pandas as pd
import json

from dagster import build_op_context, Field, String
from realestate.common.solids_spark_delta import flatten_json_with_pandas
from realestate.common.solids_scraping import cache_properies_from_rest_api

context_cache = build_op_context(
    op_config={
        "immo24_api_en": "https://rest-api.immoscout24.ch/v4/en/properties/",
    }
)

context = build_op_context(
    op_config={
        "remove_columns": [
            "propertyDetails_images",
            "propertyDetails_pdfs",
            "propertyDetails_commuteTimes_defaultPois_transportations",
            "viewData_viewDataWeb_webView_structuredData",
        ],
    }
)


def test_op_cache_properies_from_rest_api():
    properties = [4000830789]

    assert cache_properies_from_rest_api(context_cache, properties, "test").is_file, "File is not created"



def test_op_property_json():
    with open("property.json", "r") as file:
        json_data = json.load(file)

    result_df = flatten_json_with_pandas(context, json_data)
    # assert not result_df.empty, "DataFrame is unexpectedly empty"
    # assert flatten_json_with_pandas(context, json_data) == pd.DataFrame()
    try:
        assert not result_df.empty, "DataFrame is unexpectedly empty"
    except AssertionError as e:
        print(f"DataFrame is unexpectedly empty: {e}")
        raise e  # Re-raise the assertion error

    assert result_df.equals(pd.DataFrame()), "DataFrame not as expected"


def test_op_with_formated():
    with open("property2.json", "r") as file:
        json_data = json.load(file)

    assert flatten_json_with_pandas(context, json_data).equals(
        pd.DataFrame()
    ), "DataFrame not as expected"
