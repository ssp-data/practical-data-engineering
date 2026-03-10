"test json reading"

import pandas as pd
import json

import dagster as dg
from realestate.common.solids_spark_delta import flatten_json
from realestate.common.solids_scraping import cache_properies_from_rest_api

context_cache = dg.build_op_context(
    op_config={
        "immo24_api_en": "https://rest-api.immoscout24.ch/v4/en/properties/",
    }
)

context = dg.build_op_context(
    op_config={
        "remove_columns": [
            "propertyDetails_images",
            "propertyDetails_pdfs",
            "propertyDetails_commuteTimes_defaultPois_transportations",
            "viewData_viewDataWeb_webView_structuredData",
        ],
    }
)


# Note: flatten_json now takes a LocalFileHandle, not raw JSON.
# These tests need rework to provide a LocalFileHandle pointing at gzipped JSON.

# def test_op_cache_properies_from_rest_api():
#     properties = [4000830789]
#     assert cache_properies_from_rest_api(context_cache, properties, "test").is_file, "File is not created"


# def test_op_property_json():
#     with open("property.json", "r") as file:
#         json_data = json.load(file)
#     # flatten_json now requires a LocalFileHandle, not raw JSON
#     pass


# def test_op_with_formated():
#     with open("property2.json", "r") as file:
#         json_data = json.load(file)
#     # flatten_json now requires a LocalFileHandle, not raw JSON
#     pass
