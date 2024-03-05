"""Type definitions for the realestate project"""

import json
from dagster import (
    Field,
    String,
    StringSource,
    Int,
    IntSource,
    DagsterType,
    usable_as_dagster_type
)

from dagster_aws.s3.ops import dict_with_fields

def is_json(_, value):
    try:
        json.loads(value)
        return True
    except ValueError:
        return False


JsonType = DagsterType(
    name="JsonType",
    description="A valid representation of a JSON, validated with json.loads().",
    type_check_fn=is_json,
)


SearchCoordinate = dict_with_fields(
    "SearchCoordinate",
    fields={
        "propertyType": Field(
            StringSource,
            description="Type of the Property [house, flat, real-estate (flat and house), plot, parking-space, multi-family-residential, office-commerce-industry, agriculture, other-objects]",
        ),
        "rentOrBuy": Field(
            StringSource,
            description="Searching for rent or to buy",
        ),
        "radius": Field(
            IntSource,
            description="Searched radius",
        ),
        "city": Field(StringSource, "The city in you want to search for properties"),
    },
)

@usable_as_dagster_type
class SearchCoordinateClass:
    def __init__(self, propertyType, rentOrBuy, radius, city):
        self.propertyType = propertyType
        self.rentOrBuy = rentOrBuy
        self.radius = radius
        self.city = city

# Define the fields for the SearchCoordinate type
SearchCoordinateType = SearchCoordinateClass(
    propertyType=Field(
        String,
        description="Type of the Property [house, flat, real-estate (flat and house), plot, parking-space, multi-family-residential, office-commerce-industry, agriculture, other-objects]",
    ),
    rentOrBuy=Field(
        String,
        description="Searching for rent or to buy",
    ),
    radius=Field(
        Int,
        description="Searched radius",
    ),
    city=Field(String, description="The city in you want to search for properties"),
)

PropertyDataFrame = DagsterType(
    name="PropertyDataFrame",
    type_check_fn=lambda _, value: isinstance(value, list),
    description="A List with scraped Properties with id, last_normalized_price and search criterias wich it was found.",
)

# SimplePropertyDict = dict_with_fields(
#     name='SimplePropertyDict',
#     fields={
#         'name': Field(Int, description='this is the unique ID of property'),
#         'fingerprint': Field(
#             String,
#             description='hash to identify if object need to be downloaded again if already exists',
#         ),
#         'is_prefix': Field(
#             String,
#             description='A boolean that represents whether or not the current partition holds any child partitions',
#         ),
#         'rentOrBuy': Field(String, description='If property is for only rent or to buy'),
#         'city': Field(String, description='which city it was searched from'),
#         'propertyType': Field(
#             String,
#             description='Type of the Property [house, flat, real-estate (flat and house), plot, parking-space, multi-family-residential, office-commerce-industry, agriculture, other-objects])',
#         ),
#         'radius': Field(Int, description='searched radius'),
#         'last_normalized_price': Field(Int, description='property price at time of scraping'),
#     },
# )
