"""Type definitions for the realestate project"""

import json
from dagster import (
    Field,
    String,
    make_python_type_usable_as_dagster_type,
    Dict,
    check,
    dagster_type_loader,
    Permissive,
    List,
    Int,
    DagsterType,
    Any,
)
from dagster.core.types.dagster_type import PythonObjectDagsterType, create_string_type

# copied from dagster_aws.s3.solids import dict_with_fields - attention, don't activate import from s3.solid, this will require boto3 as S3FileHandle is included there
def dict_with_fields(name, fields):
    check.str_param(name, 'name')
    check.dict_param(fields, 'fields', key_type=str)
    field_names = set(fields.keys())

    @dagster_type_loader(fields)
    def _input_schema(_context, value):
        check.dict_param(value, 'value')
        check.param_invariant(set(value.keys()) == field_names, 'value')
        return value

    class _DictWithSchema(PythonObjectDagsterType):
        def __init__(self):
            super(_DictWithSchema, self).__init__(python_type=dict, name=name, loader=_input_schema)

    return _DictWithSchema()


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
    name='SearchCoordinate',
    fields={
        'propertyType': Field(
            String,
            description='Type of the Property [house, flat, real-estate (flat and house), plot, parking-space, multi-family-residential, office-commerce-industry, agriculture, other-objects]',
        ),
        'rentOrBuy': Field(String, description='Searching for rent or to buy',),
        'radius': Field(Int, description='Searched radius',),
        'city': Field(String, 'The city in you want to search for properties'),
    },
)

PropertyDataFrame = DagsterType(
    name="PropertyDataFrame",
    type_check_fn=lambda _, value: isinstance(value, list) or value is None,
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
