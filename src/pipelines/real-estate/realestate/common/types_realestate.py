"""Type definitions for the realestate project"""

import json
import dagster as dg


def is_json(_, value):
    try:
        json.loads(value)
        return True
    except ValueError:
        return False


JsonType = dg.DagsterType(
    name="JsonType",
    description="A valid representation of a JSON, validated with json.loads().",
    type_check_fn=is_json,
)


def search_coordinate_type_check(_context, value):
    if not isinstance(value, dict):
        return False
    required_keys = {"propertyType", "rentOrBuy", "radius", "city"}
    return all(k in value for k in required_keys)


SearchCoordinate = dg.DagsterType(
    name="SearchCoordinate",
    description="A dictionary with 'propertyType', 'rentOrBuy', 'radius', and 'city' for property search.",
    type_check_fn=search_coordinate_type_check,
)


PropertyDataFrame = dg.DagsterType(
    name="PropertyDataFrame",
    type_check_fn=lambda _, value: isinstance(value, list),
    description="A List with scraped Properties with id, last_normalized_price and search criterias wich it was found.",
)
