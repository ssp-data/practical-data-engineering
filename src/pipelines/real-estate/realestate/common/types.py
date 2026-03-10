import dagster as dg


#################################################################################
# Delta Coordinate
#################################################################################


def delta_coordinate_type_check(_context, value):
    if not isinstance(value, dict):
        return False
    expected_fields = {
        "database": str,
        "table_name": str,
        "s3_coordinate_bucket": str,
        "s3_coordinate_key": str,
    }
    for field, field_type in expected_fields.items():
        if field not in value or not isinstance(value[field], field_type):
            return False
    return True


DeltaCoordinate = dg.DagsterType(
    name="DeltaCoordinate",
    description="""A dictionary containing details about a delta coordinate,
                   including 'database', 'table_name', 's3_coordinate_bucket',
                   and 's3_coordinate_key'.""",
    type_check_fn=delta_coordinate_type_check,
)


#################################################################################
# Druid Coordinate
#################################################################################


def druid_coordinate_type_check(_context, value):
    if not isinstance(value, dict):
        return False
    required_keys = {"datasource", "intervalToDelete", "PathToJsonIngestSpec"}
    return all(k in value and isinstance(value[k], str) for k in required_keys)


DruidCoordinate = dg.DagsterType(
    name="DruidCoordinate",
    description="A dictionary with 'datasource', 'intervalToDelete', and 'PathToJsonIngestSpec' for Druid ingestion.",
    type_check_fn=druid_coordinate_type_check,
)


SqlTableName = dg.DagsterType(
    name="SqlTableName",
    description="The name of a database table",
    type_check_fn=lambda _context, value: isinstance(value, str),
)


#################################################################################
# S3 Coordinate
#################################################################################


S3Coordinate = dg.DagsterType(
    name="S3Coordinate",
    description="A dictionary with 'bucket' and 'key' to specify an S3 location.",
    type_check_fn=lambda _context, obj: isinstance(obj, dict)
    and "bucket" in obj
    and "key" in obj,
)
