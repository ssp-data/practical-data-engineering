from dagster import Field, String, check, dagster_type_loader, DagsterType
from dagster.core.types.dagster_type import PythonObjectDagsterType, create_string_type


# copied from dagster_aws.s3.solids import dict_with_fields - attention, don't activate import from s3.solid, this will require boto3 as S3FileHandle is included there
def dict_with_fields(name, fields):
    check.str_param(name, "name")
    check.dict_param(fields, "fields", key_type=str)
    field_names = set(fields.keys())

    @dagster_type_loader(fields)
    def _input_schema(_context, value):
        check.dict_param(value, "value")
        check.param_invariant(set(value.keys()) == field_names, "value")
        return value

    class _DictWithSchema(PythonObjectDagsterType):
        def __init__(self):
            super(_DictWithSchema, self).__init__(
                python_type=dict, name=name, loader=_input_schema
            )

    return _DictWithSchema()


#################################################################################
# Delta Coordinate
#################################################################################


# Define the type check function
def delta_coordinate_type_check(_context, value):
    # Check if value is a dictionary
    if not isinstance(value, dict):
        return False

    # Check for required keys and types
    expected_fields = {
        "database": str,
        "table_name": str,
        "s3_coordinate_bucket": str,
        "s3_coordinate_key": str,
    }

    # Ensure all fields are present and of the correct type
    for field, field_type in expected_fields.items():
        if field not in value or not isinstance(value[field], field_type):
            return False

    return True


# Define the custom DagsterType
DeltaCoordinate = DagsterType(
    name="DeltaCoordinate",
    description="""A dictionary containing details about a delta coordinate, 
                   including 'database', 'table_name', 's3_coordinate_bucket', 
                   and 's3_coordinate_key'.""",
    type_check_fn=delta_coordinate_type_check,
)

# DeltaCoordinate = dict_with_fields(
#     name="DeltaCoordinate",
#     fields={
#         "database": Field(String, description="database or schema or delta table"),
#         "table_name": Field(String, description="table name of the delta table"),
#         "s3_coordinate_bucket": Field(String, description="s3 bucket"),
#         "s3_coordinate_key": Field(String, description="s3 delta table path"),
#     },
# )

# make_python_type_usable_as_dagster_type(python_type=Dict, dagster_type=DeltaCoordinate)

#################################################################################
# Druid Coordinate
#################################################################################

DruidCoordinate = dict_with_fields(
    name="DruidCoordinate",
    fields={
        "datasource": Field(String, description="druid datasource"),
        "intervalToDelete": Field(
            String,
            description="this defines the interval to delete the segment with the format: [YYYY-MM-DDTHH:MM:SS.MMMZ-YYYY-MM-DDTHH:MM:SS.MMMZ]",
        ),
        "PathToJsonIngestSpec": Field(
            String, description="path from project root to druid ingestion spec"
        ),
    },
)
# make_python_type_usable_as_dagster_type(python_type=Permissive, dagster_type=DruidCoordinate)

SqlTableName = create_string_type(
    "SqlTableName", description="The name of a database table"
)

#################################################################################
# S3 Coordinate
#################################################################################


# This Dagster type checks if an object is a dictionary with "bucket" and "key" fields
S3Coordinate = DagsterType(
    name="S3Coordinate",
    description="A dictionary with 'bucket' and 'key' to specify an S3 location.",
    type_check_fn=lambda _context, obj: isinstance(obj, dict)
    and "bucket" in obj
    and "key" in obj,
)
