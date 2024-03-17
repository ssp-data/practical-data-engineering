from dagster import Out, FileHandle, In, file_relative_path
import os

import dagstermill as dm
from realestate.common.types import DeltaCoordinate


def _notebook_path(name):
    return os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "notebooks")),
        name,
    )
    # os.path.join(os.path.dirname(os.path.abspath(__file__)), "notebooks", name)


# TODO: add spark as resource and use configs inside notebook.
# TODO: plot pdfs -> copy airline demo notebook
data_exploration = dm.factory.define_dagstermill_op(
    name="data_exploration",
    notebook_path=_notebook_path("comprehensive-real-estate-data-exploration.ipynb"),
    ins={
        "delta_path": In(
            dagster_type=DeltaCoordinate, description="s3 path to the property-delta-table"
        ),
        "key": In(dagster_type=str, description="s3 key"),
        "secret": In(dagster_type=str, description="s3 secret"),
        "endpoint": In(dagster_type=str, description="s3 endpoint"),
        # InputDefinition("pyspark", ModeDefinition, description="pyspark resource"),
    },
    outs={
        "plots_pdf_path": Out(
            dagster_type=FileHandle, description="The saved PDF plots."
        )
    },
    io_manager_key="fs_io_manager",
    # required_resource_keys={"pyspark"},
)


# from dagster.utils import script_relative_path
# data_exploration = dm.factory.define_dagstermill_op(
#     "data_exploration",
#     script_relative_path("../notebooks/comprehensive-real-estate-data-exploration.ipynb"),
#     input_defs=[
#         InputDefinition("delta_path", str, description="s3 path to the property-delta-table")
#     ],
#     # config_schema={
#     #     'delta_path': Field(
#     #         str,
#     #         default_value="s3a://real-estate/lake/bronze/property",
#     #         is_required=False,
#     #         description="s3 path to the property-delta-table",
#     #     )
#     # },
# )
