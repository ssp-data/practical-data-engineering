from dagster import InputDefinition, Field, OutputDefinition, FileHandle
import dagstermill as dm
from dagster.utils import script_relative_path
import os


def _notebook_path(name):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "notebooks", name)


def notebook_solid(name, notebook_path, input_defs, output_defs, required_resource_keys=None):
    return dm.define_dagstermill_solid(
        name,
        _notebook_path(notebook_path),
        input_defs,
        output_defs,
        required_resource_keys=required_resource_keys,
    )


# TODO: add spark as resource and use configs inside notebook.
# TODO: plot pdfs -> copy airline demo notebook
data_exploration = notebook_solid(
    "data_exploration",
    "comprehensive-real-estate-data-exploration.ipynb",
    input_defs=[
        InputDefinition("delta_path", str, description="s3 path to the property-delta-table"),
    ],
    output_defs=[
        OutputDefinition(
            dagster_type=FileHandle,
            # name='plots_pdf_path',
            description="The saved PDF plots.",
        )
    ],
    # required_resource_keys={"db_info"},
)

# data_exploration = dm.define_dagstermill_solid(
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
