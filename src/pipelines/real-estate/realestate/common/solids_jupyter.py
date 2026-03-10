import os

import dagster as dg
import dagstermill as dm
from realestate.common.types import DeltaCoordinate


def _notebook_path(name):
    return os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "notebooks")),
        name,
    )


data_exploration = dm.define_dagstermill_op(
    name="data_exploration",
    notebook_path=_notebook_path("comprehensive-real-estate-data-exploration.ipynb"),
    ins={
        "delta_path": dg.In(description="s3 path to the property-delta-table"),
        "key": dg.In(dagster_type=str, description="s3 key"),
        "secret": dg.In(dagster_type=str, description="s3 secret"),
        "endpoint": dg.In(dagster_type=str, description="s3 endpoint"),
    },
    outs={
        "plots_pdf_path": dg.Out(
            dagster_type=dg.Nothing, description="The saved PDF plots."
        )
    },
)
