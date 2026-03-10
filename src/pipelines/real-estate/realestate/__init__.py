import dagster as dg
from .pipelines import scrape_realestate
from .common import resource_def

defs = dg.Definitions(
    jobs=[scrape_realestate],
    resources=resource_def["local"],
)
