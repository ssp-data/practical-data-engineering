from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)

from .pipelines import scrape_realestate, resource_def



# from .common import RESOURCES_LOCAL

# daily_refresh_schedule = ScheduleDefinition(
#     job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
# )

# print("Loading definitions for environment: ", resources.ENV)

# defs = Definitions(
#     assets=all_assets,
#     schedules=[daily_refresh_schedule],
#     resources=resource_def[resources.ENV.upper()],
# )
