from dagster import repository

from realestate.pipelines import scrape_realestate


@repository
def realestate_repo():
    return [scrape_realestate]
