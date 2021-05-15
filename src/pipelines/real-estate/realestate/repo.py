from dagster import repository

from realestate.pipelines import scrape_realestate, scrape_realestate_dynamically


@repository
def realestate_repo():
    return [scrape_realestate, scrape_realestate_dynamically]
