from dagster import (
    execute_pipeline,
)

from realestate.pipelines import scrape_realestate

if __name__ == "__main__":
    execute_pipeline(scrape_realestate, preset='local')