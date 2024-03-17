# Data API

This is the start of the `Data API`:

![Data API](https://github.com/sspaeti-com/practical-data-engineering/blob/main/src/data_api/Data%20API.jpg?raw=true)


It's planed to use these open-source tools:
- [Strawberry GraphQL](https://github.com/strawberry-graphql/strawberry) for GraphQL
- [Dbt](https://github.com/dbt-labs/dbt-core) (SQL) / [dagster](dagster.io) (Python) for the orchestration part
- [Metriql](https://github.com/metriql/metriql) for the metadata store to define metrics and dimensions in a uniformal way
- [Amundsen](https://amundsen.io/) for the data catalog and discovery
- [Great Expectations](https://github.com/great-expectations/great_expectations) for eliminate pipeline debt, through data testing, documentation, and profiling



## run GraphQL API

1. run `strawberry server api_query_engine`
2. go to GraphQL in http://0.0.0.0:8000/graphql
3. query with
```graphql
{
  books {
    title
    author
  }
}
````

## Install Metriql:
1. Install `requirements.txt` into your virtual env
2. cd `data_api/data_api/orchestraton/`
3. start a dbt project like `dbt init dbt`
4. install postgres `brew install`postgresql`
5. start postgresql `brew services start postgresql`
6. create a `profile.yml` in `~/.dbt/` and define postgres or similar
7. create `source.yml` in `models`
8. run `dbt list` (full list [here](https://metriql.com/tutorial/for-starters))
9. install metriql: [installation](https://metriql.com/metriql-cli/cli-overview/#installation)
10. run API:
```
docker run -it -p "${METRIQL_PORT}:5656" -v "${DBT_PROJECT_DIR}:/root/app" -v "${DBT_PROFILES_DIR}:/root/.dbt" -e METRIQL_RUN_HOST=127.0.0.1 -e DBT_PROJECT_DIR=/root/app buremba/metriql \
 serve
```