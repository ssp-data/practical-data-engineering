# dagster-data-pipelines



## run airline demo
```
# Clone Dagster
cd $git/dagster-data-pipeline/src/pipelines/airline_demo

# Install all dependencies
pip install -e .

# Start a local PostgreSQL database
docker-compose up -d

# Load the airline demo in Dagit
cd airline_demo
dagit
```



## TODO's

Include and incorporate: https://github.com/sspaeti/real-estate

* include notebooks from immo24 (heat map)
* include Google Way Calculation
* include latest ascend scraper code