<p align="left">
<a href="https://www.ssp.sh/" target="_blank"><img src="https://ssp.sh/images/sspaeti_quadrat.png" height="100"/></a>
</p>

# Building a Real-Estate Data Engineering Project in 20 Minutes

[![Open Source Logos](https://www.ssp.sh/blog/data-engineering-project-in-twenty-minutes/images/open-source-logos.png)](https://www.ssp.sh/blog/data-engineering-project-in-twenty-minutes/)

In this repository, we dive deep into the practical implementation of a data engineering project that spans across web-scraping real-estates, processing with Spark and Delta Lake, adding data science with Jupyter Notebooks, ingesting data into Apache Druid, visualizing with Apache Superset, and managing workflows with Dagster—all orchestrated on Kubernetes. 

**Built your own DE project or forked mine? Let me know in the comments; I'd be curious to know more about.**

## 🌟 About This Project

This project address common data engineering challenges while exploring innovative technologies. It's a comprehensive guide to building a data application that collects real-estate data, enriches it with various metrics, and offers insights through machine learning and data visualization. This application not only helps in finding dream properties but also showcases how to handle a full-fledged data engineering pipeline using modern tools and frameworks.

### Why this project?
- **Real-World Application**: Tackling a genuine problem with real estate data to find the best properties.
- **Comprehensive Tech Stack**: Utilizes a wide range of technologies from web scraping, cloud storage, big data processing, machine learning, to data visualization and orchestration.
- **Hands-On Learning**: Offers a hands-on approach to understanding how different technologies integrate and complement each other in a real-world scenario.

### Key Features & Learnings:
- Scraping real estate listings with [Beautiful Soup](https://beautiful-soup-4.readthedocs.io/en/latest/index.html)
    k
- Change Data Capture (CDC) mechanisms for efficient data updates
- Utilizing [MinIO](https://github.com/minio/minio) as an S3-Gateway for cloud-agnostic storage
- Implementing UPSERTs and ACID transactions with [Delta Lake](https://delta.io/) 
- Integrating [Jupyter Notebooks](https://github.com/jupyter/notebook) for data science tasks
- Visualizing data with [Apache Superset](https://github.com/apache/superset)
- Orchestrating workflows with [Dagster](https://github.com/dagster-io/dagster/)
- Deploying on [Kubernetes](https://github.com/kubernetes/kubernetes) for scalability and cloud-agnostic architecture

### Technologies, Tools, and Frameworks:
This project leverages a vast array of open-source technologies including MinIO, Spark, Delta Lake, Jupyter Notebooks, Apache Druid, Apache Superset, and Dagster—all running on Kubernetes to ensure scalability and cloud-agnostic deployment.

<p align="center">
<img src="https://www.ssp.sh/blog/data-engineering-project-in-twenty-minutes/images/lakehouse-open-sourced.png" height="500">
</p>

### 🔄 Project Evolution and Updates

This project started in November 2020 as a project for me to learn and teach about data engineering. I published the entire project in March 2021 (see the initial version on [branch `v1`](https://github.com/sspaeti-com/practical-data-engineering/tree/v1)). Three years later, it's interesting that the tools used in this project are still used today. We always say how fast the Modern Data Stack changes, but if you choose wisely, you see that good tools will stay the time. Today, in `March 2024`, I updated the project to the latest Dagster and representative tools versions. I kept most technologies, except Apache Spark, which I replaced with delta-rs, which uses Rust and can edit and write Delta Tables directly. 

Next, I might add Rill Developer to the mix to have some fun analyzing the data powered by DuckDB. For a more production-ready dashboard, Superset would still be my choice tough. 


## 🛠 Installation & Usage
Please refer to individual component directories for detailed setup and usage instructions. The project is designed to run both locally and on cloud environments, offering flexibility in deployment and testing.


### Prerequisites:
- Python and pip for installing dependencies
- MinIO running for cloud-agnostic S3 storage
- Docker Desktop & Kubernetes for running Jupyter Notebooks
- Basic understanding of Python and SQL for effective navigation and customization of the project

### Quick Start:
1. Clone this repository.
2. Install dependencies
3. Install and start MinIO
4. Explore the data with the provided Jupyter Notebooks and Superset dashboards.

```sh
#change to the pipeline directory
cd src/pipelines/real-estate

# installation
pip install -e ".[dev]"

# run minio
minio server /tmp/minio/

# startup dagster
dagster dev
```


## 📈 Visualizing the Pipeline

![Dagster UI – Practical Data Engineering Pipeline](https://www.ssp.sh/blog/data-engineering-project-in-twenty-minutes/images/Dagster-Practical-Data-Engineering-Pipeline.png)

## 📚 Resources & Further Reading
- [Building a Data Engineering Project in 20 Minutes](https://www.ssp.sh/blog/data-engineering-project-in-twenty-minutes/): Access the full blog post detailing the project's development, challenges, and solutions.
- [DevOps Repositories](https://github.com/sspaeti-com/data-engineering-devops): Explore the setup for Druid, MinIO and other components.
- [Business Intelligence Meets Data Engineering with Emerging Technologies](https://www.ssp.sh/blog/business-intelligence-meets-data-engineering/): An earlier post that dives into some of the technologies used in this project.

## 📣 Feedback
Your feedback is invaluable to improve this project. If you've built your project based on this repository or have suggestions, please let me know through the issues section or by reaching out directly.

---

*This project is part of my journey in exploring data engineering challenges and solutions. It's an open invitation for everyone interested in data engineering to learn, contribute, and share your experiences.*

*Below some impressions of the jupyter notebook used in this project.*


<p align="center">
<img src="https://sspaeti.com/blog/the-location-independent-lifestyle/europe/sspaeti_com_todays_office_033.jpg" width="600">

</p>





TODO: 
# Others

change directory to pipeline `cd $git/dagster-data-pipeline/src/pipelines/real-estate`



## Install requirements
```sh
python -m venv ~/.venvs/practical-de
source ~/.venvs/practical-de/bin/activate
pip install -e ".[dev]"
```


Make sure you set

$JAVA_HOME, have a running java and have spark configured:

`$SPARK_HOME/conf/spark-defaults.conf`


e.g. I set:
```sh
spark.master                                            k8s://https://kubernetes.docker.internal:6443
spark.serviceAccountName                                spark
spark.executor.instances                                1
spark.kubernetes.authenticate.driver.serviceAccountName spark
spark.kubernetes.container.image                        spark:spark-docker
spark.kubernetes.namespace                              default
spark.kubernetes.pyspark.pythonVersion                  3
spark.submit.deployMode                                 client
spark.delta.logStore.class                              org.apache.spark.sql.delta.storage.S3SingleDriverLogStore
spark.hadoop.fs.path.style.access                       true
spark.hadoop.fs.s3a.access.key                          minio
spark.hadoop.fs.s3a.secret.key                          miniostorage
spark.hadoop.fs.s3a.endpoint                            127.0.0.1:9000
spark.hadoop.fs.s3a.connection.ssl.enabled              false
spark.hadoop.fs.impl                                    org.apache.hadoop.fs.s3a.S3AFileSystem
spark.driver.port                                       4040
spark.sql.extensions                                    io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog                         org.apache.spark.sql.delta.catalog.DeltaCatalog
```

```

run:
```
dagster dev
```

## Automatic Tests
````
tox
```


## DuckDB

```sh
duckdb

INSTALL httpfs;
LOAD httpfs;

CREATE SECRET ( TYPE S3, KEY_ID 'minio', SECRET 'miniostorage', ENDPOINT 's3.127.0.0.1:9000');
CREATE SECRET ( TYPE S3, KEY_ID 'kV8hHTHnu9rAR3Kyjw6G', SECRET 'eaMymFXQHqJCwB8aFxF2QlyeWOkiGTD3ErQSpmQn', ENDPOINT 's3.127.0.0.1:9000');
CREATE SECRET ( TYPE S3, KEY_ID 'minio', SECRET 'miniostorage', REGION 'us-east-1');

INSTALL httpfs;
LOAD httpfs;
SET s3_region='us-east-1';
SET s3_url_style='path';
SET s3_endpoint='127.0.0.1:9000';
SET s3_access_key_id='kV8hHTHnu9rAR3Kyjw6G' ;
SET s3_secret_access_key='eaMymFXQHqJCwB8aFxF2QlyeWOkiGTD3ErQSpmQn';

CREATE TABLE bookings AS SELECT * FROM read_parquet('s3://real-estate/lake/bronze/property_odd/part-00000-1819c40f-eb5b-446e-8443-5fbc504aad1f-c000.snappy.parquet');


SELECT * FROM read_parquet('s3://real-estate/lake/bronze/property_odd/part-00000-1819c40f-eb5b-446e-8443-5fbc504aad1f-c000.snappy.parquet');

export MINIO_ACCESS_KEY=kV8hHTHnu9rAR3Kyjw6G
export MINIO_SECRET_KEY=eaMymFXQHqJCwB8aFxF2QlyeWOkiGTD3ErQSpmQn



```
