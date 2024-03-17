# Notes


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

