# Real-Estate Pipeline

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
