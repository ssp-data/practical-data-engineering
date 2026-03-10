from collections import namedtuple

import dagster as dg
import sqlalchemy


DbInfo = namedtuple("DbInfo", "engine url jdbc_url dialect load_table host db_name")


def create_redshift_db_url(username, password, hostname, port, db_name, jdbc=True):
    if jdbc:
        db_url = (
            "jdbc:postgresql://{hostname}:{port}/{db_name}?"
            "user={username}&password={password}".format(
                username=username, password=password, hostname=hostname, port=port, db_name=db_name
            )
        )
    else:
        db_url = "redshift+psycopg2://{username}:{password}@{hostname}:{port}/{db_name}".format(
            username=username, password=password, hostname=hostname, port=port, db_name=db_name
        )
    return db_url


def create_redshift_engine(db_url):
    return sqlalchemy.create_engine(db_url)


def create_postgres_db_url(username, password, hostname, port, db_name, jdbc=True):
    if jdbc:
        db_url = (
            "jdbc:postgresql://{hostname}:{port}/{db_name}?"
            "user={username}&password={password}".format(
                username=username, password=password, hostname=hostname, port=port, db_name=db_name
            )
        )
    else:
        db_url = "postgresql://{username}:{password}@{hostname}:{port}/{db_name}".format(
            username=username, password=password, hostname=hostname, port=port, db_name=db_name
        )
    return db_url


def create_postgres_engine(db_url):
    return sqlalchemy.create_engine(db_url)


class RedshiftResource(dg.ConfigurableResource):
    username: str
    password: str
    hostname: str
    port: int = 5439
    db_name: str
    s3_temp_dir: str

    def get_db_info(self) -> DbInfo:
        db_url_jdbc = create_redshift_db_url(
            username=self.username,
            password=self.password,
            hostname=self.hostname,
            port=self.port,
            db_name=self.db_name,
        )
        db_url = create_redshift_db_url(
            username=self.username,
            password=self.password,
            hostname=self.hostname,
            port=self.port,
            db_name=self.db_name,
            jdbc=False,
        )

        def _do_load(data_frame, table_name):
            data_frame.write.format("com.databricks.spark.redshift").option(
                "tempdir", self.s3_temp_dir
            ).mode("overwrite").jdbc(db_url_jdbc, table_name)

        return DbInfo(
            url=db_url,
            jdbc_url=db_url_jdbc,
            engine=create_redshift_engine(db_url),
            dialect="redshift",
            load_table=_do_load,
            host=self.hostname,
            db_name=self.db_name,
        )


class PostgresResource(dg.ConfigurableResource):
    username: str
    password: str
    hostname: str
    port: int = 5432
    db_name: str

    def get_db_info(self) -> DbInfo:
        db_url_jdbc = create_postgres_db_url(
            username=self.username,
            password=self.password,
            hostname=self.hostname,
            port=self.port,
            db_name=self.db_name,
        )
        db_url = create_postgres_db_url(
            username=self.username,
            password=self.password,
            hostname=self.hostname,
            port=self.port,
            db_name=self.db_name,
            jdbc=False,
        )

        def _do_load(data_frame, table_name):
            data_frame.write.option("driver", "org.postgresql.Driver").mode("overwrite").jdbc(
                db_url_jdbc, table_name
            )

        return DbInfo(
            url=db_url,
            jdbc_url=db_url_jdbc,
            engine=create_postgres_engine(db_url),
            dialect="postgres",
            load_table=_do_load,
            host=self.hostname,
            db_name=self.db_name,
        )
