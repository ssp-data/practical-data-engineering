from dagster import resource, Field

from boto3 import session


class Boto3Connector(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key, endpoint_url):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url

    # def get_session(self):

    #     session = session.Session()
    #     return session

    def get_client(self):
        session = session.Session()

        s3_client = session.client(
            service_name="s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            endpoint_url=self.endpoint_url,
        )
        return s3_client


@resource(
    config_schema={
        "aws_access_key_id": Field(str),
        "aws_secret_access_key": Field(str),
        "endpoint_url": Field(str),
    }
)
def boto3_connection(context):
    return Boto3Connector(
        context.resource_config["aws_access_key_id"],
        context.resource_config["aws_secret_access_key"],
        context.resource_config["endpoint_url"],
    )


class DruidConnector(object):
    def __init__(self, druid_router):  # , druid_auth_user, druid_auth_password):
        self._druid_router = druid_router
        # self._druid_auth_user = druid_auth_user
        # self._druid_auth_password = druid_auth_password

    # establish authentificated request-session and return it
    def get_session(self):
        session = requests.Session()
        return session

    def get_auth_session(self):
        session = requests.Session()
        # session.auth = (self._druid_auth_user, self._druid_auth_password)
        return session

    def get_router_url(self):
        return self._druid_router


@resource(
    config_schema={
        "druid_router": Field(str),
        # 'druid_auth_user': Field(str),
        # 'druid_auth_password': Field(str),
    }
)
def druid_db_info_resource(context):
    return DruidConnector(
        context.resource_config["druid_router"],
        # context.resource_config['druid_auth_user'],
        # context.resource_config['druid_auth_password'],
    )
