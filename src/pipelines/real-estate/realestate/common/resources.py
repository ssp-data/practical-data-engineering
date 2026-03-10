import dagster as dg
import requests
from boto3 import session as boto3_session


class Boto3Resource(dg.ConfigurableResource):
    aws_access_key_id: str
    aws_secret_access_key: str
    endpoint_url: str

    def get_client(self):
        sess = boto3_session.Session()
        s3_client = sess.client(
            service_name="s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            endpoint_url=self.endpoint_url,
        )
        return s3_client


class DruidResource(dg.ConfigurableResource):
    druid_router: str = "http://localhost:8888"

    def get_session(self):
        return requests.Session()

    def get_router_url(self):
        return self.druid_router
