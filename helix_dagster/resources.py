from dagster import ConfigurableResource
from girder_client import GirderClient


class GirderResource(ConfigurableResource):
    api_url: str
    api_key: str

    def get_client(self) -> GirderClient:
        client = GirderClient(apiUrl=self.api_url)
        client.authenticate(apiKey=self.api_key)
        return client
