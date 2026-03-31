from contextlib import contextmanager

import requests
from dagster import ConfigurableResource
from girder_client import GirderClient
from pydantic import PrivateAttr


class GirderClientWithSession(GirderClient):
    def __init__(
        self,
        host=None,
        port=None,
        apiRoot=None,
        scheme=None,
        apiUrl=None,
        apiKey=None,
        token=None,
        session=None,
        cacheSettings=None,
        progressReporterCls=None,
    ):
        super().__init__(
            host=host,
            port=port,
            apiRoot=apiRoot,
            scheme=scheme,
            apiUrl=apiUrl,
            cacheSettings=cacheSettings,
            progressReporterCls=progressReporterCls,
        )

        if token:
            self.setToken(token)

        if apiKey:
            self.authenticate(apiKey=apiKey)

        self._session = session


class GirderCredentials(ConfigurableResource):
    api_url: str
    api_key: str


class GirderConnection(ConfigurableResource):
    credentials: GirderCredentials
    _client: GirderClientWithSession = PrivateAttr()

    @contextmanager
    def yield_for_execution(self, context):
        with requests.Session() as session:
            self._client = GirderClientWithSession(
                apiUrl=self.credentials.api_url,
                apiKey=self.credentials.api_key,
                session=session,
            )
            yield self._client

    @property
    def client(self):
        if not self._client:
            raise Exception(
                "Client not initialized. Use yield_for_execution context manager."
            )
        return self._client
