
import os
from dagster import ConfigurableResource
from pipelines.minio_upload import create_minio_client

class MinioResource(ConfigurableResource):
    endpoint_url: str
    access_key_id: str
    secret_access_key: str

    def get_client(self):
        return create_minio_client(
            self.endpoint_url, self.access_key_id, self.secret_access_key
        )
