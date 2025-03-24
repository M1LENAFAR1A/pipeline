import logging
import os
import sys
from typing import BinaryIO, Union

from minio import Minio

sys.path.append('/opt/airflow')
from model.connector import Connector

logger = logging.getLogger(__name__)


class MinioConnector(Connector):
    def __init__(self,
                 url: str,
                 access_key: str,
                 secret_key: str,
                 bucket_name: str):
        self.client = Minio(
            url,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        self.bucket_name = bucket_name

    def upload_data(self, data: Union[BinaryIO, bytes], path: str, content_type: str, data_size: int):
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

        result = self.client.put_object(
            bucket_name=self.bucket_name,
            object_name=path,
            data=data,
            length=data_size,
            content_type=content_type
        )

        logger.info("created {0} object; etag: {1}, version-id: {2}"
                    .format(result.object_name, result.etag, result.version_id))
        logger.info(f"Data successfully uploaded to MinIO in bucket {self.bucket_name} under {path}")
