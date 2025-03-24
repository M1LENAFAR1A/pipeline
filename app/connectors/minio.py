from logging import Logger

from minio import Minio
from minio.error import S3Error

from app.models.data_lake_connector import DataLakeConnector
from app.schemas.response import MinioResponse


class MinioConnector(DataLakeConnector):
    def __init__(self, url: str,
                 access_key: str,
                 secret_key: str,
                 bucket_name: str,
                 logger: Logger):
        self.logger = logger
        self.client = Minio(
            url,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

        self.bucket_name = bucket_name

        if not self.client.bucket_exists(self.bucket_name):
            self.logger.info(f"Creating bucket {self.bucket_name}")
            self.client.make_bucket(self.bucket_name)

    def get_object(self, file_path: str) -> MinioResponse:
        response = None
        try:
            response = self.client.get_object(self.bucket_name, file_path)
        except S3Error as err:
            if err.code == 'NoSuchKey':
                return MinioResponse(success=False, value="File not found in the bucket but exists on database")
        else:
            return MinioResponse(success=True, value=response.data)
        finally:
            if response:
                response.close()
                response.release_conn()

    def upload_data(self, file_path: str, data: bytes, content_type: str, file_size: int):
        result = self.client.put_object(
            bucket_name=self.bucket_name,
            object_name=file_path,
            data=data,
            length=file_size,
            content_type=content_type
        )

        self.logger.info("Created {0} object; etag: {1}, version-id: {2}"
                         .format(result.object_name, result.etag, result.version_id))
        self.logger.info(f"Data successfully uploaded to MinIO in bucket {self.bucket_name} under {file_path}")
