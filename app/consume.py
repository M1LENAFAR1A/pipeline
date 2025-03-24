from app.connectors.postgres import PostgresConnector
from app.connectors.rabbitmq import RabbitMQConnector
from app.connectors.minio import MinioConnector
from app.models.transformation_pipeline import TransformationPipeline
from app.core.config import settings
from app.core.logger import logger as main_logger

# database
DB_HOST = settings.DB_HOST
DB_PORT = settings.DB_PORT
DB_NAME = settings.DB_NAME
DB_USER = settings.DB_USER
DB_PASSWORD = settings.DB_PASSWORD
DB_TABLE = settings.DB_TABLE
# rabbitmq
RABBITMQ_USER = settings.RABBITMQ_USER
RABBITMQ_PWD = settings.RABBITMQ_PWD
RABBITMQ_HOST = settings.RABBITMQ_HOST
RABBITMQ_PORT = settings.RABBITMQ_PORT
RABBITMQ_QUEUE = settings.RABBITMQ_QUEUE
# data lake
MINIO_URL = settings.MINIO_URL
MINIO_ACCESS_KEY = settings.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = settings.MINIO_SECRET_KEY
BUCKET_NAME = settings.BUCKET_NAME
# extra
TRANSFORMATION_DIR = settings.TRANSFORMATION_DIR

logger = main_logger

if __name__ == '__main__':
    postgres_connector = PostgresConnector(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        logger=logger
    )

    minio_connector = MinioConnector(
        url=MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket_name=BUCKET_NAME,
        logger=logger
    )

    transformation_pipeline = TransformationPipeline(
        output_folder=TRANSFORMATION_DIR,
        minio_connector=minio_connector
    )
    transformation_pipeline.create_transformation_directory()

    connector = RabbitMQConnector(queue=RABBITMQ_QUEUE,
                                  host=RABBITMQ_HOST,
                                  port=RABBITMQ_PORT,
                                  user=RABBITMQ_USER,
                                  pwd=RABBITMQ_PWD,
                                  postgres_connector=postgres_connector,
                                  transformation_pipeline=transformation_pipeline,
                                  logger=logger)
    connector.connect()
    connector.create_queue_if_not_exists()
    connector.start_consuming()
