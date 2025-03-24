import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()


class Settings(BaseSettings):
    PROJECT_NAME: str = 'Transformation Consumer'
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')

    # database configuration
    DB_HOST: str = os.getenv('DB_HOST')
    DB_PORT: int = int(os.getenv('DB_PORT'))
    DB_NAME: str = os.getenv('DB_NAME')
    DB_USER: str = os.getenv('DB_USER')
    DB_PASSWORD: str = os.getenv('DB_PASSWORD')
    DB_TABLE: str = os.getenv('DB_TABLE')

    # rabbitmq configuration
    RABBITMQ_USER: str = os.getenv("RABBITMQ_USER")
    RABBITMQ_PWD: str = os.getenv("RABBITMQ_PWD")
    RABBITMQ_HOST: str = os.getenv("RABBITMQ_HOST")
    RABBITMQ_PORT: int | str = os.getenv("RABBITMQ_PORT")
    RABBITMQ_QUEUE: str = os.getenv('RABBITMQ_QUEUE')

    # minio configuration
    MINIO_URL: str = os.getenv('MINIO_URL')
    MINIO_ACCESS_KEY: str = os.getenv('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY: str = os.getenv('MINIO_SECRET_KEY')
    BUCKET_NAME: str = os.getenv('MINIO_BUCKET_NAME', 'environbit')

    # other
    TAXONOMY_FILE: str = os.getenv('TAXONOMY_FILE')
    TRANSFORMATION_DIR: str = os.getenv('TRANSFORMATION_DIR', '/tmp/transformation/')
    MAX_RETRY: int = os.getenv('MAX_RETRY', 4)


settings = Settings()
