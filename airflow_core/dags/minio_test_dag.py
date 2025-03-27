# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.hooks.S3_hook import S3Hook

# BUCKET_NAME = "test-bucket"
# FILE_NAME = "airflow_test.txt"
# CONTENT = "Arquivo enviado via DAG do Airflow 🚀"

# def upload_to_minio():
#     hook = S3Hook(aws_conn_id="minio_s3")
#     # Cria o bucket se não existir
#     if not hook.check_for_bucket(BUCKET_NAME):
#         hook.create_bucket(bucket_name=BUCKET_NAME)
#     # Envia conteúdo para o arquivo
#     hook.load_string(
#         string_data=CONTENT,
#         key=FILE_NAME,
#         bucket_name=BUCKET_NAME,
#         replace=True
#     )
#     print(f"✔️ Arquivo '{FILE_NAME}' enviado com sucesso para bucket '{BUCKET_NAME}'.")

# with DAG(
#     dag_id="minio_upload_test",
#     schedule_interval=None,  # Executa apenas manualmente
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=["test", "minio"],
# ) as dag:

#     upload_task = PythonOperator(
#         task_id="upload_file_to_minio",
#         python_callable=upload_to_minio
#     )


from datetime import datetime
import logging
import io
import os

from airflow import DAG
from airflow.decorators import task
from minio import Minio
from minio.error import S3Error
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='minio_upload_test',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['minio', 'test'],
) as dag:

    @task()
    def upload_file_to_minio():
        # Variáveis de conexão
        minio_url = Variable.get("DATA_LAKE_URL")  # ex: http://host.docker.internal:9000
        access_key = Variable.get("DATA_LAKE_ACCESS_KEY")
        secret_key = Variable.get("DATA_LAKE_SECRET_KEY")
        bucket_name = Variable.get("DATA_LAKE_BUCKET_NAME")

        # Inicializa cliente do MinIO
        client = Minio(
            endpoint=minio_url.replace("http://", ""),  # Minio não aceita "http://" no início
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )

        logging.info(f"Conectando no MinIO em {minio_url} com bucket '{bucket_name}'")

        # Cria o bucket se não existir
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        # Cria um conteúdo para teste
        content = "Teste de upload no MinIO via DAG Airflow"
        file_name = "minio_test_file.txt"
        data = io.BytesIO(content.encode("utf-8"))

        # Faz upload para o bucket
        client.put_object(
            bucket_name,
            file_name,
            data,
            length=len(content.encode("utf-8")),
            content_type="text/plain"
        )

        logging.info(f"✔️ Arquivo '{file_name}' enviado com sucesso para o bucket '{bucket_name}'.")

    upload_file_to_minio()
