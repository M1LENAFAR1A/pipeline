import os
import datetime
import pendulum
import csv
import time

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

# Ajuste esses imports conforme sua estrutura
# Ex.: se você tiver airflow_core/connectors, aponte corretamente
from connectors.minio import MinioConnector
from connectors.rabbitmq import RabbitMQProducer

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="test_dag_csv_to_minio",
    schedule_interval=None,  # ou "0 12 * * *" se quiser agendar
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    description="DAG de teste: gera CSV, salva no Minio e publica mensagem no RabbitMQ",
) as dag:

    @task
    def generate_csv():
        """
        Gera um CSV simples em /tmp, com nome test_data_<timestamp>.csv
        """
        current_ts = int(time.time())
        filename = f"/tmp/test_data_{current_ts}.csv"
        rows = [
            ["id", "name", "value"],
            ["1", "foo", "100"],
            ["2", "bar", "200"],
            ["3", "baz", "300"],
        ]
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for row in rows:
                writer.writerow(row)

        return filename

    @task
    def upload_to_minio(csv_path: str):
        """
        Envia o CSV para o Minio, num path com ano/mes (ex: test_data/2025/03/file.csv).
        """
        # Se preferir, pegue as variáveis do Airflow
        data_lake_url = Variable.get("DATA_LAKE_URL", default_var="minio:9000")
        access_key = Variable.get("DATA_LAKE_ACCESS_KEY", default_var="admin")
        secret_key = Variable.get("DATA_LAKE_SECRET_KEY", default_var="admin123")
        bucket_name = Variable.get("DATA_LAKE_BUCKET_NAME", default_var="environbit")

        connector = MinioConnector(
            url=data_lake_url,
            access_key=access_key,
            secret_key=secret_key,
            bucket_name=bucket_name
        )

        now = datetime.datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")

        # Caminho no Minio ex: test_data/2025/03/test_data_...csv
        object_path = f"test_data/{year}/{month}/{os.path.basename(csv_path)}"

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"{csv_path} não existe. Tarefa anterior falhou?")

        csv_size = os.stat(csv_path).st_size
        with open(csv_path, "rb") as f:
            connector.upload_data(
                file_path=object_path,
                data=f,
                content_type="text/csv",
                file_size=csv_size
            )

        return object_path

    @task
    def publish_message_for_transformation(minio_object_path: str):
        """
        Publica mensagem no RabbitMQ, informando o caminho do CSV no Minio
        para que o consumer transforme e salve no Postgres.
        """
