import datetime
import io
import logging
import time
import pandas as pd

import sys
sys.path.append('/opt/airflow')

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

from connectors.minio import MinioConnector
from connectors.rabbitmq import RabbitMQProducer

logger = logging.getLogger(__name__)

with DAG(
    dag_id="teste_ingestao_dados_fakes",
    schedule_interval=None,
    start_date=datetime.datetime(2025, 1, 1),
    catchup=False,
    tags=["teste", "pipeline"]
) as dag:

    @task()
    def gerar_dados_fakes():
        dados = [
            {
                "observation_id": 1,
                "species_guess": "Capivara",
                "observed_on": "2025-03-25",
                "latitude": -23.55,
                "longitude": -46.63
            },
            {
                "observation_id": 2,
                "species_guess": "Tamanduá",
                "observed_on": "2025-03-24",
                "latitude": -15.78,
                "longitude": -47.93
            }
        ]
        logger.info(f"Dados gerados: {dados}")
        return dados

    @task()
    def salvar_no_minio(dados):
        if not dados:
            raise AirflowSkipException("Nenhum dado gerado.")

        df = pd.DataFrame(dados)
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        now = datetime.datetime.now()
        path = f"teste/{now:%Y/%m/%d}/teste_observacoes_{now:%H%M%S}.csv"
        size = len(buffer.getvalue())

        logger.info(f"Salvando arquivo no MinIO em {path}")

        minio = MinioConnector(
            url="http://172.18.0.2:9000",
            access_key="HoebsqZbQPVnyCLlrFQI",
            secret_key="luMwhHsLhxAJ8DOeLHAkr99GGjCFbJIfe6nihnad",
            bucket_name="environbit"
        )

        minio.upload_data(
            data=buffer,
            path=path,
            content_type="text/csv",
            data_size=size
        )
        return path

    @task()
    def enviar_para_rabbitmq(path):
        logger.info(f"Enviando caminho para o RabbitMQ: {path}")
        producer = RabbitMQProducer(
            channel="environbit_channel",  # substitua se necessário
            user="admin",                 # substitua se necessário
            passwd="admin",               # substitua se necessário
            host="rabbitmq",              # ou "localhost", ou IP fixo do container rabbit
            port=5672
        )

        message = {
            "path": path,
            "source": "teste",
            "keyword": "fake_data",
            "extraction_date": time.time_ns(),
            "retry": 0
        }
        producer.publish_message(message)

    # Encadeamento das tarefas
    dados = gerar_dados_fakes()
    caminho = salvar_no_minio(dados)
    enviar_para_rabbitmq(caminho)
