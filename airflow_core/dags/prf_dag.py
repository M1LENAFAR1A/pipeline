import datetime
import logging
import os.path
import sys
import time
from pathlib import Path

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from bs4 import BeautifulSoup

sys.path.append('/opt/airflow')
from connectors.minio import MinioConnector
from connectors.rabbitmq import RabbitMQProducer

logger = logging.getLogger(__name__)

logging.getLogger("pika").setLevel(logging.WARNING)  # to ensure we have less logs related to pika itself

DATA_LAKE = Variable.get("DATA_LAKE", default_var="minio")

PRF_URL = 'https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf'


@dag(
    dag_id="prf_dag",
    schedule_interval="0 0 1 * *",
    start_date=pendulum.datetime(2024, 10, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessAccidents():
    def extract_file_id(drive_url: str):
        import re
        # Use a regular expression to find the file ID in the URL
        match = re.search(r'/d/([a-zA-Z0-9_-]+)', drive_url)
        if match:
            return match.group(1)
        else:
            return None

    @task
    def extract_hrefs() -> list:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/102.0.0.0 Safari/537.36'
        }

        response = requests.get(PRF_URL, headers=headers)
        if response.status_code == 200:
            # Parse the HTML content
            soup = BeautifulSoup(response.content, 'html.parser')

            table = soup.find_all('table', class_='plain')
            table = table[1]
            if table:
                tbody = table.find('tbody')
                if tbody:
                    # Find all 'a' tags within 'td' elements in the tbody and extract the 'href' attributes
                    td_values = [
                        (row.find_all('td')[0].get_text(strip=True), row.find('a', href=True)['href'])
                        for row in tbody.find_all('tr')
                        if row.find('a', href=True) and should_include_year(row.find_all('td')[0].get_text(strip=True))
                    ]

                    for description, link in td_values:
                        logger.info(description)
                        logger.info(link)

                    full_extraction = Variable.get("FULL_EXTRACTION", default_var="True").lower() == "true"
                    if full_extraction:
                        return td_values[:-1]  # the last href isn't needed

                    return td_values
                else:
                    logger.error("No <tbody> found in the table.")
            else:
                logger.error("No table with class 'plain' found.")
        else:
            logger.error(f"Failed to retrieve the page. Status code: {response.status_code}")

    @task(retries=3, retry_delay=datetime.timedelta(minutes=15))
    def get_data(valid_urls: list) -> list:
        list_of_files = []
        for each_valid_url in valid_urls:
            description = each_valid_url[0]
            valid_url = each_valid_url[1]

            file_id = extract_file_id(valid_url)
            final_url = f'https://drive.google.com/uc?export=download&id={file_id}'
            response = requests.get(final_url)

            # Check if the request was successful
            if response.status_code == 200:
                import io
                import zipfile
                zip_data = io.BytesIO(response.content)

                # Open the ZIP file
                with zipfile.ZipFile(zip_data, 'r') as zip_ref:
                    file_list = zip_ref.namelist()
                    csv_filename = [file for file in file_list if file.endswith('.csv')][0]

                    cleaned_text = description.replace('\xa0', ' ')
                    description_split = cleaned_text.split(' ')

                    file_year = description_split[4]
                    current_month = pendulum.now().month
                    current_year = pendulum.now().year

                    full_extraction = Variable.get("FULL_EXTRACTION", default_var="True").lower() == "true"
                    if not full_extraction and str(file_year) != str(current_year):
                        month_filename = f"{os.path.splitext(csv_filename)[0]}_12{os.path.splitext(csv_filename)[1]}"
                    else:
                        month_filename = (f"{os.path.splitext(csv_filename)[0]}_{current_month}"
                                          f"{os.path.splitext(csv_filename)[1]}")

                    zip_ref.extract(csv_filename)

                    if '(Agrupados por ocorrência)' in description:
                        month_filename = f'occorência_{month_filename}'
                    elif '(Agrupados por pessoa)' in description:
                        month_filename = f'pessoa_{month_filename}'
                    else:
                        # continue with the same month filename
                        pass

                    os.rename(csv_filename, month_filename)
                    logger.info(f"Renamed '{csv_filename}' to '{month_filename}'")

                list_of_files.append((month_filename, file_year))
            else:
                logger.error(f"Failed to download the file. Status code: {response.status_code}")

        return list_of_files

    @task
    def save_data(list_of_files: list, data_lake: str):
        if data_lake == 'minio':
            minio_url = Variable.get("DATA_LAKE_URL")
            access_key = Variable.get("DATA_LAKE_ACCESS_KEY")
            secret_key = Variable.get("DATA_LAKE_SECRET_KEY")
            bucket_name = Variable.get("DATA_LAKE_BUCKET_NAME")
            connector = MinioConnector(url=minio_url,
                                       access_key=access_key,
                                       secret_key=secret_key,
                                       bucket_name=bucket_name)

            for each_file in list_of_files:
                csv_filename = each_file[0]
                csv_year = each_file[1]

                csv_name = f"/prf/{csv_year}/{csv_filename}"
                csv_path = Path(csv_filename)

                if csv_path.exists():
                    csv_size = os.stat(csv_path).st_size
                    with open(csv_path, 'rb') as csv:
                        connector.upload_data(data=csv, path=csv_name, content_type='text/csv', data_size=csv_size)

                    # after uploading the data waits 3 seconds and sends the message to rabbit mq
                    time.sleep(3)
                    publish_message_for_transformation(path=csv_name)
                else:
                    logger.error(f"CSV file {csv_filename} not found")
                    raise FileNotFoundError(f"CSV file {csv_filename} not found")

        full_extraction = bool(Variable.get("FULL_EXTRACTION", default_var=True))
        if full_extraction:
            Variable.set("FULL_EXTRACTION", False)

    def publish_message_for_transformation(path: str):
        MESSAGE_CHANNEL = Variable.get("PRODUCER_MESSAGE_CHANNEL")
        RABBITMQ_USER = Variable.get("PRODUCER_USER")
        RABBITMQ_PASSWD = Variable.get("PRODUCER_PASSWD")
        RABBITMQ_HOST = Variable.get("PRODUCER_HOST")
        RABBITMQ_PORT = Variable.get("PRODUCER_PORT")
        producer = RabbitMQProducer(channel=MESSAGE_CHANNEL,
                                    user=RABBITMQ_USER,
                                    passwd=RABBITMQ_PASSWD,
                                    host=RABBITMQ_HOST,
                                    port=RABBITMQ_PORT)
        message_object = {
            'path': path,
            'source': 'PRF',
            'extraction_date': time.time_ns(),
            'retry': 0
        }
        logger.info(f"Sending object {message_object} to {MESSAGE_CHANNEL} queue")
        producer.publish_message(message=message_object)

    def should_include_year(text):
        current_month = pendulum.now().month
        current_year = pendulum.now().year

        # check the variable to see if the full extraction is needed
        full_extraction = Variable.get("FULL_EXTRACTION", default_var="True").lower() == "true"

        # If full_extraction is True, include all years
        if current_month == 1:
            # We are in January, so include December of the previous year (include the year itself)
            relevant_years = {str(current_year), str(current_year - 1)}
        else:
            # Otherwise, just include the current year
            relevant_years = {str(current_year)}

        if full_extraction:
            return True

        # Check if any relevant year appears in the text
        return any(year in text for year in relevant_years)

    valid_urls = extract_hrefs()
    files_to_store = get_data(valid_urls=valid_urls)
    save_data(list_of_files=files_to_store, data_lake=DATA_LAKE)


dag = ProcessAccidents()
