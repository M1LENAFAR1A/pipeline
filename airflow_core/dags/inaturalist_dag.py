# import datetime
# import io
# import logging
# import os
# import sys
# import time

# import pandas as pd
# import pendulum
# import requests
# from airflow import DAG
# from airflow.decorators import task
# from airflow.exceptions import AirflowSkipException
# from airflow.models import Variable

# sys.path.append('/opt/airflow')
# from connectors.minio import MinioConnector
# from connectors.rabbitmq import RabbitMQProducer

# logger = logging.getLogger(__name__)

# dag_parameters = [
#     {'keyword': 'roadkill'},
#     {'keyword': 'atropelada'},
#     {'keyword': 'atropelamento'},
# ]

# logging.getLogger("pika").setLevel(logging.WARNING)  # to ensure we have less logs related to pika itself


# def create_dag(keyword: str):
#     with DAG(dag_id="get_inaturalist_data_{}".format(keyword),
#              schedule="*/30 * * * *",
#              start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
#              catchup=False,
#              dagrun_timeout=datetime.timedelta(minutes=60), ) as dag1:

#         def get_data(maximum_id: str, api_url: str) -> tuple:
#             observations_extracted = []
#             last_observation_id = None
#             logger.info(f"Maximum id {maximum_id} for the request")
#             params = {
#                 'geo': 'true',
#                 'iconic_taxa': 'Amphibia,Aves,Mammalia,Reptilia,unknown',
#                 'id_above': maximum_id,
#                 'q': keyword,   
#                 'page': 1,
#                 'per_page': 200,  # this should be a variable, 200 is the max supported by the API
#                 'order': 'asc',
#                 'order_by': 'id'
#             }

#             response = requests.get(api_url, params=params)

#             if response.status_code == 200:
#                 observations = response.json()
#                 results = observations.get('results')
#                 logger.info(f"Got {len(results)} results")

#                 for each_result in results:
#                     if each_result.get('out_of_range') is not None:
#                         continue
#                     observation_id = each_result.get('id')
#                     uuid = each_result.get('uuid')
#                     species_guess = each_result.get('species_guess')
#                     observed_on_string = each_result.get('observed_on_string')
#                     updated_at = each_result.get('updated_at')
#                     description = each_result.get('description')
#                     geojson_coordinates = each_result.get('geojson').get('coordinates')
#                     # coordinates are in reversed order
#                     geojson_latitude = geojson_coordinates[1]
#                     geojson_longitude = geojson_coordinates[0]

#                     observed_on = each_result.get('observed_on')
#                     created_at = each_result.get('created_at')
#                     photos_data = each_result.get('observation_photos')
#                     if each_result.get('taxon') is None or each_result.get('taxon') == 'None':
#                         taxon_id = None
#                         taxon_name = None
#                         default_photo_object = None
#                     else:
#                         taxon_id = each_result.get('taxon').get('id')
#                         taxon_name = each_result.get('taxon').get('name')
#                         if each_result.get('taxon').get('default_photo') is not None:
#                             default_photo = each_result.get('taxon').get('default_photo')
#                             default_photo_object = {
#                                 'url': default_photo.get('url'),
#                                 'original_dimensions': default_photo.get('original_dimensions'),
#                             }
#                         else:
#                             default_photo_object = None
#                     place_guess = each_result.get('place_guess')

#                     positional_accuracy = each_result.get('positional_accuracy')
#                     quality_grade = each_result.get('quality_grade')
#                     if each_result.get('project_ids') is not None:
#                         project_ids = each_result.get('project_ids')
#                     else:
#                         project_ids = []

#                     observations_photos = []

#                     if len(photos_data) > 0:
#                         for photo_entry in photos_data:
#                             photo_details = {
#                                 'id': photo_entry.get('photo').get('id'),
#                                 'url': photo_entry.get('photo').get('url')
#                             }
#                             observations_photos.append(photo_details)

#                     observation_object = {
#                         'observation_id': observation_id,
#                         'uuid': uuid,
#                         'species_guess': species_guess,
#                         'observed_on_string': observed_on_string,
#                         'updated_at': updated_at,
#                         'description': description,
#                         'geojson_latitude': geojson_latitude,
#                         'geojson_longitude': geojson_longitude,
#                         'observed_on': observed_on,
#                         'created_at': created_at,
#                         'observations_photos': observations_photos,
#                         'taxon_id': taxon_id,
#                         'taxon_name': taxon_name,
#                         'place_guess': place_guess,
#                         'quality_grade': quality_grade,
#                         'positional_accuracy': positional_accuracy,
#                         'default_photo': default_photo_object,
#                         'project_ids': project_ids
#                     }

#                     observations_extracted.append(observation_object)

#                 # if list is not empty, extract the last observation
#                 if observations_extracted:
#                     last_observation_id = observations_extracted[-1]['observation_id']
#                 else:  # otherwise maximum is the same, next iteration will stop
#                     last_observation_id = maximum_id

#                 return last_observation_id, observations_extracted

#             else:
#                 logger.error(f"Status code {response.status_code}")
#                 logger.error(f"Something went wrong {response.text}")
#                 return maximum_id, []

#         @task(retries=3, retry_delay=datetime.timedelta(minutes=1))
#         def get_all_data():
#             all_observations = []
#             logger.info(f"Getting data from keyword: {keyword}")
#             maximum_id = int(Variable.get(f"inaturalist_{keyword}_latest_observation_id", default_var=1))
#             number_results_extracted = 0
#             api_url = Variable.get("INATURALIST_API_URL", default_var="https://api.inaturalist.org/v1/observations")
#             while True:
#                 last_observation_id, observations_extracted = get_data(maximum_id, api_url)

#                 if not observations_extracted:
#                     logger.info("No more observations found, stopping.")
#                     Variable.set(f"inaturalist_{keyword}_latest_observation_id", maximum_id)
#                     break

#                 number_results_extracted = number_results_extracted + len(observations_extracted)
#                 logger.info(f"Extracted {number_results_extracted} observations")

#                 all_observations.extend(observations_extracted)

#                 if last_observation_id:
#                     maximum_id = last_observation_id
#                     if number_results_extracted > 5000:
#                         logger.info(
#                             f"Max number of results per file reached, updating maximum_id variable to {maximum_id}")
#                         Variable.set(f"inaturalist_{keyword}_latest_observation_id", maximum_id)
#                         break
#                 else:
#                     logger.info("No new maximum_id found, stopping.")
#                     break

#             return all_observations

#         @task(retries=3, retry_delay=datetime.timedelta(minutes=1))
#         def process_data(observations: list, keyword: str) -> dict:
#             if len(observations) == 0:
#                 raise AirflowSkipException("No observations to process, skipping tasks")
#             current_date = datetime.datetime.now()
#             year = current_date.strftime('%Y')
#             month = current_date.strftime('%m')
#             day = current_date.strftime('%d')

#             file_name = f"{keyword}_observations_{current_date.strftime('%H%M%S')}.csv"

#             object_path = f"inaturalist/{keyword}/{year}/{month}/{day}/{file_name}"

#             return {'path': object_path, 'format': 'text/csv'}

#         @task(retries=3, retry_delay=datetime.timedelta(minutes=1))
#         def save_data(data: list, path: str, content_type: str):
#             data_lake = Variable.get("DATA_LAKE", default_var="minio")
#             if len(data) == 0:
#                 logger.info("No new observations to store.")
#             else:
#                 json_body = pd.json_normalize(data)

#                 csv_buffer = io.BytesIO()
#                 json_body.to_csv(csv_buffer, index=False, encoding='utf-8')  # Specify encoding explicitly
#                 csv_buffer.seek(0)

#                 # Get the size of the CSV content in bytes
#                 csv_size = len(csv_buffer.getvalue())

#                 if data_lake == 'minio':
#                     minio_url = Variable.get("DATA_LAKE_URL")
#                     access_key = Variable.get("DATA_LAKE_ACCESS_KEY")
#                     secret_key = Variable.get("DATA_LAKE_SECRET_KEY")
#                     bucket_name = Variable.get("DATA_LAKE_BUCKET_NAME")
#                     connector = MinioConnector(url=minio_url,
#                                                access_key=access_key,
#                                                secret_key=secret_key,
#                                                bucket_name=bucket_name)
#                     connector.upload_data(data=csv_buffer,
#                                           path=path,
#                                           content_type=content_type,
#                                           data_size=csv_size)

#         @task(retries=3, retry_delay=datetime.timedelta(minutes=1))
#         def publish_message_for_transformation(path: str):
#             MESSAGE_CHANNEL = Variable.get("PRODUCER_MESSAGE_CHANNEL")
#             RABBITMQ_USER = Variable.get("PRODUCER_USER")
#             RABBITMQ_PASSWD = Variable.get("PRODUCER_PASSWD")
#             RABBITMQ_HOST = Variable.get("PRODUCER_HOST")
#             RABBITMQ_PORT = Variable.get("PRODUCER_PORT")
#             producer = RabbitMQProducer(channel=MESSAGE_CHANNEL,
#                                         user=RABBITMQ_USER,
#                                         passwd=RABBITMQ_PASSWD,
#                                         host=RABBITMQ_HOST,
#                                         port=RABBITMQ_PORT)
#             message_object = {
#                 'path': path,
#                 'source': 'Inaturalist',
#                 'keyword': keyword,
#                 'extraction_date': time.time_ns(),
#                 'retry': 0
#             }
#             producer.publish_message(message=message_object)

#         try:
#             observations = get_all_data()
#             information = process_data(observations=observations, keyword=keyword)
#             save_data_result = save_data(data=observations,
#                                          path=information['path'],
#                                          content_type=information['format'])
#             publish_message_result = publish_message_for_transformation(path=information['path'])
#         except AirflowSkipException as e:
#             logger.info(f"Task skipped: {str(e)}")

#         save_data_result >> publish_message_result


# for dag in dag_parameters:
#     keyword = dag['keyword']
#     globals()[f'inaturalist_{keyword}'] = create_dag(keyword=keyword)

import datetime
import io
import logging
import os
import sys
import time

import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

sys.path.append('/opt/airflow')
from connectors.minio import MinioConnector
from connectors.rabbitmq import RabbitMQProducer

logger = logging.getLogger(__name__)

dag_parameters = [
    {'keyword': 'roadkill'},
    {'keyword': 'atropelada'},
    {'keyword': 'atropelamento'},
]

logging.getLogger("pika").setLevel(logging.WARNING)

def create_dag(keyword: str):
    with DAG(dag_id=f"get_inaturalist_data_{keyword}",
             schedule="*/30 * * * *",
             start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
             catchup=False,
             dagrun_timeout=datetime.timedelta(minutes=60)) as dag1:

        def get_data(maximum_id: str, api_url: str) -> tuple:
            observations_extracted = []
            last_observation_id = None
            logger.info(f"Maximum id {maximum_id} for the request")
            params = {
                'geo': 'true',
                'iconic_taxa': 'Amphibia,Aves,Mammalia,Reptilia,unknown',
                'id_above': maximum_id,
                'q': keyword,
                'page': 1,
                'per_page': 200,
                'order': 'asc',
                'order_by': 'id'
            }

            response = requests.get(api_url, params=params)
            if response.status_code == 200:
                observations = response.json()
                results = observations.get('results', [])
                logger.info(f"Got {len(results)} results")

                for each_result in results:
                    if each_result.get('out_of_range') is not None:
                        continue
                    geojson = each_result.get('geojson', {}).get('coordinates', [None, None])
                    taxon = each_result.get('taxon')
                    photo_obj = None
                    if taxon and taxon.get('default_photo'):
                        default_photo = taxon['default_photo']
                        photo_obj = {
                            'url': default_photo.get('url'),
                            'original_dimensions': default_photo.get('original_dimensions')
                        }
                    photos_data = each_result.get('observation_photos', [])
                    photo_list = [{
                        'id': p['photo'].get('id'),
                        'url': p['photo'].get('url')
                    } for p in photos_data if p.get('photo')]

                    observation_object = {
                        'observation_id': each_result.get('id'),
                        'uuid': each_result.get('uuid'),
                        'species_guess': each_result.get('species_guess'),
                        'observed_on_string': each_result.get('observed_on_string'),
                        'updated_at': each_result.get('updated_at'),
                        'description': each_result.get('description'),
                        'geojson_latitude': geojson[1],
                        'geojson_longitude': geojson[0],
                        'observed_on': each_result.get('observed_on'),
                        'created_at': each_result.get('created_at'),
                        'observations_photos': photo_list,
                        'taxon_id': taxon.get('id') if taxon else None,
                        'taxon_name': taxon.get('name') if taxon else None,
                        'place_guess': each_result.get('place_guess'),
                        'quality_grade': each_result.get('quality_grade'),
                        'positional_accuracy': each_result.get('positional_accuracy'),
                        'default_photo': photo_obj,
                        'project_ids': each_result.get('project_ids', [])
                    }

                    observations_extracted.append(observation_object)

                last_observation_id = observations_extracted[-1]['observation_id'] if observations_extracted else maximum_id
                return last_observation_id, observations_extracted
            else:
                logger.error(f"Status code {response.status_code}")
                logger.error(f"Something went wrong {response.text}")
                return maximum_id, []

        @task(retries=3, retry_delay=datetime.timedelta(minutes=1))
        def get_all_data():
            all_observations = []
            maximum_id = int(Variable.get(f"inaturalist_{keyword}_latest_observation_id", default_var=1))
            number_results_extracted = 0
            api_url = Variable.get("INATURALIST_API_URL", default_var="https://api.inaturalist.org/v1/observations")

            while True:
                last_observation_id, observations_extracted = get_data(maximum_id, api_url)
                if not observations_extracted:
                    Variable.set(f"inaturalist_{keyword}_latest_observation_id", maximum_id)
                    break
                number_results_extracted += len(observations_extracted)
                all_observations.extend(observations_extracted)
                maximum_id = last_observation_id
                if number_results_extracted > 5000:
                    Variable.set(f"inaturalist_{keyword}_latest_observation_id", maximum_id)
                    break
            return all_observations

        @task(retries=3, retry_delay=datetime.timedelta(minutes=1))
        def process_data(observations: list, keyword: str) -> dict:
            if len(observations) == 0:
                raise AirflowSkipException("No observations to process, skipping tasks")
            current_date = datetime.datetime.now()
            file_name = f"{keyword}_observations_{current_date.strftime('%H%M%S')}.csv"
            object_path = f"inaturalist/{keyword}/{current_date.strftime('%Y/%m/%d')}/{file_name}"
            return {'path': object_path, 'format': 'text/csv'}

        @task(retries=3, retry_delay=datetime.timedelta(minutes=1))
        def save_data(data: list, path: str, content_type: str):
            if not data:
                logger.info("No new observations to store.")
                return
            json_body = pd.json_normalize(data)
            csv_buffer = io.BytesIO()
            json_body.to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_buffer.seek(0)
            csv_size = len(csv_buffer.getvalue())
            minio_url = Variable.get("DATA_LAKE_URL").replace("http://", "")
            connector = MinioConnector(
                url=minio_url,
                access_key=Variable.get("DATA_LAKE_ACCESS_KEY"),
                secret_key=Variable.get("DATA_LAKE_SECRET_KEY"),
                bucket_name=Variable.get("DATA_LAKE_BUCKET_NAME")
            )
            connector.upload_data(data=csv_buffer, path=path, content_type=content_type, data_size=csv_size)

        @task(retries=3, retry_delay=datetime.timedelta(minutes=1))
        def publish_message_for_transformation(path: str):
            producer = RabbitMQProducer(
                channel=Variable.get("PRODUCER_MESSAGE_CHANNEL"),
                user=Variable.get("PRODUCER_USER"),
                passwd=Variable.get("PRODUCER_PASSWD"),
                host=Variable.get("PRODUCER_HOST"),
                port=Variable.get("PRODUCER_PORT")
            )
            message_object = {
                'path': path,
                'source': 'Inaturalist',
                'keyword': keyword,
                'extraction_date': time.time_ns(),
                'retry': 0
            }
            producer.publish_message(message=message_object)

        @task()
        def test_minio_upload():
            content = f"Arquivo de teste da DAG principal ({keyword})"
            file_name = f"test/test_file_{keyword}.txt"
            data = io.BytesIO(content.encode("utf-8"))
            connector = MinioConnector(
                url=Variable.get("DATA_LAKE_URL").replace("http://", ""),
                access_key=Variable.get("DATA_LAKE_ACCESS_KEY"),
                secret_key=Variable.get("DATA_LAKE_SECRET_KEY"),
                bucket_name=Variable.get("DATA_LAKE_BUCKET_NAME")
            )
            connector.upload_data(data=data, path=file_name, content_type="text/plain", data_size=len(content.encode("utf-8")))

        try:
            observations = get_all_data()
            info = process_data(observations=observations, keyword=keyword)
            save = save_data(data=observations, path=info['path'], content_type=info['format'])
            pub = publish_message_for_transformation(path=info['path'])
            test_minio_upload()
            save >> pub
        except AirflowSkipException as e:
            logger.info(f"Task skipped: {str(e)}")

    return dag1

for dag in dag_parameters:
    keyword = dag['keyword']
    globals()[f'inaturalist_{keyword}'] = create_dag(keyword=keyword)
