import csv
import datetime
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional, List, Union

import pendulum
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from pydantic import BaseModel

sys.path.append('/opt/airflow')
from connectors.minio import MinioConnector
from connectors.rabbitmq import RabbitMQProducer

logger = logging.getLogger(__name__)


class Occurence(BaseModel):
    key: int
    basisOfRecord: Optional[str] = None
    individualCount: Optional[int] = None
    occurrenceStatus: Optional[str] = None
    sex: Optional[str] = None
    lifeStage: Optional[str] = None
    taxonKey: Optional[int] = None
    kingdomKey: Optional[int] = None
    phylumKey: Optional[int] = None
    classKey: Optional[int] = None
    orderKey: Optional[int] = None
    familyKey: Optional[int] = None
    genusKey: Optional[int] = None
    subgenusKey: Optional[int] = None
    speciesKey: Optional[int] = None
    acceptedTaxonKey: Optional[int] = None
    scientificName: Optional[str] = None
    acceptedScientificName: Optional[str] = None
    kingdom: Optional[str] = None
    phylum: Optional[str] = None
    order: Optional[str] = None
    family: Optional[str] = None
    genus: Optional[str] = None
    subgenus: Optional[str] = None
    species: Optional[str] = None
    genericName: Optional[str] = None
    dateIdentified: Optional[str] = None
    decimalLatitude: Optional[float] = None
    decimalLongitude: Optional[float] = None
    continent: Optional[str] = None
    stateProvince: Optional[str] = None
    country: Optional[str] = None


dag_parameters = [
    {'keyword': 'roadkill'},
    {'keyword': 'atropelada'},
    {'keyword': 'atropelamento'},
]

API_URL = os.getenv('GBIF_API', 'https://www.gbif.org/api/occurrence/search')
DATA_LAKE = Variable.get("DATA_LAKE", default_var="minio")
MESSAGE_CHANNEL = Variable.get("MESSAGE_CHANNEL", default_var="environbit")


def create_dag(keyword: str):
    with DAG(dag_id="get_gbif_data_{}".format(keyword),
             schedule="*/30 * * * *",
             start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
             catchup=False,
             dagrun_timeout=datetime.timedelta(minutes=60), ) as dag1:

        def get_data(offset: int, limit: int) -> tuple:
            occurrences_extracted = []
            params = {
                'advanced': 'false',
                'classKey': [131, 358, 212, 359],
                'continent': ['EUROPE', 'NORTH_AMERICA', 'SOUTH_AMERICA'],
                'dwca_extension.facetLimit': 1000,
                'facet': 'continent',
                'facetMultiselect': 'true',
                'issue.facetLimit': 1000,
                'locale': 'en',
                'month.facetLimit': 12,
                'occurrence_status': 'present',
                'q': keyword,
                'type_status.facetLimit': 1000,
                'offset': offset,
                'limit': limit
            }
            response = requests.get(API_URL, params=params)

            if response.status_code == 200:
                occurrences = response.json()

                number_of_occurrences = occurrences.get('count')
                results = occurrences.get('results')
                index = 0
                for each_occurrence in results:
                    if index == 0:
                        print("each occurrence", each_occurrence)

                    gbif_id = each_occurrence.get('gbifID')
                    dataset_key = each_occurrence.get('datasetKey')
                    occurrence_id = each_occurrence.get('occurrenceID')
                    kingdom = each_occurrence.get('kingdom')
                    phylum = each_occurrence.get('phylum')
                    class_ = each_occurrence.get('class')
                    order = each_occurrence.get('order')
                    family = each_occurrence.get('family')
                    genus = each_occurrence.get('genus')
                    species = each_occurrence.get('species')
                    infraspecific_epithet = each_occurrence.get('infraspecificEpithet')  # not exist in all of them
                    taxon_rank = each_occurrence.get('taxonRank')
                    scientific_name = each_occurrence.get('scientificName')
                    verbantic_scientific_name = each_occurrence.get('verbatimScientificName')
                    verbantic_scientific_name_authorship = each_occurrence.get('verbatimScientificNameAuthorship')
                    country_code = each_occurrence.get('countryCode')
                    locality = each_occurrence.get('locality')
                    state_province = each_occurrence.get('stateProvince')
                    occurrence_status = each_occurrence.get('occurrenceStatus')
                    individual_count = each_occurrence.get('individualCount')
                    publishing_org_key = each_occurrence.get('publishingOrgKey')
                    if isinstance(publishing_org_key, dict):
                        publishing_org_key = publishing_org_key.get('title')
                    decimal_latitude = each_occurrence.get('decimalLatitude')
                    decimal_longitude = each_occurrence.get('decimalLongitude')
                    coordinate_uncertainty_in_meters = each_occurrence.get('coordinateUncertaintyInMeters')
                    coordinate_precision = each_occurrence.get('coordinatePrecision')
                    elevation = each_occurrence.get('elevation')
                    elevation_accuracy = each_occurrence.get('elevationAccuracy')
                    depth = each_occurrence.get('depth')
                    depth_accuracy = each_occurrence.get('depthAccuracy')
                    event_date = each_occurrence.get('eventDate')
                    day = each_occurrence.get('day')
                    month = each_occurrence.get('month')
                    year = each_occurrence.get('year')
                    taxon_key = each_occurrence.get('taxonKey')
                    species_key = each_occurrence.get('speciesKey')
                    basis_of_record = each_occurrence.get('basisOfRecord')
                    institution_code = each_occurrence.get('institutionCode')
                    collection_code = each_occurrence.get('collectionCode')
                    catalog_number = each_occurrence.get('catalogNumber')
                    record_number = each_occurrence.get('recordNumber')
                    identified_by = each_occurrence.get('identifiedBy')
                    date_identified = each_occurrence.get('dateIdentified')
                    occurrence_license = each_occurrence.get('license')
                    rights_holder = each_occurrence.get('rightsHolder')
                    recorded_by = each_occurrence.get('recordedBy')
                    type_status = each_occurrence.get('typeStatus')
                    establishment_means = each_occurrence.get('establishmentMeans')
                    last_interpreted = each_occurrence.get('lastInterpreted')
                    media_type = each_occurrence.get('mediaType')
                    issue = each_occurrence.get('issue')

                    occurrences_extracted.append({
                        "gbifID": gbif_id,
                        "datasetKey": dataset_key,
                        "occurrenceID": occurrence_id,
                        "kingdom": kingdom,
                        "phylum": phylum,
                        "class": class_,
                        "order": order,
                        "family": family,
                        "genus": genus,
                        "species": species,
                        "infraspecificEpithet": infraspecific_epithet,
                        "taxonRank": taxon_rank,
                        "scientificName": scientific_name,
                        "verbatimScientificName": verbantic_scientific_name,
                        "verbatimScientificNameAuthorship": verbantic_scientific_name_authorship,
                        "countryCode": country_code,
                        "locality": locality,
                        "stateProvince": state_province,
                        "occurrenceStatus": occurrence_status,
                        "individualCount": individual_count,
                        "publishingOrgKey": publishing_org_key,
                        "decimalLatitude": decimal_latitude,
                        "decimalLongitude": decimal_longitude,
                        "coordinateUncertaintyInMeters": coordinate_uncertainty_in_meters,
                        "coordinatePrecision": coordinate_precision,
                        "elevation": elevation,
                        "elevationAccuracy": elevation_accuracy,
                        "depth": depth,
                        "depthAccuracy": depth_accuracy,
                        "eventDate": event_date,
                        "day": day,
                        "month": month,
                        "year": year,
                        "taxonKey": taxon_key,
                        "speciesKey": species_key,
                        "basisOfRecord": basis_of_record,
                        "institutionCode": institution_code,
                        "collectionCode": collection_code,
                        "catalogNumber": catalog_number,
                        "recordNumber": record_number,
                        "identifiedBy": identified_by,
                        "dateIdentified": date_identified,
                        "license": occurrence_license,
                        "rightsHolder": rights_holder,
                        "recordedBy": recorded_by,
                        "typeStatus": type_status,
                        "establishmentMeans": establishment_means,
                        "lastInterpreted": last_interpreted,
                        "mediaType": media_type,
                        "issue": issue,
                    })

                    if index == 0:
                        print(occurrences_extracted[0])
                        index += 1

                return offset + number_of_occurrences, occurrences_extracted

            else:
                logger.error(f"Status code {response.status_code}")
                logger.error(f"Something went wrong {response.text}")
                return offset, []

        @task(retries=3, retry_delay=datetime.timedelta(minutes=1))
        def get_all_data():
            all_occurrences = []
            logger.info(f"Getting data from keyword: {keyword}")
            offset = int(Variable.get(f"gbif_{keyword}_offset", default_var=0))
            while True:
                new_offset, occurrences_extracted = get_data(offset, limit=300)

                if not occurrences_extracted:
                    logger.info("No more occurrences found, stopping.")
                    Variable.set(f"gbif_{keyword}_offset", new_offset)
                    break

                all_occurrences.extend(occurrences_extracted)
                if new_offset:
                    offset = new_offset
                else:
                    logger.info("No new offset, stopping.")
                    break

            return all_occurrences

        @task(retries=3, retry_delay=datetime.timedelta(minutes=1))
        def process_data(occurrences: List[dict], keyword: str) -> Union[dict, None]:
            current_date = datetime.datetime.now()
            year = current_date.strftime('%Y')
            month = current_date.strftime('%m')
            day = current_date.strftime('%d')

            file_name = f"{keyword}_occurrences_{current_date.strftime('%H%M%S')}.csv"
            object_path = f"gbif_data/{keyword}_data/{year}/{month}/{day}/{file_name}"

            os.makedirs(f'/tmp/gbif/{keyword}/', exist_ok=True)

            if len(occurrences) == 0:
                logger.info("No new occurrences to process")
                raise AirflowSkipException("No occurrences to process")
            else:
                # Open the CSV file in write mode
                with open(f'/tmp/gbif/{keyword}/{file_name}', 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=occurrences[0].keys())

                    # Write the header row
                    writer.writeheader()

                    # Write each data row
                    for item in occurrences:
                        writer.writerow(item)

            return {'path': object_path, 'tmp_path': f'/tmp/gbif/{keyword}/{file_name}', 'format': 'text/csv'}

        @task(retries=3, retry_delay=datetime.timedelta(minutes=1))
        def save_data(data: list, tmp_path: str, path: str, content_type: str, data_lake: str) -> bool:
            if len(data) == 0:
                logger.info("No new occurrences to store.")
            else:
                if data_lake == 'minio':
                    connector = MinioConnector()
                    csv_path = Path(tmp_path)
                    if csv_path.exists():
                        csv_size = os.stat(csv_path).st_size
                        with open(csv_path, 'rb') as csv:
                            connector.upload_data(data=csv, path=path, content_type=content_type, data_size=csv_size)
                    return True
            return False

        @task(retries=3, retry_delay=datetime.timedelta(minutes=1))
        def publish_message_for_transformation(path: str, publish: bool):
            if not publish:
                logger.info("No data to publish")
            else:
                logger.info(f"Publishing message for path {path}")
                producer = RabbitMQProducer(channel=MESSAGE_CHANNEL)
                message_object = {
                    'path': path,
                    'source': 'GBIF',
                    'keyword': keyword,
                    'extraction_date': time.time_ns()
                }
                producer.publish_message(message=message_object)

        try:
            occurrences = get_all_data()
            information = process_data(occurrences=occurrences, keyword=keyword)
            saved_data = save_data(data=occurrences,
                                   tmp_path=information['tmp_path'],
                                   path=information['path'],
                                   content_type=information['format'],
                                   data_lake=DATA_LAKE)
            publish_message_for_transformation(path=information['path'], publish=saved_data)
        except AirflowSkipException as e:
            logger.info(f"Task skipped: {str(e)}")


for dag in dag_parameters:
    keyword = dag['keyword']
    globals()[f'gbif_{keyword}'] = create_dag(keyword=keyword)
